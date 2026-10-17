# Code review — `mentoring/bad_code.py`

**Contexto:** entrega hipotética de un ingeniero junior. La función `process(file_path, country)`
toma las entregas de rutina (ZPRE, ZVE1) de un país, pasa la cantidad a unidades (1 CS = 20 ST),
calcula el total y lo escribe en Parquet. El refactor está en
[`good_code.py`](good_code.py) y sus pruebas en [`tests/test_mentoring.py`](../tests/test_mentoring.py).

Las líneas citadas corresponden a `bad_code.py`. Las afirmaciones marcadas con 🔎 se verificaron
ejecutando la lógica original sobre el dataset real de la prueba (pandas 2.2 y PySpark 3.5).

## Lo que está bien

- La **regla de negocio es correcta**: filtra rutina, convierte CS a ST con factor 20 y calcula el
  total como cantidad × precio.
- La función es corta y se entiende qué intenta hacer.
- La salida queda separada por país.

Es una buena base: los problemas están en **cómo** se ejecuta, no en **qué** calcula.

## Resumen

| # | Observación | Severidad | Líneas |
|---|---|---|---|
| 1 | Publica datos incorrectos sin avisar | Alta | 11–22 |
| 2 | Procesa en pandas, fila por fila, en una sola máquina | Alta | 7, 10, 23–24 |
| 3 | Se rompe con un país sin datos y no maneja errores | Alta | 8, 24, 26 |
| 4 | Escritura no idempotente ni atómica, y en `/tmp` | Alta | 25 |
| 5 | Reglas de negocio y rutas escritas en el código | Media | 11, 13, 25, 29 |
| 6 | Ejecuta trabajo al importar el módulo | Media | 4, 29 |
| 7 | Tipos inferidos y `float` para dinero | Media | 7 |
| 8 | Multi-tenant resuelto con un literal sensible a mayúsculas | Media | 8, 29 |
| 9 | Legibilidad: nombres, `print`, sin tipos ni docstrings | Baja | 6, 10, 16–22, 26–27 |
| 10 | Sin pruebas | Media | — |

**Cobertura de las áreas de mejora que plantea la consigna:**

| Área | Observaciones |
|---|---|
| Uso de pandas donde corresponde Spark nativo | 2 |
| Iteración fila por fila | 2 |
| Lógica de negocio hardcodeada | 5 |
| Ausencia de validaciones y tipado | 1, 7, 9 |
| Naming inconsistente | 9 |
| Manejo de errores inexistente | 3 |
| Escritura no idempotente | 4 |
| Ausencia de tests | 10 |
| Falta de soporte multi-tenant | 8 |

---

## 1. Publica datos incorrectos sin avisar — **Alta**

**Qué está mal.** Ninguna fila se valida antes de calcular:
- Cualquier `unidad` distinta de `CS` se trata como `ST` (líneas 14–15): una unidad desconocida
  (`KG`, `PK`, vacía) se suma como si fueran unidades sueltas.
- Cantidades negativas, en cero o nulas, precios nulos y fechas nulas o inválidas se publican tal
  cual.
- Los duplicados exactos se cuentan dos veces.

**Por qué importa.** 🔎 Con `GT` publica 262 filas: 1 con total negativo y 2 sin fecha. Quien
consuma la tabla suma ingresos negativos y filas imposibles de ubicar en el tiempo, y nadie se
entera, porque el script termina con `done`.

**Cómo se corrige.** Validar antes de publicar y **rechazar con un motivo** las filas que no
cumplen, contándolas en el log (o enviándolas a cuarentena, como hace el pipeline). La conversión
debe aceptar solo `CS` y `ST`:

```python
qty = F.when(F.col("unidad") == "CS", F.col("cantidad") * units_per_case) \
       .when(F.col("unidad") == "ST", F.col("cantidad"))   # cualquier otra unidad -> null -> rechazo
```

En `good_code.py`: `routine_deliveries()` devuelve `(valid, rejected)`; cada fila rechazada lleva
su `reject_reason` y el script registra cuántas hubo de cada motivo.

## 2. Procesa en pandas, fila por fila, en una sola máquina — **Alta**

**Qué está mal.** `pd.read_csv` (línea 7) carga todo el archivo en la memoria de un solo proceso,
`iterrows()` (línea 10) recorre fila por fila en Python, y al final (líneas 23–24) se copia todo
a Spark solo para escribir.

**Por qué importa.** Spark está en el código pero no hace ningún trabajo. Con el volumen real de
una empresa (millones de filas) el proceso se queda sin memoria o tarda horas; `iterrows` es de
las formas más lentas de recorrer un DataFrame. Además se pagan dos conversiones de formato.

**Cómo se corrige.** Leer y transformar con la API de DataFrames de Spark, expresando la lógica
por columnas (`filter`, `when`, `withColumn`). El trabajo se reparte entre los ejecutores y nunca
pasa por un bucle de Python.

## 3. Se rompe con un país sin datos y no maneja errores — **Alta**

**Qué está mal.** Si el filtro de la línea 8 no deja filas, `pd.DataFrame([])` queda sin columnas
y `spark.createDataFrame` (línea 24) falla. No hay ningún manejo de errores y el resultado se
anuncia con `print("done")` (línea 26).

**Por qué importa.** 🔎 `process("data.csv", "gt")` (el país en minúscula, como lo usa el resto de
la plataforma) termina con `IndexError: list index out of range`. Un día sin entregas en un país
haría fallar el job completo con un error que no explica la causa.

**Cómo se corrige.** Trabajar con DataFrames de Spark (un resultado vacío es simplemente cero
filas), registrar el resultado con `logging` y devolver un código de salida distinto de cero cuando
algo falla, para que el orquestador lo detecte.

## 4. Escritura no idempotente ni atómica, y en `/tmp` — **Alta**

**Qué está mal.** `write.mode("overwrite").parquet("/tmp/output/" + country)` (línea 25).

**Por qué importa.**
- **No es idempotente de forma confiable:** reejecutar con la misma entrada da el mismo resultado
  solo si nada falla durante la escritura; y como el destino es todo el país, no hay forma de
  reprocesar un día o un rango sin reescribir toda su historia. Una escritura idempotente
  reemplaza exactamente lo que se reprocesa, la ejecute una o diez veces.
- **No es atómica:** Parquet en modo `overwrite` borra la carpeta y luego escribe. Si el proceso se
  cae en el medio, el país queda sin datos o con datos a medias, y un lector concurrente puede ver
  la carpeta vacía.
- **`/tmp` es efímero:** se borra al reiniciar la máquina o el cluster.

**Cómo se corrige.** Escribir en **Delta Lake**, que es transaccional: o se escribe todo o nada,
y un lector nunca ve la tabla a medias. Como este script recalcula el tenant completo, basta un
`overwrite` atómico (es lo que hace `good_code.py`): correrlo dos veces da el mismo resultado.
Cuando se procesa por rango de fechas, se reemplaza solo ese rango, como hace el pipeline:

```python
df.write.format("delta").mode("overwrite") \
  .option("replaceWhere", f"fecha_proceso BETWEEN '{start}' AND '{end}'") \
  .partitionBy("fecha_proceso").save(output_path)   # output_path viene de la configuración
```

## 5. Reglas de negocio y rutas escritas en el código — **Media**

**Qué está mal.** Los tipos `"ZPRE"`/`"ZVE1"` (línea 11), el factor `20` (línea 13), la ruta
`/tmp/output/` (línea 25) y la llamada con `"data.csv"`, `"GT"` (línea 29).

**Por qué importa.** Cambiar el tipo de entrega, el factor de un país o el ambiente obliga a
modificar y redesplegar código. Es fácil que dos scripts terminen con reglas distintas.

**Cómo se corrige.** Como primer paso, constantes con nombre al inicio del archivo
(`ROUTINE_TYPES`, `UNITS_PER_CASE`) y rutas y tenant por argumento: es lo que hace `good_code.py`.
En el pipeline, esos valores salen de la configuración del proyecto
(`business.routine_delivery_types`, `business.units_per_case`, `paths.*`), que además cambia por
ambiente y por tenant.

## 6. Ejecuta trabajo al importar el módulo — **Media**

**Qué está mal.** La `SparkSession` se crea a nivel de módulo (línea 4) y `process(...)` se llama
al final del archivo (línea 29), fuera de un `if __name__ == "__main__":`.

**Por qué importa.** Basta con hacer `import bad_code` (por ejemplo, desde un test) para que arranque
Spark y se procese `data.csv`. No se puede probar ni reutilizar la función sin ejecutar todo.

**Cómo se corrige.** Crear la sesión dentro de una función (o recibirla como parámetro) y poner la
ejecución detrás de `if __name__ == "__main__": sys.exit(main())`.

## 7. Tipos inferidos y `float` para dinero — **Media**

**Qué está mal.** `pd.read_csv` (línea 7) adivina los tipos.

**Por qué importa.** 🔎 `precio` y `cantidad` se leen como `float64`: el precio
`16.115560169153356500` se convierte en `16.115560169153355`. En sumas de millones de filas esos
errores de redondeo se acumulan en los ingresos. `fecha_proceso` queda como texto mezclado con
`NaN`, y el tipo de cada columna puede cambiar de un archivo a otro.

**Cómo se corrige.** Esquema explícito con `decimal(38,18)` para montos y cantidades, y
`enforceSchema=False` para que la lectura falle si cambia el encabezado del archivo.

## 8. Multi-tenant resuelto con un literal sensible a mayúsculas — **Media**

**Qué está mal.** El país llega como literal (`"GT"`, línea 29) y se compara exacto con la columna
(línea 8).

**Por qué importa.** La convención de la plataforma es el código de tenant en minúscula; si alguien
pasa `gt`, el resultado es vacío (ver observación 3). Agregar un país implica tener que tocar el script.

**Cómo se corrige.** Recibir el tenant por argumento, validarlo contra la configuración y
normalizar la columna de origen (`lower(trim(pais))`) antes de comparar.

## 9. Legibilidad — **Baja**

**Qué está mal.**
- Nombres poco descriptivos o inconsistentes: `fecha` en lugar de `fecha_proceso`, `total` (es
  ingreso), `out`, `sdf`; la variable `i` (línea 10) no se usa.
- Sin type hints ni docstring en `process` (línea 6).
- `print("done")` (línea 26) en lugar de `logging`.
- La función lee, transforma, escribe **y** devuelve un DataFrame de pandas: en total hace cuatro cosas a la vez.

**Por qué importa.** El código se lee más veces de las que se escribe; nombres claros y funciones
con una sola responsabilidad hacen que el siguiente cambio sea seguro.

**Cómo se corrige.** Separar la transformación (`routine_deliveries`, una función pura que se
prueba sin archivos) de la lectura y escritura (`process`); nombres que digan lo que contienen
(`fecha_proceso` en vez de `fecha`, `cantidad_st`); `logging` con el tenant en cada mensaje.

## 10. Sin pruebas — **Media**

**Qué está mal.** No hay ninguna prueba, y la estructura actual (observación 6) impide escribirlas.

**Por qué importa.** Nada garantiza que el factor 20 o el filtro de rutina sigan funcionando
después del próximo cambio.

**Cómo se corrige.** Con la transformación como función pura, se prueba con un DataFrame de pocas
filas. `tests/test_mentoring.py` verifica las reglas de negocio, los rechazos (unidad desconocida,
cantidades inválidas, fechas inválidas, duplicados), la idempotencia y que un rango sin datos no
falla.

---

## Cómo se lo explicaría al junior

Lo haría en una **conversación 1:1 o una sesión de pair programming**, no solo con comentarios en
el PR. Empezaría por lo que está bien: entendió la regla de negocio, y eso es lo más difícil de
aprender. Después daría el feedback con el formato **situación → comportamiento → impacto** y con
evidencia concreta: "con GT, este script publica un total negativo y dos filas sin fecha".

En vez de diez comentarios a la vez, **priorizaría tres**: no publicar datos inválidos en silencio,
usar Spark de punta a punta y escribir de forma idempotente. El resto iría en un segundo PR. Usaría
**preguntas** antes que instrucciones ("¿qué pasa si el país llega en minúscula?", "¿y si el job se
cae a mitad de la escritura?") para que llegue a la conclusión por su cuenta, y le pediría que
escriba primero el test de la conversión CS→ST.

Le pediría investigar:
1. El **modelo de ejecución de Spark**: driver y ejecutores, evaluación perezosa, y por qué
   `pandas` + `iterrows` no escala.
2. **Idempotencia y Delta Lake**: transacciones ACID, `replaceWhere` y `MERGE`.
3. **Tipos de datos**: `Decimal` frente a `float` y esquemas explícitos.
4. **Pruebas con pytest** sobre transformaciones de Spark.
5. **Configuración externa y logging** (principios de *12-factor app*).

Cerraría acordando revisar juntos su siguiente PR en una semana.
