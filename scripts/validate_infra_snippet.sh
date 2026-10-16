#!/usr/bin/env bash
# Extract the HCL blocks of docs/infra.md into a temp project and run terraform fmt + validate.
# Requires the Terraform CLI; downloads the azurerm and databricks providers (no credentials).
# Only the harness file (variables of the root module + provider configs) is added here; the
# snippet files are exactly what the document shows.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
T=$(mktemp -d)
mkdir -p "$T/infra/modules/tenant" "$T/config"
cp -r "$REPO/config/tenants" "$T/config/"

python3 - "$REPO/docs/infra.md" "$T" <<'PY'
import re, sys, pathlib
text = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
root, tenant_main, tenant_versions = re.findall(r"```hcl\n(.*?)```", text, re.S)
t = pathlib.Path(sys.argv[2])
(t / "infra/main.tf").write_text(root)
(t / "infra/modules/tenant/main.tf").write_text(tenant_main)
(t / "infra/modules/tenant/versions.tf").write_text(tenant_versions)
PY

cat > "$T/infra/harness.tf" <<'HCL'
# Validation harness only (not part of the snippet).
terraform {
  required_providers {
    azurerm    = { source = "hashicorp/azurerm", version = "~> 4.0" }
    databricks = { source = "databricks/databricks", version = "~> 1.50" }
  }
}
provider "azurerm" {
  features {}
}
provider "databricks" {}
variable "env" { type = string }
variable "pipeline_sp" { type = string }
variable "storage" { type = object({ id = string, name = string, container = string }) }
variable "key_vault" { type = object({ id = string, uri = string }) }
HCL

echo "== terraform fmt -check (snippet files) =="
terraform fmt -check -diff "$T/infra/main.tf" "$T/infra/modules/tenant/main.tf" "$T/infra/modules/tenant/versions.tf" && echo "fmt OK"
echo "== terraform init + validate (root module, includes the tenant module) =="
cd "$T/infra"
terraform init -backend=false -input=false -no-color >/tmp/tf_init.log 2>&1 || { tail -20 /tmp/tf_init.log; exit 1; }
grep -E "Installed" /tmp/tf_init.log | sed 's/^- //' || true
terraform validate -no-color
rm -rf "$T"
