"""Command line entry point.

Examples:
    saas-pipeline run --env dev --tenant sv --start-date 2025-03-01 --end-date 2025-03-31
    saas-pipeline run --env qa --tenant all execution.fail_fast=true
    saas-pipeline validate-config
"""

from __future__ import annotations

import argparse
import logging
import sys

from saas_pipeline.config import ENVIRONMENTS, available_tenants, load_config
from saas_pipeline.pipeline import LAYER_ORDER, run


def _parse_layers(value: str) -> list[str]:
    layers = [layer.strip() for layer in value.split(",") if layer.strip()]
    unknown = set(layers) - set(LAYER_ORDER)
    if unknown:
        raise argparse.ArgumentTypeError(f"Unknown layers {sorted(unknown)}; use {LAYER_ORDER}")
    return layers


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="saas-pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    run_cmd = sub.add_parser("run", help="Run the pipeline")
    run_cmd.add_argument("--env", choices=ENVIRONMENTS, default="dev")
    run_cmd.add_argument(
        "--tenant", help="Tenant code (lowercase) or 'all'. Default: execution.tenant"
    )
    run_cmd.add_argument("--start-date", help="First fecha_proceso, YYYY-MM-DD")
    run_cmd.add_argument("--end-date", help="Last fecha_proceso, YYYY-MM-DD")
    run_cmd.add_argument(
        "--layers", type=_parse_layers, default=list(LAYER_ORDER), help="e.g. bronze,silver"
    )
    run_cmd.add_argument(
        "overrides", nargs="*", help="Config overrides, e.g. execution.fail_fast=true"
    )

    sub.add_parser("validate-config", help="Load every env x tenant combination")
    return parser


def validate_all() -> int:
    for env in ENVIRONMENTS:
        for tenant in available_tenants():
            load_config(env, tenant)
    print(f"OK: {len(ENVIRONMENTS)} envs x {len(available_tenants())} tenants")
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = build_parser().parse_args(argv)
    if args.command == "validate-config":
        return validate_all()

    overrides = list(args.overrides)
    if args.start_date:
        overrides.append(f"execution.start_date={args.start_date}")
    if args.end_date:
        overrides.append(f"execution.end_date={args.end_date}")
    return run(args.env, args.tenant, args.layers, overrides)


if __name__ == "__main__":
    sys.exit(main())
