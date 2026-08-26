"""TPUSwarm control-plane entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import os

from tpuswarm.backend import SkyPilotBackend
from tpuswarm.builtin import register_builtin_handlers
from tpuswarm.controller import TPUSwarmController
from tpuswarm.handlers import TaskRegistry
from tpuswarm.store import SwarmStore


def _registry(modules: list[str]) -> TaskRegistry:
    registry = TaskRegistry()
    register_builtin_handlers(registry)
    for module_name in modules:
        module = importlib.import_module(module_name)
        register = getattr(module, "register", None)
        if register is None:
            raise ValueError(
                f"registry module {module_name} has no register(registry) function"
            )
        register(registry)
    return registry


def _controller(args: argparse.Namespace) -> TPUSwarmController:
    return TPUSwarmController(
        SwarmStore(args.database),
        _registry(args.registry_module),
        SkyPilotBackend(),
        submission_grace_seconds=args.submission_grace_seconds,
        reconcile_seconds=args.reconcile_seconds,
    )


def _serve(args: argparse.Namespace) -> None:
    try:
        import uvicorn
    except ImportError as exc:
        raise RuntimeError("install TPUSwarm with its 'server' extra") from exc
    from tpuswarm.api import create_app

    controller = _controller(args)
    token = None if args.insecure_no_auth else os.environ.get(args.token_env)
    if token is None and not args.insecure_no_auth:
        raise ValueError(
            f"{args.token_env} must be set, or pass --insecure-no-auth for local use"
        )
    app = create_app(
        controller.store,
        controller.registry,
        controller,
        bearer_token=token,
    )
    uvicorn.run(app, host=args.host, port=args.port)


def _reconcile_once(args: argparse.Namespace) -> None:
    result = asyncio.run(_controller(args).reconcile_once())
    print(result)


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--database", required=True)
    parser.add_argument("--registry-module", action="append", default=[])
    parser.add_argument("--submission-grace-seconds", type=float, default=120)
    parser.add_argument("--reconcile-seconds", type=float, default=10)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tpuswarm")
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve")
    _common(serve)
    serve.add_argument("--host", default="0.0.0.0")
    serve.add_argument("--port", type=int, default=8787)
    serve.add_argument("--token-env", default="TPUSWARM_TOKEN")
    serve.add_argument("--insecure-no-auth", action="store_true")
    serve.set_defaults(func=_serve)

    reconcile = subparsers.add_parser("reconcile-once")
    _common(reconcile)
    reconcile.set_defaults(func=_reconcile_once)
    return parser


def main() -> None:
    args = _parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args.func(args)


if __name__ == "__main__":
    main()
