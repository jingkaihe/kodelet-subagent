"""Maintenance commands for the subagent extension."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from .install import install_plugin


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kodelet-subagent")
    subcommands = parser.add_subparsers(dest="command", required=True)

    install = subcommands.add_parser(
        "install",
        help="Install the Kodelet plugin wrapper in ~/.kodelet/plugins",
    )
    install.add_argument(
        "--home",
        type=Path,
        help="Override the home directory used for installation",
    )

    migrate = subcommands.add_parser(
        "migrate",
        help="Upgrade a subagent SQLite database to the latest Alembic revision",
    )
    migrate.add_argument("database", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "install":
        executable = install_plugin(home=args.home)
        print(f"Installed Kodelet subagent extension at {executable}")
        return 0

    from .persistence import migrate_database

    database = args.database.expanduser().resolve()
    migrate_database(database)
    print(f"Migrated {database}")
    return 0
