"""Executable entry point for the Kodelet extension."""

from __future__ import annotations


def main() -> None:
    from .extension import ext

    ext.run_sync()


if __name__ == "__main__":
    main()
