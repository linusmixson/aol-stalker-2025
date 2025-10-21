from pathlib import Path


def package_root() -> Path:
    return Path(__file__).parent


def static_root() -> Path:
    return package_root() / "static"
