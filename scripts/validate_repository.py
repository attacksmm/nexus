#!/usr/bin/env python3
import json
import py_compile
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_MANIFEST_KEYS = {"id", "name", "version"}
FORBIDDEN_PARTS = {"data", "logs", "uploads", "backups", "__pycache__"}
FORBIDDEN_SUFFIXES = {".db", ".sqlite", ".sqlite3", ".pyc", ".zip"}


def tracked_files() -> list[Path]:
    output = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT)
    return [Path(raw.decode()) for raw in output.split(b"\0") if raw]


def validate_tracked_files(paths: list[Path]) -> None:
    forbidden = []
    for path in paths:
        if path.name == ".env" or path.name.startswith(".env."):
            forbidden.append(path)
        elif FORBIDDEN_PARTS.intersection(path.parts) or path.suffix.lower() in FORBIDDEN_SUFFIXES:
            forbidden.append(path)
    if forbidden:
        raise SystemExit("Runtime/build files tracked by Git:\n" + "\n".join(map(str, forbidden)))


def validate_python(paths: list[Path]) -> None:
    for path in paths:
        if path.suffix == ".py":
            py_compile.compile(ROOT / path, doraise=True)


def validate_manifests() -> None:
    for path in sorted(ROOT.glob("module_*/manifest.json")):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        missing = REQUIRED_MANIFEST_KEYS - manifest.keys()
        if missing:
            raise SystemExit(f"{path.relative_to(ROOT)}: missing {sorted(missing)}")
        module_id = str(manifest["id"])
        if not module_id.replace("-", "_").isidentifier():
            raise SystemExit(f"{path.relative_to(ROOT)}: invalid module id {module_id!r}")
        expected_dir = "module_" + module_id.replace("-", "_")
        if path.parent.name != expected_dir:
            raise SystemExit(f"{path.relative_to(ROOT)}: expected directory {expected_dir}")


def main() -> None:
    paths = tracked_files()
    validate_tracked_files(paths)
    validate_python(paths)
    validate_manifests()
    print(f"Repository validation passed: {len(paths)} tracked files")


if __name__ == "__main__":
    main()
