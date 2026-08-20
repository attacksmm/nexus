from __future__ import annotations

import json
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT.parent / "messenger-widget.zip"
EXCLUDED_PARTS = {"data", "dist", "output", "__pycache__", ".pytest_cache", "tests"}


def included(path: Path) -> bool:
    relative = path.relative_to(ROOT)
    if relative.as_posix() == "static/downloads/widget.zip":
        return True
    return not any(part in EXCLUDED_PARTS for part in relative.parts) and path.suffix not in {".pyc", ".pyo", ".zip"}


def main() -> None:
    manifest = json.loads((ROOT / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("id") != "messenger-widget" or manifest.get("replaces") != "getcourse-wazzup":
        raise SystemExit("Unexpected module manifest")
    files = sorted(path for path in ROOT.rglob("*") if path.is_file() and included(path))
    if any("data" in path.relative_to(ROOT).parts for path in files):
        raise SystemExit("Runtime data must not be packaged")
    with zipfile.ZipFile(OUTPUT, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in files:
            archive.write(path, path.relative_to(ROOT).as_posix())
    with zipfile.ZipFile(OUTPUT) as archive:
        if archive.testzip():
            raise SystemExit("Module ZIP CRC check failed")
        if (
            "manifest.json" not in archive.namelist()
            or "static/downloads/widget.zip" not in archive.namelist()
            or any(name.startswith("data/") for name in archive.namelist())
        ):
            raise SystemExit("Module ZIP contract failed")
    print(OUTPUT)


if __name__ == "__main__":
    main()
