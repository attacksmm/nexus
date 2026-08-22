from __future__ import annotations

import json
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1] / "amocrm_widget"
OUTPUT = Path(__file__).resolve().parents[1] / "static" / "downloads" / "widget.zip"
REQUIRED = (
    "manifest.json", "script.js", "i18n/ru.json", "images/logo.png", "images/logo_main.png",
    "images/logo_medium.png", "images/logo_min.png", "images/logo_small.png",
)


def main() -> None:
    manifest = json.loads((ROOT / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("widget", {}).get("version") != "1.8.3":
        raise SystemExit("Unexpected amoCRM widget version")
    required_widget = {"name", "description", "short_description", "locale", "installation"}
    if not required_widget <= manifest.get("widget", {}).keys() or not isinstance(manifest.get("locations"), list):
        raise SystemExit("Incomplete amoCRM manifest")
    settings = manifest.get("settings")
    if not isinstance(settings, dict) or not settings:
        raise SystemExit("amoCRM manifest settings must be a non-empty object")
    for code, field in settings.items():
        if not isinstance(field, dict) or not isinstance(field.get("name"), str):
            raise SystemExit(f"Invalid amoCRM setting: {code}")
        if field.get("type") not in {"text", "pass", "users", "users_lp", "custom"}:
            raise SystemExit(f"Invalid amoCRM setting type: {code}")
        if not isinstance(field.get("required"), bool):
            raise SystemExit(f"Invalid amoCRM setting required flag: {code}")
    missing = [name for name in REQUIRED if not (ROOT / name).is_file()]
    if missing:
        raise SystemExit("Missing: " + ", ".join(missing))
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(OUTPUT, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in REQUIRED:
            archive.write(ROOT / name, name)
    with zipfile.ZipFile(OUTPUT) as archive:
        bad = archive.testzip()
        if bad:
            raise SystemExit(f"Bad ZIP entry: {bad}")
    print(OUTPUT)


if __name__ == "__main__":
    main()
