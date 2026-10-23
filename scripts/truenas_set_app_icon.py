#!/usr/bin/env python3
"""Give the TrueNAS app an icon in the Apps list.

TrueNAS gives every custom (YAML) app the same blank metadata, and the YAML
cannot set an icon. This writes the icon URL into the app's own metadata file
and asks TrueNAS to rebuild its app list. TrueNAS does not rewrite that file
when the app is edited or updated, so the icon stays until the app is
deleted. The icon is linked from Valve's Store, not copied into this repo.

Run it on TrueNAS (System > Shell, or ssh) as root:

    sudo python3 truenas_set_app_icon.py                      # steam-data-pipeline
    sudo python3 truenas_set_app_icon.py my-app-name          # another name
"""

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import yaml  # ships with TrueNAS

ICON = "https://store.steampowered.com/favicon.ico"  # 256 px Steam logo
APPS_DIR = Path("/mnt/.ix-apps/app_configs")
DEFAULT_APP = "steam-data-pipeline"


def midclt(*args):
    return subprocess.check_output(["midclt", "call", *args], text=True)


def set_icon(app):
    path = APPS_DIR / app / "metadata.yaml"

    if not path.is_file():
        raise SystemExit(f"{path} not found: is {app!r} the app's name?")

    data = yaml.safe_load(path.read_text())

    if not isinstance(data, dict) or not isinstance(data.get("metadata"), dict):
        raise SystemExit(
            f"{path} does not look like TrueNAS app metadata; not touching it"
        )

    if data["metadata"].get("icon") == ICON:
        print(f"{app}: icon already set")
        return

    backup = path.with_name(
        f"metadata.yaml.bak-icon-{time.strftime('%Y%m%d-%H%M%S')}"
    )
    shutil.copy2(path, backup)
    data["metadata"]["icon"] = ICON
    info = path.stat()
    tmp = path.with_name("metadata.yaml.tmp-icon")
    tmp.write_text(
        yaml.safe_dump(data, default_flow_style=False, sort_keys=False)
    )
    os.chmod(tmp, info.st_mode & 0o7777)
    os.chown(tmp, info.st_uid, info.st_gid)
    os.replace(tmp, path)
    print(f"{app}: icon set (backup: {backup})")


def main():
    if os.geteuid() != 0:
        raise SystemExit("Run this with sudo on TrueNAS.")

    apps = sys.argv[1:] or [DEFAULT_APP]

    for app in apps:
        set_icon(app)

    # Rebuild TrueNAS's combined app list from the per-app files. The private
    # job is app.metadata.generate up to 25.10 and app.metadata_generate later.
    for method in ("app.metadata.generate", "app.metadata_generate"):
        result = subprocess.run(
            ["midclt", "call", "-j", method], capture_output=True, text=True
        )

        if result.returncode == 0:
            break
    else:
        raise SystemExit(
            "TrueNAS did not rebuild its app list: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )

    for app in apps:
        rows = json.loads(midclt(
            "app.query", json.dumps([["name", "=", app]]),
            '{"select": ["metadata"]}'
        ))
        icon = rows[0]["metadata"].get("icon") if rows else None
        print(f"{app}: "
              f"{'icon active' if icon == ICON else 'icon NOT active, check the output above'}")

    print("Reload the TrueNAS Apps page to see it.")


if __name__ == "__main__":
    main()
