"""Fetch checksum-verified portable Python 3.11 and Temurin JDK 17 into .tools.

Uses only the standard library. Runtime URLs/checksums are saved for reproducibility.
No registry, global PATH or system installation changes.
"""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tarfile
import urllib.request
import zipfile
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / ".tools"


def fetch_json(url):
    request = urllib.request.Request(url, headers={"User-Agent": "beverage-checkpoint2", "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=90) as response:
        return json.load(response)


def download(url, expected, name):
    path = TOOLS / "downloads" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and hashlib.file_digest(path.open("rb"), "sha256").hexdigest() == expected:
        print("Verified cached", name, flush=True)
        return path
    print("Downloading", url, flush=True)
    request = urllib.request.Request(url, headers={"User-Agent": "beverage-checkpoint2"})
    digest = hashlib.sha256()
    with urllib.request.urlopen(request, timeout=180) as response, path.open("wb") as out:
        while chunk := response.read(8 * 1024 * 1024):
            out.write(chunk)
            digest.update(chunk)
    if digest.hexdigest() != expected:
        raise ValueError(f"Download SHA-256 mismatch: {name}")
    print("SHA256 OK", expected, path.stat().st_size, flush=True)
    return path


def main():
    TOOLS.mkdir(exist_ok=True)
    evidence_path = ROOT / "docs/evidence/runtime_downloads.json"
    if evidence_path.exists():
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    else:
        release = fetch_json("https://api.github.com/repos/astral-sh/python-build-standalone/releases/latest")
        asset = next(a for a in release["assets"] if re.fullmatch(
            r"cpython-3\.11\.[0-9]+\+[^/]+-x86_64-pc-windows-msvc-install_only\.tar\.gz", a["name"]))
        digest = asset.get("digest", "") or ""
        if digest.startswith("sha256:"):
            checksum = digest.split(":", 1)[1]
        else:
            request = urllib.request.Request(asset["browser_download_url"] + ".sha256", headers={"User-Agent": "beverage-checkpoint2"})
            with urllib.request.urlopen(request, timeout=90) as response:
                checksum = response.read().decode().split()[0]
        jdk = fetch_json("https://api.adoptium.net/v3/assets/latest/17/hotspot?architecture=x64&image_type=jdk&os=windows&vendor=eclipse")[0]
        package = jdk["binary"]["package"]
        evidence = {"selected_at_utc": datetime.now(timezone.utc).isoformat(),
                    "python": {"url": asset["browser_download_url"], "sha256": checksum, "name": asset["name"],
                               "release": release["tag_name"], "distribution": "Astral python-build-standalone"},
                    "java": {"url": package["link"], "sha256": package["checksum"], "name": package["name"],
                             "version": jdk["version"]["semver"], "distribution": "Eclipse Temurin"}}
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    python_home = TOOLS / "python311"
    if not (python_home / "python/python.exe").exists():
        item = evidence["python"]
        archive = download(item["url"], item["sha256"], item["name"])
        python_home.mkdir(exist_ok=True)
        with tarfile.open(archive) as bundle:
            bundle.extractall(python_home, filter="data")
    java_home = next(iter((TOOLS / "java17").glob("*/bin/java.exe")), None)
    if java_home is None:
        item = evidence["java"]
        archive = download(item["url"], item["sha256"], item["name"])
        destination = TOOLS / "java17"
        destination.mkdir(exist_ok=True)
        with zipfile.ZipFile(archive) as bundle:
            for member in bundle.infolist():
                if not (destination / member.filename).resolve().is_relative_to(destination.resolve()):
                    raise ValueError("Unsafe archive path")
            bundle.extractall(destination)
        java_home = next((TOOLS / "java17").glob("*/bin/java.exe"))
    java_home = java_home.parents[1]
    python = python_home / "python/python.exe"
    subprocess.run([str(python), "--version"], check=True)
    subprocess.run([str(java_home / "bin/java.exe"), "-version"], check=True)
    runtime = {"python": python.relative_to(ROOT).as_posix(), "java_home": java_home.relative_to(ROOT).as_posix()}
    (TOOLS / "runtime.json").write_text(json.dumps(runtime, indent=2) + "\n", encoding="utf-8")
    evidence["verified_at_utc"] = datetime.now(timezone.utc).isoformat()
    evidence["status"] = "PASS"
    evidence_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
