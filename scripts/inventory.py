"""Read-only dataset inventory with hashes and ZIP member metadata."""
import csv
import json
from pathlib import Path
import subprocess
import zipfile
from src.common import ROOT, sha256, utc_now, write_json


def main():
    previous = json.loads((ROOT / "metadata/dataset_layout_manifest.json").read_text(encoding="utf-8"))
    reference = {f["path"]: f for f in previous["files"]}
    result = {"created_at_utc": utc_now(), "root": str(ROOT), "source_files": []}
    for path in sorted((ROOT / "datasets").rglob("*")):
        if not path.is_file() or path.name == ".gitkeep":
            continue
        relative = path.relative_to(ROOT).as_posix()
        stat = path.stat()
        entry = {"path": relative, "size_bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns, "sha256": sha256(path)}
        if relative in reference:
            entry["matches_previous_layout_sha256"] = reference[relative]["sha256"] == entry["sha256"]
        if path.suffix.lower() == ".csv":
            with path.open(encoding="utf-8-sig", newline="") as stream:
                reader = csv.reader(stream)
                entry["header"] = next(reader)
                entry["first_record_width"] = len(next(reader, []))
        elif path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path) as archive:
                entry["zip_members"] = [{"name": m.filename, "size_bytes": m.file_size, "compressed_bytes": m.compress_size, "crc32": f"{m.CRC:08x}"} for m in archive.infolist()]
            entry["zip_validation_scope"] = "Central directory read only; member CRC not fully tested"
        result["source_files"].append(entry)
    disk = __import__("shutil").disk_usage(ROOT)
    result["disk_free_bytes"] = disk.free
    result["git"] = {}
    for name, arguments in {"status": ["status", "--short", "--branch"], "remotes": ["remote"], "head": ["rev-parse", "--verify", "HEAD"]}.items():
        process = subprocess.run(["git", "-c", "safe.directory=" + str(ROOT), "-C", str(ROOT)] + arguments,
                                 capture_output=True, text=True)
        result["git"][name] = {"exit_code": process.returncode, "stdout": process.stdout, "stderr": process.stderr}
    write_json(ROOT / "docs/evidence/inventory_before.json", result)
    print(json.dumps({"files": len(result["source_files"]), "bytes": sum(f["size_bytes"] for f in result["source_files"]),
                      "free_bytes": disk.free, "all_previous_hashes_match": all(f.get("matches_previous_layout_sha256", True) for f in result["source_files"])}, indent=2))


if __name__ == "__main__":
    main()
