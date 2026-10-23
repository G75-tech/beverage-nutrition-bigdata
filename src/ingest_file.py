"""Batch-copy CSV bytes into immutable raw storage with checksum deduplication."""
from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
import re
import shutil
import sys
import uuid

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.common import ROOT, load_config, logger, sha256, utc_now, write_json


def quality_gate(path, specification, sample_limit):
    csv.field_size_limit(16 * 1024 * 1024)
    path = Path(path)
    if not path.is_file():
        raise ValueError(f"Missing regular source file: {path}")
    if path.stat().st_size == 0:
        raise ValueError("Source has zero bytes")
    if path.suffix.lower() != ".csv":
        raise ValueError("Only CSV files are accepted")
    if sample_limit < 1:
        raise ValueError("type_sample_rows must be positive")
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        header = next(reader, [])
        if not header or any(not name for name in header) or len(set(header)) != len(header):
            raise ValueError("Empty or duplicate header")
        expected = specification["columns"]
        if header != expected:
            raise ValueError(f"Header mismatch: expected={expected!r}, actual={header!r}")
        indices = {name: header.index(name) for name in specification.get("types", {})}
        count = 0
        for row in reader:
            if len(row) != len(header):
                raise ValueError(f"CSV width mismatch at record {count + 2}")
            for name, kind in specification.get("types", {}).items():
                value = row[indices[name]]
                if value == "":
                    if name in specification.get("required_values", []):
                        raise ValueError(f"Missing required {name} at record {count + 2}")
                    continue
                if kind == "long":
                    number = int(value)
                    if not -(2 ** 63) <= number < 2 ** 63:
                        raise ValueError(f"Out-of-range long in {name}")
                elif kind == "double":
                    import math
                    if not math.isfinite(float(value)):
                        raise ValueError(f"Non-finite double in {name}")
                else:
                    raise ValueError(f"Unsupported gate type: {kind}")
            count += 1
            if count >= sample_limit:
                break
        if count == 0:
            raise ValueError("CSV contains only a header")
    return {"exists": True, "nonzero": True, "extension": "csv", "encoding": "utf-8-sig",
            "header": header, "sample_records_checked": count, "type_sample_limit": sample_limit,
            "types": specification.get("types", {}), "scope": "header and first N parsed records; not a full-data quality audit"}


def _events(path):
    if not path.exists():
        return []
    # Fail closed on a damaged journal. Do not silently forget previous raw copies.
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def ingest_batch(sources, config, root=ROOT, run_id=None):
    if not sources:
        raise ValueError("At least one source is required")
    root = Path(root).resolve()
    raw_root = (root / config["raw_dir"]).resolve()
    journal = root / config["manifest"]
    journal.parent.mkdir(parents=True, exist_ok=True)
    lock = journal.with_suffix(".lock")
    log = logger("ingestion")
    started = utc_now()
    run_id = run_id or started.replace("-", "").replace(":", "").replace(".", "") + "_" + uuid.uuid4().hex[:8]
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("Invalid run_id")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", config["dataset"]):
        raise ValueError("Invalid dataset identifier")
    # One writer at a time. A stale lock requires human inspection after a hard crash.
    descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    os.write(descriptor, json.dumps({"pid": os.getpid(), "started_at_utc": started}).encode())
    os.close(descriptor)
    events = []
    try:
        history = _events(journal)
        previous = {(e["source"], e["release"], e["dataset"], e["file_name"], e["sha256"]): e
                    for e in history if e.get("status") == "INGESTED"}
        for source in sources:
            source = Path(source).resolve()
            event = {"manifest_version": 1, "run_id": run_id, "source": config["source"],
                     "release": config["release"], "dataset": config["dataset"], "source_path": str(source),
                     "file_name": source.name, "started_at_utc": utc_now(), "destination": None,
                     "size_bytes": None, "sha256": None, "status": "FAILED"}
            partial = None
            try:
                specification = config["csv_schemas"].get(source.name)
                if specification is None:
                    raise ValueError(f"No declared schema for {source.name}")
                event["quality_gate"] = quality_gate(source, specification, config["type_sample_rows"])
                before = source.stat()
                event["size_bytes"] = before.st_size
                event["sha256"] = sha256(source)
                key = (config["source"], config["release"], config["dataset"], source.name, event["sha256"])
                prior = previous.get(key)
                if prior:
                    destination = (root / prior["destination"]).resolve()
                    if not destination.is_relative_to(raw_root):
                        raise ValueError("Prior manifest destination escapes raw storage")
                    event["destination"] = destination.relative_to(root).as_posix()
                    if (not destination.is_file() or destination.stat().st_size != before.st_size
                            or sha256(destination) != event["sha256"]):
                        raise ValueError("Prior raw copy is missing or corrupt; refusing silent overwrite")
                    event.update(status="SKIPPED_DUPLICATE", duplicate_of_run_id=prior["run_id"])
                else:
                    destination = (raw_root / config["dataset"] / ("ingest_date=" + started[:10])
                                   / ("run_id=" + run_id) / source.name)
                    event["destination"] = destination.relative_to(root).as_posix()
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    if source == destination or source.is_relative_to(raw_root):
                        raise ValueError("Source must be outside raw storage")
                    if destination.exists():
                        raise FileExistsError(f"Immutable raw path already exists: {destination}")
                    partial = destination.with_name(destination.name + ".partial")
                    with source.open("rb") as inp, partial.open("xb") as out:
                        shutil.copyfileobj(inp, out, length=8 * 1024 * 1024)
                        out.flush()
                        os.fsync(out.fileno())
                    if sha256(partial) != event["sha256"] or partial.stat().st_size != before.st_size:
                        raise ValueError("Raw checksum/size mismatch")
                    after = source.stat()
                    if (after.st_size, after.st_mtime_ns) != (before.st_size, before.st_mtime_ns):
                        raise ValueError("Source changed during ingestion")
                    # On Windows rename fails if the destination exists; never replace raw.
                    partial.rename(destination)
                    partial = None
                    event["status"] = "INGESTED"
                    previous[key] = event
                event["raw_sha256_verified"] = True
            except Exception as exc:
                event["error"] = f"{type(exc).__name__}: {exc}"
            finally:
                if partial is not None and partial.exists():
                    partial.unlink()  # Only the temporary copy created by this operation.
                event["finished_at_utc"] = utc_now()
                with journal.open("a", encoding="utf-8", newline="\n") as stream:
                    stream.write(json.dumps(event, ensure_ascii=False) + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                events.append(event)
                log.info("run=%s status=%s file=%s size=%s sha256=%s destination=%s error=%s",
                         run_id, event["status"], source.name, event["size_bytes"], event["sha256"],
                         event["destination"], event.get("error", ""))
            if event["status"] == "FAILED":
                break  # Fail fast: later files are not accepted after a failed source.
    finally:
        lock.unlink()
    summary = {"run_id": run_id, "started_at_utc": started, "finished_at_utc": utc_now(),
               "status": "FAIL" if any(e["status"] == "FAILED" for e in events) else "PASS", "events": events}
    write_json(root / "logs/checkpoint2" / (run_id + ".json"), summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--source", nargs="+", type=Path, help="CSV files; default: four configured tables")
    parser.add_argument("--run-id")
    args = parser.parse_args()
    config = load_config(args.config)
    sources = args.source or [ROOT / config["input_dir"] / name for name in config["default_files"]]
    summary = ingest_batch(sources, config, run_id=args.run_id)
    print(json.dumps({"run_id": summary["run_id"], "status": summary["status"],
                      "events": [{"file": e["file_name"], "status": e["status"]} for e in summary["events"]]}, indent=2))
    return 0 if summary["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
