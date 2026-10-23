"""Read-only review of the current index before checkpoint commit/tag/push."""
import json
import argparse
from pathlib import Path
import re
import subprocess
from src.common import ROOT, utc_now, write_json


def git(*args, input=None):
    return subprocess.run(["git", "-c", "safe.directory=" + str(ROOT), "-C", str(ROOT), *args], input=input,
                          capture_output=True, text=True, encoding="utf-8", errors="replace")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-write", action="store_true", help="Check final index without changing saved evidence")
    args = parser.parse_args()
    staged = git("diff", "--cached", "--name-only", "-z").stdout.split("\0")
    files = []
    problems = []
    secret = re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,}|AKIA[0-9A-Z]{16}|https?://[^\s/@:]+:[^\s/@]+@")
    for name in filter(None, staged):
        content = git("show", ":" + name)
        size_result = git("cat-file", "-s", ":" + name)
        size = int(size_result.stdout.strip())
        path = Path(name)
        forbidden = ((name.startswith(("datasets/", "data/", "logs/")) and path.name != ".gitkeep")
                     or name.startswith((".venv/", ".tools/")) or (path.name.startswith(".env") and path.name != ".env.example")
                     or path.suffix.lower() in (".csv", ".zip", ".parquet", ".pem", ".key", ".p12", ".pfx"))
        if size > 1024 * 1024:
            problems.append({"file": name, "reason": "staged blob over 1 MiB"})
        if forbidden:
            problems.append({"file": name, "reason": "forbidden dataset/runtime/secret path"})
        if secret.search(content.stdout):
            problems.append({"file": name, "reason": "possible secret signature (value not printed)"})
        files.append({"path": name, "staged_bytes": size})
    probes = ["datasets/usda_branded_2026/csv/branded_food.csv", "datasets/usda_branded_2026/json/FoodData_Central_branded_food_json_2026-04-30.zip",
              "data/raw/example/test.csv", ".env", ".env.production", ".venv/Scripts/python.exe", ".tools/runtime.json", "logs/checkpoint2/ingestion.log"]
    ignored = []
    for probe in probes:
        result = git("check-ignore", "--no-index", "-v", probe)
        passed = result.returncode == 0
        ignored.append({"path": probe, "ignored": passed, "matched_rule": result.stdout.strip()})
        if not passed:
            problems.append({"file": probe, "reason": "not ignored"})
    remotes = git("remote").stdout.splitlines()
    evidence = {"checked_at_utc": utc_now(), "status": "PASS" if files and not problems else "FAIL",
                "staged_files": files, "staged_count": len(files), "max_staged_bytes": max((x["staged_bytes"] for x in files), default=0),
                "ignore_probes": ignored, "problems": problems, "remote_names": remotes,
                "remote_credentials_check": "NOT_APPLICABLE_NO_REMOTE" if not remotes else "REQUIRES_REMOTE_SPECIFIC_REVIEW_BEFORE_PUSH",
                "secret_scan_scope": "Staged text content: private key blocks, GitHub/AWS token signatures and embedded URL credentials. No claim of exhaustive secret detection.",
                "push_performed": False}
    if not args.no_write:
        write_json(ROOT / "docs/evidence/git_safety.json", evidence)
    print(json.dumps({"status": evidence["status"], "staged_count": len(files), "max_staged_bytes": evidence["max_staged_bytes"],
                      "remote_names": remotes, "problems": problems}, indent=2))
    return 0 if evidence["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
