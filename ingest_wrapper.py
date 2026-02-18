#!/usr/bin/env python3
"""
Agentica Phase 1 — Manual Batch Ingestion Wrapper
=================================================

Purpose:
--------
A lightweight manual controller for `agentica_phase1.py`.  
Reads from `urls_pending.txt`, runs ingestion for each URL not marked ✅,
and updates the same file with success or failure status inline.

Features:
---------
• Skips completed URLs by default (marked with ✅ SUCCESS)
• Retries previously failed ones (marked ❌ FAILED) unless --nada flag is used
• Real-time terminal feedback via tqdm progress bar
• Writes completion results inline into the pending file
• Logs all details to logs/ingest_wrapper.log

Usage:
------
    python ingest_wrapper.py
    python ingest_wrapper.py --nada     # Skip failed URLs, only process new ones
    python ingest_wrapper.py --limit 5  # Process first 5 pending URLs

File Conventions:
-----------------
urls_pending.txt example:

    # Agentica ingestion queue
    https://example.com/article1
    https://example.com/article2   ✅ SUCCESS [doc_id=4a82c3...]
    https://example.com/article3   ❌ FAILED [Timeout]
    https://symmetrymagazine.org/article/the-problem-solver-cosmic-inflation

After running, updated file:

    https://example.com/article1   ✅ SUCCESS [doc_id=4a82c3..., 3 chunks, 2025-11-03T14:42Z]
    https://example.com/article3   ❌ FAILED [Timeout after 30s, 2025-11-03T14:43Z]
    https://symmetrymagazine.org/article/the-problem-solver-cosmic-inflation   ✅ SUCCESS [doc_id=baf12e..., 5 chunks, 2025-11-03T14:45Z]

Dependencies:
-------------
• tqdm
• The `agentica_phase1.py` script in the same directory.

Note:
-----
This wrapper *imports* the `process_url()` function from agentica_phase1.py,
so it does not spawn new subprocesses — it runs cleanly in one process.
"""

import os, re, argparse, datetime as dt
from tqdm import tqdm

# Import the ingestion logic directly
import agentica_phase1 as phase1


# =========================================================
# Utility Functions
# =========================================================

def now_iso():
    """ISO timestamp (UTC) for marking results."""
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def read_urls_file(path="urls_pending.csv"):
    """
    Read URLs and metadata (level, era, status) from a CSV file.
    Expected columns: url, level, era [, status]
    Falls back to default values if missing.
    """
    import csv, os

    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found.")

    entries = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)

        # -----------------------------------------------------------------
        # Detect header row (optional) and normalize expected positions
        # -----------------------------------------------------------------
        has_header = header and "url" in ",".join(header).lower()
        if has_header:
            rows = [row for row in reader]
        else:
            rows = [header] + [row for row in reader] if header else []

        # -----------------------------------------------------------------
        # Build a unified entries list of dicts
        # -----------------------------------------------------------------
        for row in rows:
            if not row or not row[0].strip():
                continue
            url = row[0].strip()
            level = row[1].strip() if len(row) > 1 else "HS"
            era = row[2].strip() if len(row) > 2 else "Cosmic Inflation"
            status = row[3].strip() if len(row) > 3 else "pending"

            entries.append({
                "url": url,
                "level": level,
                "era": era,
                "status": status
            })

    return entries

def write_urls_file(entries, path="urls_pending.csv"):
    """
    Write updated URL entries back to CSV.
    Each entry should be a dict with url, level, era, status.
    """
    import csv

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Header row
        writer.writerow(["url", "level", "era", "status"])
        # Data rows
        for e in entries:
            if not e.get("url"):
                continue
            writer.writerow([
                e.get("url", ""),
                e.get("level", ""),
                e.get("era", ""),
                e.get("status", "pending")
            ])


def update_entry(entries, url, new_status, note):
    """Update an entry's status annotation."""
    for e in entries:
        if e["url"] == url:
            e["line"] = f"{url}   {new_status} {note}"
            e["status"] = "done" if "✅" in new_status else "failed"
            break


def ensure_logs_dir():
    """Ensure logs directory exists."""
    os.makedirs("logs", exist_ok=True)


# =========================================================
# Main Logic
# =========================================================

def main():
    ap = argparse.ArgumentParser(description="Manual ingestion wrapper for Agentica Phase 1")
    ap.add_argument("--nada", action="store_true",
                    help="Do NOT retry previously failed URLs (skip ❌ lines)")
    ap.add_argument("--limit", type=int, default=None,
                    help="Only process first N eligible URLs")
    args = ap.parse_args()

    ensure_logs_dir()
    log_path = os.path.join("logs", "ingest_wrapper.log")

    # Read URL entries
    entries = read_urls_file()
    pending = [e for e in entries if e["status"] in ("pending", "failed")]

    # Apply filters
    if args.nada:
        pending = [e for e in pending if e["status"] == "pending"]

    if args.limit:
        pending = pending[: args.limit]

    if not pending:
        print("✅ Nothing to do — all URLs completed or skipped.")
        return

    print(f"🚀 Starting ingestion batch for {len(pending)} URL(s)...\n")

    for e in tqdm(pending, desc="Ingesting URLs", ncols=90):
        url = e["url"]
        tqdm.write(f"\n⚙️ Processing: {url}")
        update_entry(entries, url, "⚙️ IN PROGRESS", "")
        write_urls_file(entries)

        try:
            # Pass metadata fields from the current entry (CSV row)
            info = phase1.process_url(e["url"], e["level"], e["era"])
            ts = now_iso()
            note = f"[doc_id={info['doc_id']}, {info['chunks']} chunks, {ts}]"
            update_entry(entries, url, "✅ SUCCESS", note)
            tqdm.write(f"✅ SUCCESS: {info['title']} ({info['chunks']} chunks)")
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(f"[{ts}] SUCCESS {url} -> {note}\n")

        except Exception as err:
            ts = now_iso()
            note = f"[{err.__class__.__name__}: {err}, {ts}]"
            update_entry(entries, url, "❌ FAILED", note)
            tqdm.write(f"❌ FAILED: {url} ({err})")
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(f"[{ts}] FAILED {url} -> {repr(err)}\n")

        # Persist progress incrementally
        write_urls_file(entries)

    print("\n🎯 Batch complete. Check urls_pending.txt for results.")
    print(f"📄 Detailed log: {log_path}")


if __name__ == "__main__":
    main()

