"""
reset_zoning_skips.py

Finds all deals skipped due to zoning reasons across every seen_deals*.json file
and resets them to pending so they get reprocessed on the next run.

Usage:
    python reset_zoning_skips.py           # preview only (dry run)
    python reset_zoning_skips.py --apply   # actually reset the entries
"""

import glob
import json
import os
import sys

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
APPLY = "--apply" in sys.argv


def reset_zoning_skips(path: str) -> int:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    changed = 0
    for lid, entry in data.items():
        reason = entry.get("skip_reason") or ""
        if "zoning" in reason.lower():
            print(f"  {'RESET' if APPLY else 'FOUND'}: {entry.get('address') or lid}")
            print(f"         skip_reason: {reason}")
            if APPLY:
                entry["skip_reason"] = None
                entry["processed"] = False
            changed += 1

    if APPLY and changed:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)

    return changed


def main():
    paths = glob.glob(os.path.join(DATA_DIR, "seen_deals*.json"))
    if not paths:
        print("No seen_deals*.json files found in data/")
        return

    if not APPLY:
        print("DRY RUN — pass --apply to actually reset entries\n")

    total = 0
    for path in sorted(paths):
        name = os.path.basename(path)
        print(f"\n{name}:")
        n = reset_zoning_skips(path)
        if n == 0:
            print("  (none)")
        total += n

    print(f"\n{'Reset' if APPLY else 'Found'} {total} zoning-skipped deal(s) across {len(paths)} file(s).")
    if total and not APPLY:
        print("Run with --apply to reset them.")


if __name__ == "__main__":
    main()
