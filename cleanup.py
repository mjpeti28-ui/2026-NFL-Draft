"""
Claim taxonomy cleanup script.

Reads every Claim record, uses Claude to normalise Claim Type and Category to
the approved taxonomy, and optionally rescues records where claim text was
accidentally placed in the Claim Type field.

Usage:
    python cleanup.py               # real run — writes to Airtable
    python cleanup.py --dry-run     # preview only, writes cleanup_preview.json
    python cleanup.py --limit 100   # process first N claims only
    python cleanup.py --offset 500  # skip first N claims (resume partial run)
"""

import argparse
import json
import time
import sys
import textwrap
from datetime import datetime

import config
from pipeline.airtable import _get_all, update_records_batch
from pipeline.claims import _get_client, _parse_json_response

# ---------------------------------------------------------------------------
# Taxonomy
# ---------------------------------------------------------------------------

VALID_TYPES = config.VALID_CLAIM_TYPES   # 14 options
VALID_CATS  = config.VALID_CATEGORIES    # 28 options

# A Claim Type value is almost certainly a "swapped" sentence if it's long
SENTENCE_THRESHOLD = 50  # chars

# ---------------------------------------------------------------------------
# Claude prompt
# ---------------------------------------------------------------------------

CLEANUP_PROMPT = """\
You are normalising NFL draft scouting claim records. For each record, assign:
- new_type: exactly one value from the VALID TYPES list
- new_category: exactly one value from the VALID CATEGORIES list
- new_claim_text: see rules below (often null)

VALID TYPES (pick exactly one):
{types}

VALID CATEGORIES (pick exactly one):
{cats}

RULES:
1. Choose the type and category that best describe the claim content.
2. Biographical noise, recruiting grades, raw performance metrics, or background
   facts → type="Context", category="Background / Bio".
3. SWAPPED FIELD RESCUE: if `claim_type` looks like a full sentence (not a
   category label) AND `claim_text` is empty or identical to `claim_type`:
   - Set new_claim_text to the sentence from claim_type
   - Choose new_type based on the content of that sentence
   Otherwise set new_claim_text to null (do NOT touch claim_text).
4. Measurement data (height, weight, arm length, combine numbers) →
   type="Measurement", category="Combine Results" or "Size / Measurements".
5. Player comparisons → type="Comparison", category="Player Comparison".
6. Never invent new type or category values — only use values from the lists.

INPUT RECORDS (JSON array):
{records}

Return ONLY a JSON array — one object per input record — with keys:
  id, new_type, new_category, new_claim_text (string or null)
No explanation, no markdown fences.
"""


def normalize_batch(batch: list[dict]) -> list[dict]:
    """Send a batch to Claude, return list of {id, new_type, new_category, new_claim_text}."""
    client = _get_client()
    prompt = CLEANUP_PROMPT.format(
        types="\n".join(f"  - {t}" for t in VALID_TYPES),
        cats="\n".join(f"  - {c}" for c in VALID_CATS),
        records=json.dumps(batch, ensure_ascii=False, indent=2),
    )
    msg = client.messages.create(
        model=config.CLAUDE_MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text.strip()
    return _parse_json_response(raw)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_sentence(text: str) -> bool:
    """Heuristic: looks more like a sentence than a category label."""
    if not text:
        return False
    text = text.strip()
    if len(text) > SENTENCE_THRESHOLD:
        return True
    # Short but contains sentence-like punctuation
    if any(c in text for c in [".", ",", ";", ":", "("]):
        return True
    return False


def build_update(record: dict, result: dict) -> dict | None:
    """
    Return an Airtable update dict {id, fields} if anything changed, else None.
    'record' has the original values; 'result' has Claude's suggestions.
    """
    fields: dict = {}

    new_type = (result.get("new_type") or "").strip()
    new_cat  = (result.get("new_category") or "").strip()
    new_text = result.get("new_claim_text")  # None means "don't touch"

    # Validate against taxonomy (Claude occasionally hallucinates)
    if new_type not in VALID_TYPES:
        new_type = "Trait"
    if new_cat not in VALID_CATS:
        new_cat = "Overall Grade"

    if new_type != (record.get("claim_type") or "").strip():
        fields[config.F_CLAIM_TYPE] = new_type
    if new_cat != (record.get("category") or "").strip():
        fields[config.F_CLAIM_CATEGORY] = new_cat
    if new_text is not None and new_text.strip():
        existing = (record.get("claim_text") or "").strip()
        if not existing or existing == (record.get("claim_type") or "").strip():
            fields[config.F_CLAIM_TEXT] = new_text.strip()

    if not fields:
        return None
    return {"id": record["id"], "fields": fields}


def fmt_change(old: str, new: str) -> str:
    old = (old or "").strip()[:35]
    new = (new or "").strip()[:35]
    if old == new:
        return old
    return f"{old} → {new}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Normalise Claim Type and Category fields.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes; write cleanup_preview.json. No Airtable writes.")
    parser.add_argument("--limit",  type=int, default=0,
                        help="Process at most N claims (0 = all).")
    parser.add_argument("--offset", type=int, default=0,
                        help="Skip the first N claims.")
    parser.add_argument("--batch-size", type=int, default=25,
                        help="Claims per Claude call (default 25).")
    args = parser.parse_args()

    mode = "DRY-RUN" if args.dry_run else "REAL RUN"
    print(f"\n🏈  Claim Taxonomy Cleanup  [{mode}]")
    print(f"   Model   : {config.CLAUDE_MODEL}")
    print(f"   Batch   : {args.batch_size} claims/call")
    if args.limit:
        print(f"   Limit   : {args.limit} claims")
    if args.offset:
        print(f"   Offset  : skip first {args.offset}")
    print()

    # ------------------------------------------------------------------
    # 1. Fetch all claims
    # ------------------------------------------------------------------
    print("Fetching claims from Airtable...", end=" ", flush=True)
    t0 = time.time()
    raw_records = _get_all(
        config.TABLE_CLAIM,
        [config.F_CLAIM_TEXT, config.F_CLAIM_TYPE, config.F_CLAIM_CATEGORY],
    )
    print(f"{len(raw_records)} records ({time.time()-t0:.1f}s)")

    # Normalise into flat dicts
    all_claims = []
    for r in raw_records:
        f = r.get("fields", {})
        all_claims.append({
            "id":         r["id"],
            "claim_text": f.get(config.F_CLAIM_TEXT, ""),
            "claim_type": f.get(config.F_CLAIM_TYPE, ""),
            "category":   f.get(config.F_CLAIM_CATEGORY, ""),
        })

    # Apply offset / limit
    claims = all_claims[args.offset:]
    if args.limit:
        claims = claims[:args.limit]

    total = len(claims)
    n_batches = (total + args.batch_size - 1) // args.batch_size
    print(f"Processing {total} claims in {n_batches} batches...\n")

    # ------------------------------------------------------------------
    # 2. Process batches
    # ------------------------------------------------------------------
    all_updates: list[dict] = []
    rescues = type_changes = cat_changes = skipped = errors = 0
    preview_rows: list[dict] = []

    def _progress(done: int, total: int, elapsed: float, suffix: str = "") -> None:
        """Print a compact inline progress bar."""
        pct = done / total if total else 1
        bar_w = 35
        filled = int(bar_w * pct)
        bar = "█" * filled + "░" * (bar_w - filled)
        eta_str = ""
        if done > 0 and done < total:
            eta = elapsed / done * (total - done)
            m, s = divmod(int(eta), 60)
            eta_str = f"  ETA {m}m{s:02d}s"
        mins, secs = divmod(int(elapsed), 60)
        sys.stdout.write(
            f"\r  [{bar}] {pct:>5.1%}  {done}/{total} claims"
            f"  {mins}m{secs:02d}s{eta_str}  {suffix:<25}"
        )
        sys.stdout.flush()

    for b_idx in range(n_batches):
        start = b_idx * args.batch_size
        batch_claims = claims[start:start + args.batch_size]

        processed_so_far = b_idx * args.batch_size
        _progress(processed_so_far, total, time.time() - t0,
                  f"batch {b_idx+1}/{n_batches}")

        # Build compact input for Claude (only what it needs)
        claude_input = [
            {
                "id":         c["id"],
                "claim_text": c["claim_text"],
                "claim_type": c["claim_type"],
                "category":   c["category"],
            }
            for c in batch_claims
        ]

        try:
            results = normalize_batch(claude_input)
        except Exception as e:
            errors += len(batch_claims)
            _progress(processed_so_far + len(batch_claims), total, time.time() - t0,
                      f"ERROR: {str(e)[:20]}")
            continue

        # Match results back to original records
        result_by_id = {r["id"]: r for r in results if isinstance(r, dict)}

        batch_updates = []
        for c in batch_claims:
            res = result_by_id.get(c["id"])
            if not res:
                skipped += 1
                continue

            upd = build_update(c, res)
            if upd is None:
                skipped += 1
                continue

            # Count change types
            if config.F_CLAIM_TYPE in upd["fields"]:
                type_changes += 1
            if config.F_CLAIM_CATEGORY in upd["fields"]:
                cat_changes += 1
            if config.F_CLAIM_TEXT in upd["fields"]:
                rescues += 1

            batch_updates.append(upd)

            # Build preview row
            preview_rows.append({
                "id":       c["id"],
                "old_type": c["claim_type"],
                "new_type": upd["fields"].get(config.F_CLAIM_TYPE, c["claim_type"]),
                "old_cat":  c["category"],
                "new_cat":  upd["fields"].get(config.F_CLAIM_CATEGORY, c["category"]),
                "rescued":  config.F_CLAIM_TEXT in upd["fields"],
                "claim":    c["claim_text"][:80] or c["claim_type"][:80],
            })

        all_updates.extend(batch_updates)

    # Final bar at 100%
    _progress(total, total, time.time() - t0, "done")
    print()  # newline after bar

    # ------------------------------------------------------------------
    # 3. Apply updates or preview
    # ------------------------------------------------------------------
    print(f"\n{'─'*60}")
    print(f"  Total claims processed : {total}")
    print(f"  Type field changes     : {type_changes}")
    print(f"  Category changes       : {cat_changes}")
    print(f"  Field rescues          : {rescues}")
    print(f"  No-op (unchanged)      : {skipped}")
    print(f"  Errors (skipped)       : {errors}")
    print(f"  Total updates queued   : {len(all_updates)}")
    print(f"{'─'*60}\n")

    if args.dry_run:
        # Print sample table
        sample = preview_rows[:50]
        if sample:
            print(f"Sample of changes (first {len(sample)}):\n")
            print(f"  {'ID':<14}  {'TYPE CHANGE':<40}  {'CAT CHANGE':<40}  R  CLAIM")
            print(f"  {'-'*14}  {'-'*40}  {'-'*40}  -  -----")
            for row in sample:
                tc = fmt_change(row["old_type"], row["new_type"])
                cc = fmt_change(row["old_cat"], row["new_cat"])
                r  = "✓" if row["rescued"] else " "
                cl = (row["claim"] or "")[:50]
                print(f"  {row['id']:<14}  {tc:<40}  {cc:<40}  {r}  {cl}")

        # Write full preview JSON
        preview_path = "cleanup_preview.json"
        with open(preview_path, "w") as f:
            json.dump({
                "generated_at": datetime.utcnow().isoformat() + "Z",
                "total_processed": total,
                "total_updates": len(all_updates),
                "type_changes": type_changes,
                "category_changes": cat_changes,
                "rescues": rescues,
                "updates": all_updates,
                "preview": preview_rows,
            }, f, indent=2)
        print(f"\n✓ Preview written to {preview_path}")
        print("  Run without --dry-run to apply changes.\n")

    else:
        if not all_updates:
            print("Nothing to update.\n")
            return

        print(f"Writing {len(all_updates)} updates to Airtable...", flush=True)
        t_write = time.time()
        write_batches = (len(all_updates) + 9) // 10
        for i in range(0, len(all_updates), 10):
            chunk = all_updates[i:i + 10]
            update_records_batch(config.TABLE_CLAIM, chunk)
            done = min(i + 10, len(all_updates))
            b_num = i // 10 + 1
            sys.stdout.write(
                f"\r  Write batch {b_num}/{write_batches}  ({done}/{len(all_updates)} records)"
            )
            sys.stdout.flush()

        print(f"\n\n✓ Done in {time.time()-t_write:.1f}s")
        print(f"  {len(all_updates)} claim records updated in Airtable.\n")


if __name__ == "__main__":
    main()
