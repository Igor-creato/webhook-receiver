#!/usr/bin/env python3
"""Read-only parity replay for the universal dedup resolver.

Replays the SAME fixture the plugin's DedupResolverParityTest.php uses, against
the Python app.identity.resolve_uniq_id(). No DB, no network, stdlib only.

Usage:
    python tests/replay_dedup_vectors.py [path/to/dedup-vectors.json]

Default fixture path is the canonical copy in the cash-back plugin repo
(single source of truth). Exit code 0 = full parity, 1 = any mismatch.
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.identity import resolve_uniq_id  # noqa: E402

_DEFAULT_FIXTURE = os.path.join(
    "F:\\",
    "wamp64", "www", "kash-back", "wp-content", "plugins", "cash-back",
    "development", "test", "fixtures", "dedup-vectors.json",
)


def main() -> int:
    fixture = sys.argv[1] if len(sys.argv) > 1 else _DEFAULT_FIXTURE
    with open(fixture, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    failures = 0
    total = 0
    for v in data["vectors"]:
        total += 1
        got_id, got_reason = resolve_uniq_id(
            str(v["slug"]),
            str(v["native_uniq_id"]),
            dict(v["fields"]),
            v["dedup_identity"],
        )
        exp_id = v["expected_id"]
        exp_reason = v["expected_reason"]
        if got_id != exp_id or got_reason != exp_reason:
            failures += 1
            print(
                f"FAIL [{v['name']}]: "
                f"got=({got_id!r},{got_reason!r}) "
                f"expected=({exp_id!r},{exp_reason!r})"
            )

    if failures:
        print(f"\n{failures}/{total} vectors FAILED — PHP/Python parity broken")
        return 1
    print(f"OK — {total}/{total} vectors match the frozen contract")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
