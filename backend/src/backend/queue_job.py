from __future__ import annotations

import json

from backend.services import select_review_candidates


def run_queue_selector(limit: int = 50, per_device_cap: int = 10) -> list[dict]:
    return select_review_candidates(limit=limit, per_device_cap=per_device_cap)


def main() -> None:
    candidates = run_queue_selector()
    print(json.dumps({"count": len(candidates), "candidates": candidates}, indent=2))


if __name__ == "__main__":
    main()
