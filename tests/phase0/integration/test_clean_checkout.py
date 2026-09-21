"""Phase 0 integration suite — I1, clean-checkout boot.

There is nothing to integrate yet; this phase's integration test is just proving
the foundation boots. It clones HEAD into a throwaway directory (so nothing
untracked — .env files above all — can make it pass locally and fail on CI) and
asserts `docker compose up` brings the stack up with no manual fixes.

Runs under pytest, or standalone: `python tests/phase0/integration/test_clean_checkout.py`
"""

from __future__ import annotations

import json
import subprocess
import tempfile
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
UP_TIMEOUT_S = 900  # first run pulls kafka/clickhouse and builds the API image


def _run(cmd: list[str], cwd: Path, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, timeout=timeout, capture_output=True, text=True)


def _conflicting_containers() -> list[str]:
    """Fixed container_name plus a fixed network name mean only one copy of this
    stack can run at a time. That is fine on CI, where this test runs from a
    fresh clone against a clean daemon, but locally it collides with a stack the
    developer (or the Phase 1 suite) already has up — so say so plainly rather
    than failing on a raw Docker name conflict."""
    names = {"redis", "kafka", "clickhouse", "task-manager", "controller"}
    got = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        capture_output=True, text=True, timeout=60,
    )
    return sorted(names.intersection(got.stdout.split()))


def test_clean_checkout_boots() -> None:
    running = _conflicting_containers()
    assert not running, (
        f"another copy of this stack is already running ({', '.join(running)}). "
        "The compose file pins container and network names, so a second copy "
        "cannot start. Run `docker compose down` first."
    )

    with tempfile.TemporaryDirectory(prefix="phase0-clean-checkout-") as tmp:
        clone = Path(tmp) / "repo"
        got = _run(["git", "clone", "--quiet", "--depth", "1", f"file://{REPO}", str(clone)], REPO)
        assert got.returncode == 0, f"clone failed:\n{got.stderr}"

        # The container would otherwise create these as root inside the bind
        # mount, and this process could not clean the temp directory up after.
        for sub in ("processed", "failed"):
            (clone / "pipeline_definitions" / sub).mkdir(parents=True, exist_ok=True)

        try:
            up = _run(["docker", "compose", "up", "-d", "--build", "--wait"], clone, UP_TIMEOUT_S)
            assert up.returncode == 0, (
                "`docker compose up` needed manual fixes on a clean checkout:\n"
                f"{up.stdout}\n{up.stderr}"
            )

            # --wait only proves the healthchecks passed; ask the API itself.
            with urllib.request.urlopen("http://localhost:8000/health", timeout=30) as resp:
                health = json.load(resp)
            assert health["status"] == "ok", health

            ps = _run(["docker", "compose", "ps", "--format", "json"], clone)
            services = [json.loads(line) for line in ps.stdout.splitlines() if line.strip()]
            running = {s["Service"] for s in services if s["State"] == "running"}
            assert {"redis", "kafka", "clickhouse", "task-manager"} <= running, running
        finally:
            _run(["docker", "compose", "down", "-v", "--remove-orphans"], clone, 300)


if __name__ == "__main__":
    test_clean_checkout_boots()
    print("PASS: clean checkout boots")
