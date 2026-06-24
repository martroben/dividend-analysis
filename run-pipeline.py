"""
Runs all pipeline scripts in order. Stops on first failure.
"""

# standard
import subprocess
import sys
import time
from pathlib import Path


SCRIPTS = sorted(Path("src").glob("[0-9][0-9]-*.py"))

pad = max(len(s.name) for s in SCRIPTS)

for script in SCRIPTS:
    print(f"  {script.name:<{pad}}  ", end="", flush=True)
    start = time.monotonic()
    result = subprocess.run([sys.executable, str(script)], cwd=Path(__file__).parent)
    elapsed = time.monotonic() - start
    if result.returncode != 0:
        print(f"FAILED  ({elapsed:.1f}s)")
        print(f"\nPipeline stopped at {script.name}.")
        sys.exit(1)
    print(f"OK  ({elapsed:.1f}s)")

print(f"\nAll {len(SCRIPTS)} steps completed.")
