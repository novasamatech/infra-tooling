# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Container smoke check with the API token sent only over stdin."""

import json
import os
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


def main() -> int:
    engine, image, port, timeout = sys.argv[1:]
    token = os.environ["CF_EXPORTER_API_TOKEN"].strip()
    command = shlex.split(engine)
    # Temporary files contain only the container ID and redacted process logs.
    with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent.parent) as directory:
        cidfile = Path(directory) / "container.id"
        with tempfile.TemporaryFile(mode="w+") as log:
            process = subprocess.Popen(
                [
                    *command,
                    "run",
                    "--rm",
                    "-i",
                    "--cidfile",
                    str(cidfile),
                    "-p",
                    f"127.0.0.1:{port}:8000",
                    image,
                    "/bin/sh",
                    "-c",
                    "IFS= read -r CF_EXPORTER_API_TOKEN; "
                    "export CF_EXPORTER_API_TOKEN; exec python script.py",
                ],
                stdin=subprocess.PIPE,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            try:
                process.stdin.write(token + "\n")
                process.stdin.close()
                metadata_checked = False
                deadline = time.monotonic() + float(timeout)
                while process.poll() is None and time.monotonic() < deadline:
                    if not metadata_checked and cidfile.exists():
                        metadata = subprocess.run(
                            [*command, "inspect", cidfile.read_text().strip()],
                            check=True,
                            capture_output=True,
                            text=True,
                            timeout=10,
                        )
                        leaked = token in metadata.stdout
                        if leaked:
                            print("image smoke test: FAILED - token found in container metadata")
                            return 1
                        metadata_checked = (
                            json.loads(metadata.stdout)[0]["State"]["Running"] is True
                        )
                    try:
                        with urllib.request.urlopen(
                            f"http://127.0.0.1:{port}/metrics", timeout=5
                        ) as response:
                            body = response.read().decode()
                        if (
                            metadata_checked
                            and "cf_exporter_last_cycle_success 1.0" in body.splitlines()
                        ):
                            print(f"image smoke test: OK ({image})")
                            return 0
                    except (urllib.error.URLError, ConnectionError):
                        pass
                    time.sleep(1)
                log.seek(0)
                print("image smoke test: FAILED - container logs:")
                print(log.read().replace(token, "[REDACTED]"))
                return 1
            finally:
                if process.poll() is None and cidfile.exists():
                    subprocess.run(
                        [*command, "stop", "-t", "10", cidfile.read_text().strip()],
                        check=False,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        timeout=30,
                    )
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    sys.exit(main())
