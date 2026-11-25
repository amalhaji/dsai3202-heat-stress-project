"""Shared helper for the deployment checks: calls the endpoint and times it (client-observed)."""
import json
import os
import time
import urllib.error
import urllib.request

URL = os.environ.get("EP_URL", "")
KEY = os.environ.get("EP_KEY", "")
EVIDENCE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "evidence")


def call(body: bytes, timeout=30):
    if not URL or not KEY:
        raise SystemExit("set EP_URL and EP_KEY first (see README, deployment step)")
    req = urllib.request.Request(URL, data=body, method="POST",
                                 headers={"Content-Type": "application/json", "Authorization": f"Bearer {KEY}"})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            status, text = r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        status, text = e.code, e.read().decode()
    ms = (time.perf_counter() - t0) * 1000
    try:
        out = json.loads(text)
        if isinstance(out, str):          # some servers double-encode
            out = json.loads(out)
    except ValueError:
        out = text
    return status, out, ms


def save(name, obj):
    os.makedirs(EVIDENCE, exist_ok=True)
    path = os.path.join(EVIDENCE, name)
    json.dump(obj, open(path, "w"), indent=2)
    print("saved", os.path.normpath(path))
