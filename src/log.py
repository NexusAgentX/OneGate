from __future__ import annotations

import json
import os
from datetime import datetime, timezone

LOG_DIR = "logs"


def _ensure_log_dir():
    os.makedirs(LOG_DIR, exist_ok=True)


def _try_parse_json_body(headers, body: bytes | None) -> bytes | dict | list | None:
    if not body:
        return None
    content_type = headers.get("Content-Type", "")
    if "application/json" in content_type:
        try:
            return json.loads(body)
        except (json.JSONDecodeError, ValueError):
            pass
    return body.decode("utf-8", errors="replace")


def save_log(
    request_method: str,
    request_path: str,
    req_headers,
    req_body: bytes | None,
    resp_status: int,
    resp_headers: dict,
    resp_body: bytes | None,
    forward_headers: dict,
    target_url: str,
):
    _ensure_log_dir()
    now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    log_entry = {
        "timestamp": now,
        "request": {
            "method": request_method,
            "path": request_path,
            "headers": dict(req_headers),
            "body": _try_parse_json_body(req_headers, req_body),
        },
        "forward": {
            "url": target_url,
            "headers": dict(forward_headers),
            "body": _try_parse_json_body(forward_headers, req_body),
        },
        "response": {
            "status": resp_status,
            "headers": dict(resp_headers),
            "body": _try_parse_json_body(resp_headers, resp_body),
        },
    }
    log_file = os.path.join(LOG_DIR, f"{now}.json")
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(log_entry, f, ensure_ascii=False, indent=2)


def save_log_headers_only(
    request_method: str,
    request_path: str,
    req_headers,
    resp_status: int,
    resp_headers: dict,
    forward_headers: dict,
    provider_name: str,
):
    _ensure_log_dir()
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond:06d}Z"
    log_entry = {
        "ts": ts_str,
        "method": request_method,
        "path": request_path,
        "provider": provider_name,
        "status": resp_status,
        "req_headers": dict(req_headers),
        "forward_headers": dict(forward_headers),
        "resp_headers": dict(resp_headers),
    }
    log_file = os.path.join(LOG_DIR, f"{date_str}.jsonl")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
