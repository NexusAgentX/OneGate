from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import aiohttp
from aiohttp import web

from src.config import AppConfig, match_provider
from src.db import (
    check_rate_limit,
    get_provider_status,
    record_status,
    record_success,
    record_usage,
)
from src.log import save_log, save_log_headers_only
from src.models import TokenEntry
from src.pool import resolve_token

STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")
logger = logging.getLogger(__name__)


def _parse_iso(s: str) -> datetime:
    s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)


async def _query_usage(token, token_pool, db_conn, start_str=None, end_str=None):
    entry = token_pool.get(token)
    if not entry:
        return None, "token not found in pool"

    now = datetime.now(timezone.utc)
    current_hour_ts = int(now.timestamp()) - (int(now.timestamp()) % 3600)

    if start_str:
        start_dt = _parse_iso(start_str)
    else:
        start_dt = now - timedelta(hours=24)

    if end_str:
        end_dt = _parse_iso(end_str)
    else:
        end_dt = now

    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())

    start_hour = start_ts - (start_ts % 3600)
    end_hour = end_ts - (end_ts % 3600)

    cur = db_conn.execute(
        "SELECT provider, ts - (ts % 3600) AS hour_ts, SUM(count) AS cnt "
        "FROM success_log WHERE token = ? AND ts >= ? AND ts <= ? "
        "GROUP BY provider, hour_ts ORDER BY provider, hour_ts",
        (token, start_hour, end_hour),
    )
    rows = cur.fetchall()

    by_provider: dict[str, dict[int, int]] = {}
    for provider_name, hour_ts, cnt in rows:
        by_provider.setdefault(provider_name, {})[hour_ts] = cnt

    hourly: dict[str, dict[str, int]] = {}
    t = start_hour
    while t <= end_hour:
        dt = datetime.fromtimestamp(t, tz=timezone.utc)
        key = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        hourly[key] = {}
        for prov_name in by_provider:
            hourly[key][prov_name] = by_provider[prov_name].get(t, 0)
        t += 3600

    total = sum(cnt for _, _, cnt in rows)

    summary: dict[str, int] = {}
    for label, hours in [("last_24h", 24), ("last_7d", 168), ("last_30d", 720)]:
        cutoff = int(now.timestamp()) - hours * 3600
        cutoff_hour = cutoff - (cutoff % 3600)
        row = db_conn.execute(
            "SELECT COALESCE(SUM(count), 0) FROM success_log "
            "WHERE token = ? AND ts >= ?",
            (token, cutoff_hour),
        ).fetchone()
        summary[label] = row[0]

    current_hour_rows = db_conn.execute(
        "SELECT provider, SUM(count) FROM success_log "
        "WHERE token = ? AND ts >= ? AND ts < ? "
        "GROUP BY provider",
        (token, current_hour_ts, current_hour_ts + 3600),
    ).fetchall()
    current_hour = {
        "hour": datetime.fromtimestamp(current_hour_ts, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "by_provider": {r[0]: r[1] for r in current_hour_rows},
    }

    return {
        "token": token,
        "providers": entry.providers,
        "describe": getattr(entry, "describe", ""),
        "current_hour": current_hour,
        "range": {
            "start": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        "total": total,
        "summary": summary,
        "hourly": hourly,
    }, None


def create_handlers(
    cfg: AppConfig,
    token_pool: dict[str, TokenEntry],
    db_conn,
    public_ip: str | None,
    session: aiohttp.ClientSession,
    start_time: float,
):
    async def serve_usage_page(request: web.Request) -> web.Response:
        html_path = os.path.join(STATIC_DIR, "usage.html")
        if not os.path.exists(html_path):
            return web.json_response({"error": "usage.html not found"}, status=500)
        with open(html_path, "r", encoding="utf-8") as f:
            return web.Response(text=f.read(), content_type="text/html")

    async def info_api_handler(request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "invalid JSON"}, status=400)

        token = body.get("token")
        if not token:
            return web.json_response({"error": "missing token"}, status=400)

        data, err = await _query_usage(
            token,
            token_pool,
            db_conn,
            body.get("start"),
            body.get("end"),
        )
        if err:
            return web.json_response({"error": err}, status=404)
        return web.json_response(data)

    async def proxy_handler(request: web.Request) -> web.StreamResponse:
        client_ip = request.headers.get("X-Forwarded-For", "")
        if not client_ip:
            client_ip = request.headers.get("X-Real-IP", "")
        if not client_ip:
            client_ip = request.remote or ""
        if client_ip in cfg.banned_ips:
            return web.json_response({"error": "forbidden"}, status=403)

        provider = match_provider(cfg.providers, request.path)
        if not provider:
            return web.json_response({"error": "no provider matched"}, status=502)

        path = request.path
        if provider.strip_prefix:
            prefix = provider.prefix.rstrip("/")
            if path.startswith(prefix):
                path = path[len(prefix) :] or "/"

        if cfg.intercept_port:
            target_url = f"http://127.0.0.1:{cfg.intercept_port}" + path
        else:
            target_url = provider.origin + path
        if request.query_string:
            target_url += "?" + request.query_string

        forced_headers = {
            "Host": provider.host,
            "X-Forwarded-For": public_ip,
            "X-Real-IP": public_ip,
        }

        headers = dict(request.headers)
        for key, value in forced_headers.items():
            if key in headers:
                headers[key] = value

        proxy_token = None
        forbidden = False
        if "Authorization" in headers:
            proxy_token, resolved_auth = resolve_token(
                headers["Authorization"], provider, token_pool, cfg.real_tokens
            )
            if proxy_token is not None and resolved_auth == "":
                forbidden = True
            else:
                headers["Authorization"] = resolved_auth

        if "x-api-key" in headers:
            real_token = cfg.real_tokens.get(provider.name, "")
            if real_token:
                headers["x-api-key"] = real_token

        if forbidden:
            return web.json_response(
                {"error": f"token not authorized for provider '{provider.name}'"},
                status=403,
            )

        if proxy_token:
            token_entry = token_pool.get(proxy_token)
            if token_entry and (
                token_entry.rpm != -1
                or token_entry.rph != -1
                or token_entry.rpd != -1
                or token_entry.rpt != -1
            ):
                allowed, triggered, quota_info = check_rate_limit(
                    db_conn,
                    proxy_token,
                    token_entry.rpm,
                    token_entry.rph,
                    token_entry.rpd,
                    token_entry.rpt,
                )
                if not allowed:
                    return web.json_response(
                        {
                            "error": "rate limit exceeded",
                            "triggered": triggered,
                            "quota": quota_info,
                        },
                        status=429,
                    )

        body = await request.read() if request.body_exists else None

        try:
            async with session.request(
                method=request.method,
                url=target_url,
                headers=headers,
                data=body,
                allow_redirects=False,
            ) as resp:
                resp_headers = dict(resp.headers)
                resp_headers.pop("Transfer-Encoding", None)
                resp_headers.pop("Content-Length", None)

                response = web.StreamResponse(
                    status=resp.status,
                    headers=resp_headers,
                )
                await response.prepare(request)

                resp_chunks = []
                async for chunk in resp.content.iter_any():
                    if cfg.enable_log and cfg.log_format == "full":
                        resp_chunks.append(chunk)
                    await response.write(chunk)

                await response.write_eof()

                resp_body = b"".join(resp_chunks) if resp_chunks else None
                if cfg.enable_log:
                    if cfg.log_format == "headers":
                        save_log_headers_only(
                            request.method,
                            request.path,
                            request.headers,
                            resp.status,
                            resp_headers,
                            headers,
                            provider.name,
                        )
                    else:
                        save_log(
                            request.method,
                            request.path,
                            request.headers,
                            body,
                            resp.status,
                            resp_headers,
                            resp_body,
                            headers,
                            target_url,
                            decompress=cfg.decompress_log,
                        )
                if proxy_token:
                    record_usage(db_conn, proxy_token, provider.name)
                    if resp.status == 200:
                        record_success(db_conn, proxy_token, provider.name)
                record_status(db_conn, provider.name, resp.status)
                logger.info(
                    "%s %s -> [%s] %s",
                    request.method,
                    request.path,
                    provider.name,
                    resp.status,
                )

                return response

        except asyncio.TimeoutError:
            record_status(db_conn, provider.name, 504)
            logger.error(
                "Timeout proxying %s %s -> [%s] %s",
                request.method,
                request.path,
                provider.name,
                target_url,
            )
            return web.json_response(
                {"error": "upstream request timed out"},
                status=504,
            )
        except aiohttp.ClientError as e:
            record_status(db_conn, provider.name, 502)
            logger.error(
                "Client error proxying %s %s -> [%s] %s: %s",
                request.method,
                request.path,
                provider.name,
                target_url,
                e,
            )
            return web.json_response(
                {"error": f"upstream connection error: {e}"},
                status=502,
            )

    async def serve_status_page(request: web.Request) -> web.Response:
        html_path = os.path.join(STATIC_DIR, "status.html")
        if not os.path.exists(html_path):
            return web.json_response({"error": "status.html not found"}, status=500)
        with open(html_path, "r", encoding="utf-8") as f:
            return web.Response(text=f.read(), content_type="text/html")

    async def status_api_handler(request: web.Request) -> web.Response:
        uptime = int(time.time() - start_time)
        stats = get_provider_status(db_conn)

        grouped: dict[str, dict] = {}
        for p in cfg.providers:
            real_token = cfg.real_tokens.get(p.name, "")
            key = f"{p.upstream}::{real_token}"
            if key not in grouped:
                grouped[key] = {
                    "name": p.name,
                    "prefixes": [p.prefix],
                    "upstream": p.upstream,
                    "original_names": [p.name],
                }
            else:
                grouped[key]["prefixes"].append(p.prefix)
                grouped[key]["original_names"].append(p.name)

        providers = [
            {
                "name": v["name"],
                "prefix": ", ".join(v["prefixes"]),
                "upstream": v["upstream"],
            }
            for v in grouped.values()
        ]

        merged_hourly: dict[str, list] = {}
        for pname, hourly_data in stats.get("hourly", {}).items():
            for p in cfg.providers:
                real_token = cfg.real_tokens.get(p.name, "")
                key = f"{p.upstream}::{real_token}"
                if p.name == pname and key in grouped:
                    group_name = grouped[key]["name"]
                    if group_name not in merged_hourly:
                        merged_hourly[group_name] = hourly_data
                    else:
                        for i, h in enumerate(hourly_data):
                            if i < len(merged_hourly[group_name]):
                                merged_hourly[group_name][i]["total"] += h["total"]
                                merged_hourly[group_name][i]["success"] += h["success"]
                                if merged_hourly[group_name][i]["total"] > 0:
                                    merged_hourly[group_name][i]["success_rate"] = (
                                        round(
                                            merged_hourly[group_name][i]["success"]
                                            / merged_hourly[group_name][i]["total"]
                                            * 100,
                                            2,
                                        )
                                    )
                                for sc, cnt in h.get("error_distribution", {}).items():
                                    if (
                                        sc
                                        not in merged_hourly[group_name][i][
                                            "error_distribution"
                                        ]
                                    ):
                                        merged_hourly[group_name][i][
                                            "error_distribution"
                                        ][sc] = 0
                                    merged_hourly[group_name][i]["error_distribution"][
                                        sc
                                    ] += cnt
                    break

        merged_summary: dict[str, dict] = {}
        for pname, period_data in stats.items():
            if pname == "hourly":
                continue
            for p in cfg.providers:
                real_token = cfg.real_tokens.get(p.name, "")
                key = f"{p.upstream}::{real_token}"
                if p.name == pname and key in grouped:
                    group_name = grouped[key]["name"]
                    if group_name not in merged_summary:
                        merged_summary[group_name] = period_data
                    else:
                        for period, data in period_data.items():
                            if period in merged_summary[group_name]:
                                merged_summary[group_name][period]["total"] += data[
                                    "total"
                                ]
                                merged_summary[group_name][period]["success"] += data[
                                    "success"
                                ]
                                if merged_summary[group_name][period]["total"] > 0:
                                    merged_summary[group_name][period][
                                        "success_rate"
                                    ] = round(
                                        merged_summary[group_name][period]["success"]
                                        / merged_summary[group_name][period]["total"]
                                        * 100,
                                        2,
                                    )
                                for sc, cnt in data.get(
                                    "error_distribution", {}
                                ).items():
                                    if (
                                        sc
                                        not in merged_summary[group_name][period][
                                            "error_distribution"
                                        ]
                                    ):
                                        merged_summary[group_name][period][
                                            "error_distribution"
                                        ][sc] = 0
                                    merged_summary[group_name][period][
                                        "error_distribution"
                                    ][sc] += cnt
                    break

        return web.json_response(
            {
                "uptime": uptime,
                "providers": providers,
                "stats": merged_hourly,
                "summary": merged_summary,
            }
        )

    return (
        serve_usage_page,
        info_api_handler,
        proxy_handler,
        serve_status_page,
        status_api_handler,
    )
