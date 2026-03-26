from __future__ import annotations

import ssl
from datetime import datetime, timedelta, timezone

import aiohttp
from aiohttp import web

from src.config import AppConfig, match_provider
from src.db import record_usage
from src.log import save_log
from src.models import TokenEntry
from src.pool import resolve_token


def _parse_iso(s: str) -> datetime:
    s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)


def create_handlers(
    cfg: AppConfig,
    token_pool: dict[str, TokenEntry],
    db_conn,
    public_ip: str | None,
):
    async def info_handler(request: web.Request) -> web.Response:
        token = request.query.get("token")
        if not token:
            return web.json_response({"error": "missing token parameter"}, status=400)
        entry = token_pool.get(token)
        if not entry:
            return web.json_response({"error": "token not found in pool"}, status=404)

        now = datetime.now(timezone.utc)
        current_hour_ts = int(now.timestamp()) - (int(now.timestamp()) % 3600)

        start_str = request.query.get("start")
        end_str = request.query.get("end")

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
            "SELECT provider, ts, count FROM request_log "
            "WHERE token = ? AND ts >= ? AND ts <= ? "
            "ORDER BY provider, ts",
            (token, start_hour, end_hour),
        )
        rows = cur.fetchall()

        by_provider: dict[str, dict[int, int]] = {}
        for provider_name, ts, count in rows:
            by_provider.setdefault(provider_name, {})[ts] = count

        hourly: dict[str, dict[str, int]] = {}
        t = start_hour
        while t <= end_hour:
            dt = datetime.fromtimestamp(t, tz=timezone.utc)
            key = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            hourly[key] = {}
            for prov_name in by_provider:
                hourly[key][prov_name] = by_provider[prov_name].get(t, 0)
            t += 3600

        total = sum(count for _, _, count in rows)

        summary: dict[str, int] = {}
        for label, hours in [("last_24h", 24), ("last_7d", 168), ("last_30d", 720)]:
            cutoff = int(now.timestamp()) - hours * 3600
            cutoff_hour = cutoff - (cutoff % 3600)
            row = db_conn.execute(
                "SELECT COALESCE(SUM(count), 0) FROM request_log "
                "WHERE token = ? AND ts >= ?",
                (token, cutoff_hour),
            ).fetchone()
            summary[label] = row[0]

        current_hour_rows = db_conn.execute(
            "SELECT provider, COALESCE(count, 0) FROM request_log "
            "WHERE token = ? AND ts = ?",
            (token, current_hour_ts),
        ).fetchall()
        current_hour = {
            "hour": datetime.fromtimestamp(current_hour_ts, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "by_provider": {r[0]: r[1] for r in current_hour_rows},
        }

        return web.json_response(
            {
                "token": token,
                "providers": entry.providers,
                "current_hour": current_hour,
                "range": {
                    "start": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "end": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
                "total": total,
                "summary": summary,
                "hourly": hourly,
            }
        )

    async def proxy_handler(request: web.Request) -> web.StreamResponse:
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

        if forbidden:
            return web.json_response(
                {"error": f"token not authorized for provider '{provider.name}'"},
                status=403,
            )

        body = await request.read() if request.body_exists else None

        connector = aiohttp.TCPConnector(ssl=ssl.create_default_context())
        async with aiohttp.ClientSession(
            connector=connector,
            skip_auto_headers={"User-Agent"},
        ) as session:
            async with session.request(
                method=request.method,
                url=target_url,
                headers=headers,
                data=body,
                allow_redirects=False,
            ) as resp:
                resp_headers = dict(resp.headers)
                resp_headers.pop("Transfer-Encoding", None)

                resp_chunks = []
                response = web.StreamResponse(
                    status=resp.status,
                    headers=resp_headers,
                )
                await response.prepare(request)

                async for chunk in resp.content.iter_any():
                    resp_chunks.append(chunk)
                    await response.write(chunk)

                await response.write_eof()

                resp_body = b"".join(resp_chunks) if resp_chunks else None
                if cfg.enable_log:
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
                    )
                if proxy_token:
                    record_usage(db_conn, proxy_token, provider.name)
                print(
                    f"[LOG] {request.method} {request.path} "
                    f"-> [{provider.name}] {resp.status}"
                )

                return response

    return info_handler, proxy_handler
