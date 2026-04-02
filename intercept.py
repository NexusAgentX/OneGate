import asyncio
import json
import logging
import os
import ssl
from datetime import datetime, timezone

import aiohttp
from aiohttp import web

TARGET_HOST = "open.bigmodel.cn"
TARGET_ORIGIN = f"https://{TARGET_HOST}"
BIND_PORT = 5679
LOG_DIR = "intercept_logs"

logger = logging.getLogger(__name__)


def _ensure_log_dir():
    os.makedirs(LOG_DIR, exist_ok=True)


def _save_log(
    request_method,
    request_path,
    req_headers,
    req_body,
    resp_status,
    resp_headers,
    resp_body,
):
    _ensure_log_dir()
    now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    log_entry = {
        "timestamp": now,
        "upstream_request": {
            "method": request_method,
            "url": f"https://{TARGET_HOST}{request_path}",
            "headers": dict(req_headers),
            "body": req_body.decode("utf-8", errors="replace") if req_body else None,
        },
        "upstream_response": {
            "status": resp_status,
            "headers": dict(resp_headers),
            "body": resp_body.decode("utf-8", errors="replace") if resp_body else None,
        },
    }
    log_file = os.path.join(LOG_DIR, f"{now}.json")
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(log_entry, f, ensure_ascii=False, indent=2)


async def intercept_handler(
    request: web.Request, session: aiohttp.ClientSession
) -> web.StreamResponse:
    target_url = TARGET_ORIGIN + request.path
    if request.query_string:
        target_url += "?" + request.query_string

    headers = dict(request.headers)
    headers["Host"] = TARGET_HOST

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
            _save_log(
                request.method,
                request.path,
                request.headers,
                body,
                resp.status,
                resp_headers,
                resp_body,
            )
            logger.info(
                "[INTERCEPT] %s %s -> %s saved",
                request.method,
                request.path,
                resp.status,
            )

            return response

    except asyncio.TimeoutError:
        logger.error(
            "Timeout intercepting %s %s -> %s", request.method, request.path, target_url
        )
        return web.json_response({"error": "upstream request timed out"}, status=504)
    except aiohttp.ClientError as e:
        logger.error(
            "Client error intercepting %s %s -> %s: %s",
            request.method,
            request.path,
            target_url,
            e,
        )
        return web.json_response(
            {"error": f"upstream connection error: {e}"}, status=502
        )


async def main():
    timeout = aiohttp.ClientTimeout(total=1800, sock_connect=30, sock_read=900)
    connector = aiohttp.TCPConnector(ssl=ssl.create_default_context())
    session = aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        skip_auto_headers={"User-Agent"},
        auto_decompress=False,
    )

    async def on_cleanup(app: web.Application):
        await session.close()

    app = web.Application(handler_args={"keepalive_timeout": 75})
    app.on_cleanup.append(on_cleanup)

    def make_handler(s: aiohttp.ClientSession):
        async def handler(request: web.Request) -> web.StreamResponse:
            return await intercept_handler(request, s)

        return handler

    app.router.add_route("*", "/{path:.*}", make_handler(session))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", BIND_PORT)
    await site.start()
    print(f"Intercept proxy running on http://0.0.0.0:{BIND_PORT} -> {TARGET_ORIGIN}")
    print(f"Logs saved to {LOG_DIR}/")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
