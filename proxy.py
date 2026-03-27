import asyncio

from aiohttp import web
from dotenv import load_dotenv

from src.admin import create_admin_handlers
from src.config import load_config
from src.db import init_db
from src.handlers import create_handlers
from src.network import fetch_public_ip
from src.pool import load_pool_from_db

load_dotenv()


async def main():
    cfg = load_config()

    print("Fetching public IP...")
    public_ip = await fetch_public_ip()
    print(f"Current public IP: {public_ip}")

    db_conn, generated_admins = init_db(
        cfg.usage_db, cfg.pool_file, cfg.admin_tokens, cfg.pool_count
    )
    token_pool = load_pool_from_db(db_conn)

    if generated_admins:
        for t in generated_admins:
            print(f"[ADMIN] Auto-generated admin token: {t}")
            print(f"[ADMIN] Use this to access: http://0.0.0.0:{cfg.bind_port}/admin")

    provider_names = [p.name for p in cfg.providers]
    print(f"Providers configured: {provider_names}")

    serve_usage_page, info_api_handler, proxy_handler = create_handlers(
        cfg, token_pool, db_conn, public_ip
    )

    (
        serve_admin_page,
        api_list_tokens,
        api_create_token,
        api_update_token,
        api_delete_token,
    ) = create_admin_handlers(cfg, db_conn, token_pool)

    app = web.Application()
    app.router.add_get("/usage", serve_usage_page)
    app.router.add_post("/usage/api", info_api_handler)
    app.router.add_get("/admin", serve_admin_page)
    app.router.add_get("/admin/api/tokens", api_list_tokens)
    app.router.add_post("/admin/api/tokens", api_create_token)
    app.router.add_put("/admin/api/tokens", api_update_token)
    app.router.add_delete("/admin/api/tokens", api_delete_token)
    app.router.add_route("*", "/{path:.*}", proxy_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", cfg.bind_port)
    await site.start()

    provider_list = ", ".join(f"{p.prefix} -> {p.upstream}" for p in cfg.providers)
    print(f"Reverse proxy running on http://0.0.0.0:{cfg.bind_port}")
    print(f"Providers: {provider_list}")
    print(f"Token pool: {len(token_pool)} tokens")
    print(f"Usage query: http://0.0.0.0:{cfg.bind_port}/usage")
    print(f"Admin panel: http://0.0.0.0:{cfg.bind_port}/admin")
    print(f"Intercept: {cfg.intercept_port or 'disabled'}")
    print(f"Logging: {'enabled' if cfg.enable_log else 'disabled'}")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
