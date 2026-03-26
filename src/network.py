from __future__ import annotations

import ssl

import aiohttp


async def fetch_public_ip() -> str:
    connector = aiohttp.TCPConnector(ssl=ssl.create_default_context())
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get("https://ifconfig.me/ip") as resp:
            return (await resp.text()).strip()
