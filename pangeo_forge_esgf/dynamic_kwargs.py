from typing import Dict, List, Tuple

import aiohttp

from .utils import facets_from_iid

async def response_data_processing(
    session: aiohttp.ClientSession,
    response_data: List[Dict[str, str]],
    iid: str,
) -> Tuple[List[str], Dict[str, Dict[str, str]]]:
    table_id = facets_from_iid(iid).get("table_id")
    urls = [r["url"] for r in response_data]

    print(f"Found {len(urls)} urls")
    print(list(urls))
    return urls


async def is_netcdf3(session: aiohttp.ClientSession, url: str) -> bool:
    """Simple check to determine the netcdf file version behind a url.
    Requires the server to support range requests"""
    headers = {"Range": "bytes=0-2"}
    # TODO: how should i handle it if these are failing?
    # TODO: need to implement a retry here too
    # TODO: I believe these are independent of the search nodes? So we should not retry these with another node? I might need to look into what 'replicas' mean in this context.
    async with session.get(url, headers=headers) as resp:
        status_code = resp.status
        if not status_code == 206:
            raise RuntimeError(f"Range request failed with {status_code} for {url}")
        head = await resp.read()
        return "CDF" in str(head)
