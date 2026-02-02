import asyncio
import aiohttp
import logging
from datetime import date
today = date.today().isoformat()
print(today,"date")

BASE_URL = "https://api.veracross.com"
MAX_CONCURRENT = 15        # SAFE for Veracross
RETRIES = 3
TIMEOUT = aiohttp.ClientTimeout(total=60)

logging.basicConfig(level=logging.INFO)


async def fetch_attendance(session, class_id):
    url = f"{BASE_URL}/ACSAD/v3/classes/{class_id}/attendance"
    params = {
        "attendance_date" : today
    }

    for attempt in range(RETRIES):
        try:
            async with session.get(url,params = params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # print(data["data"])
                    return data.get("data", [])

                if resp.status in (429, 500, 502, 503):
                    wait = 2 ** attempt
                    logging.warning(
                        f"Retry {attempt + 1} for class {class_id}, status {resp.status}"
                    )
                    await asyncio.sleep(wait)
                else:
                    return {
                        "class_id": class_id,
                        "error": f"HTTP {resp.status}"
                    }

        except asyncio.TimeoutError:
            await asyncio.sleep(2 ** attempt)

    return {
        "class_id": class_id,
        "error": "Failed after retries"
    }


async def fetch_class_attendance(class_ids, access_token):
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)

    headers = {
        "Authorization": f"Bearer {access_token}"
    }


    async with aiohttp.ClientSession(
        headers=headers,
        connector=connector,
        timeout=TIMEOUT
    ) as session:

        tasks = [
            fetch_attendance(session, class_id)
            for class_id in class_ids
        ]

        results = await asyncio.gather(*tasks)
        # print(results)

        flat_results = []
        for sublist in results:
            flat_results.extend(sublist)
        print(flat_results)
        return flat_results
