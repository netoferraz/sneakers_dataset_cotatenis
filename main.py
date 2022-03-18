import pandas as pd
from pathlib import Path
import asyncio
from aiohttp import ClientSession
import aiofiles

async def fetch(sku, url, session):
    async with session.get(url) as response:
        filename = url.split("?")[0].split("/")[-1]
        sneaker_folder = sku
        if sneaker_folder not in filename:
            filename = f"{sku}_{filename}"
        output_dir = Path().resolve() / f'dataset/{sneaker_folder}'
        output_dir.mkdir(parents=True, exist_ok=True)
        if response.status == 200:
            f = await aiofiles.open(output_dir / filename, mode='wb')
            await f.write(await response.read())
            await f.close()

async def main(container: dict):
    tasks = []
    # semaphore
    async with ClientSession() as session:
        for sku, container_urls in container.items():
            for url in container_urls:
                tasks.append(
                    asyncio.create_task(
                        fetch(sku, url, session)
                    )
                )
        pages_content = await asyncio.gather(*tasks)
        return pages_content
df = pd.read_json("./data/2022_03_17_data_to_mvp.json", lines=True)
container = {}
for data in df.itertuples():
    sku = data.sku
    container_urls = data.imgs
    container[sku] = container_urls

results = asyncio.run(main(container))
