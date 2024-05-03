import polars as pl
from datetime import datetime, timedelta
from hypersync_lancedb_pipe.db_manager import Manager

# start and end date range
start_date = datetime(2024, 4, 20)
end_date = datetime(2024, 4, 25)

# base url endpiont. Replace this with other tables to explore different datasets
base_url = "https://mempool-dumpster.flashbots.net/ethereum/mainnet/"


# list comprehension to generate endpoints
parquet_urls = []
parquet_urls = [
    f"{base_url}{
        date.year}-{date.month:02d}/{date.year}-{date.month:02d}-{date.day:02d}.parquet"
    for date in (start_date + timedelta(days=n) for n in range((end_date - start_date).days + 1))
]

for url in parquet_urls:
    print(f'processing {url}')

    mempool_df = pl.scan_parquet(url).collect()

    # write to lancedb
    lance_manager = Manager()
    lance_manager.write_db("mempool", mempool_df,
                           merge_on="timestamp")
