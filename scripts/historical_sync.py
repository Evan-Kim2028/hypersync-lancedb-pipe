import asyncio
import hypersync
import polars as pl
import time

from hypersync import ColumnMapping, DataType, TransactionField, BlockField
from hypersync_lancedb_pipe.db_manager import Manager

# This script demonstrates how to batch query hypersync blocks and transactions data, store in a parquet sink, and then write to a LanceDB table.
# This approach leverages the mutable Lance columnar storage format with the writer capabilities of Lancedb to continuously stream a


async def historical_blocks_txs_sync():
    """
    Uses the HypersyncClient to collect blocks and transactions from the Ethereum network. This data is stored in a parquet sink.
    """

    client = hypersync.HypersyncClient("https://eth.hypersync.xyz")

    # set to_block and from_block to query the desired block range.
    to_block: int = await client.get_height()
    from_block: int = to_block - (7_200 * 30)
    db_batch_size: int = 10_000  # Define the number of blocks to process per batch

    while from_block < to_block:
        current_to_block = min(from_block + db_batch_size, to_block)
        print(
            f"Processing blocks {from_block} to {current_to_block}")

        query = client.preset_query_blocks_and_transactions(
            from_block, current_to_block)
        # Setting this number lower reduces client sync console error messages.
        query.max_num_transactions = 1_000  # for troubleshooting

        config = hypersync.ParquetConfig(
            path="data",
            hex_output=True,
            batch_size=5_000,
            concurrency=10,
            retry=True,
            column_mapping=ColumnMapping(
                transaction={
                    TransactionField.GAS_USED: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
                    TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.GAS_PRICE: DataType.FLOAT64,
                    TransactionField.CUMULATIVE_GAS_USED: DataType.FLOAT64,
                    TransactionField.EFFECTIVE_GAS_PRICE: DataType.FLOAT64,
                    TransactionField.NONCE: DataType.INT64,
                    TransactionField.GAS: DataType.FLOAT64,
                },
                block={
                    BlockField.GAS_LIMIT: DataType.FLOAT64,
                    BlockField.GAS_USED: DataType.FLOAT64,
                    BlockField.SIZE: DataType.FLOAT64,
                    BlockField.BLOB_GAS_USED: DataType.FLOAT64,
                    BlockField.EXCESS_BLOB_GAS: DataType.FLOAT64,
                    BlockField.BASE_FEE_PER_GAS: DataType.FLOAT64
                },
            )
        )

        await client.create_parquet_folder(query, config)
        from_block = current_to_block + 1  # Update from_block for the next batch

        # Write blocks and transactions data into LanceDB tables.
        blocks_table_name = "blocks"
        txs_table_name = "transactions"
        index: str = "block_number"

        # load the dataframe into a polars dataframe, which is one way to insert data into Lancedb.
        # This adds analytical transformations flexibility into the pipeline.
        blocks_df = pl.read_parquet(
            f"data/{blocks_table_name}.parquet").rename({'number': 'block_number'})
        txs_df = pl.read_parquet(f"data/{txs_table_name}.parquet")
        # write to lancedb
        lance_manager = Manager()
        lance_manager.write_db(f"{blocks_table_name}", blocks_df,
                               merge_on=index)
        lance_manager.write_db(f"{txs_table_name}",
                               txs_df, merge_on=index)


start_time = time.time()
asyncio.run(historical_blocks_txs_sync())
end_time = time.time()

print(f"Time taken: {end_time - start_time}")
