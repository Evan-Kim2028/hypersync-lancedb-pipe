# hypersync-lancedb-pipe

A serverless embedded streaming OLAP data pipeline that leverages historical blockchain data from [Hypersync](https://github.com/enviodev/hypersync-client-python) and mutable columnar storage format [lance](https://lancedb.github.io/lance/).

Since Lance is designed to be mutable, it is possible to create an embedded streaming pipeline using the same data source. The main advantage of this streaming approach is that it doesn't require any parquet glob file management. This reduces the complexity of streaming to the same as batch processing. The other main benefit is that LanceDB has tight integration with both
[polars](https://lancedb.github.io/lancedb/python/polars_arrow/#from-polars-dataframe) and [duckdb](https://lancedb.github.io/lancedb/python/duckdb/). LanceDB accepts polars dataframes as
data inputs, which allows for a more flexible ETL pipeline, allowing polars to be used as a preprocessing tool. 

Since LanceDB leverages the [Apache Arrow Standard](https://arrow.apache.org/overview/), there is a lot of flexibility to query from ths database - such as querying larger than memory
datasets with polars lazyframes and a dataframe API, or using an embedded OLAP engine like duckdb for faster speed and SQL API.

### Getting Started
1. This repository uses rye to manage dependencies and the virtual environment. To install, refer to this link for instructions [here](https://rye-up.com/guide/installation/). 
2. Once rye is installed, run `rye sync` to install dependencies and setup the virtual environment, which has a default name of `.venv`. 
3. Activate the virtual environment with the command `source .venv/bin/activate`.

### Running the Pipeline
There are some script examples in the `scripts` folder. These examples demonstrate the versatility of the lancedb writer.

* Run `historical_sync.py` file to backfill data from a historical block number. Assumes there is no existing table.
* Run `head_sync.py` to sync the database to the head of the chain. Assumes existing table exists.
* Run `backfill_sync.py` to perform a backfill sync from the earliest block number. Assumes existing table exists.