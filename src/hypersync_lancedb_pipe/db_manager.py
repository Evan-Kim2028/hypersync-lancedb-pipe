import datetime
import lancedb
import polars as pl

from dataclasses import dataclass
from lancedb import DBConnection


@dataclass
class Manager:
    """
    Manager class that contains main functionality to create and write data LanceDB tables. 
    """
    # CLEANUP_TIME determines the the frequency that lancedb will clean up the database and compact the files.
    # Increase the number for less frequent cleanups and longer batch update times.
    CLEANUP_TIME = 86400 / 24 / 60

    def write_db(self, table: lancedb.table, data: pl.DataFrame, merge_on: str = "block_number"):
        """
        Write a dataframe to a LanceDB table. If a dataframe doesn't exist, then create it with the given data. The default
        merge_on value is "block_number" because that is one of the most universal columns for blockchain data.
        """
        try:
            db: DBConnection = lancedb.connect(table)
            # Try to open and merge data into existing table.
            table: lancedb.table = db.open_table(table)
            table.compact_files()
            table.to_lance().optimize.optimize_indices()
            table.cleanup_old_versions(
                datetime.timedelta(seconds=self.CLEANUP_TIME))

            # Perform a "upsert" operation
            table.merge_insert(merge_on)   \
                .when_not_matched_insert_all() \
                .execute(data)

            table.cleanup_old_versions(
                older_than=datetime.timedelta(seconds=self.CLEANUP_TIME), delete_unverified=True
            )
        # if the table doesn't exist, create it with the data
        except FileNotFoundError:
            # Handle the case where the table does not exist
            print(
                f"Creating table {table} because it does not exist.")
            # create table
            table: lancedb.table = db.create_table(name=table, data=data)
