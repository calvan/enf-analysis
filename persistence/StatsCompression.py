import sqlite3
from sqlite3 import Connection
from typing import Optional
from base_functions import *

logger = logging.getLogger(__file__)
logger.setLevel(LOGGER_LEVEL)


class StatsCompression:
    table_name = "stats_compression"
    stmt_create_table = """
                CREATE TABLE IF NOT EXISTS """ + table_name + """ (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    v_id,
                    compression TEXT,
                    max_correlation REAL,
                    p_median REAL,
                    matching_diff INTEGER,                    
                    comment TEXT
                )"""
    stmt_create = f"insert into {table_name} (v_id) values (?)"
    stmt_find_by_id = f"select * from {table_name} where id=?"

    def __init__(self):
        self.id: Optional[int] = None
        self.v_id: Optional[int] = None
        self.compression: Optional[str] = None
        self.max_correlation: Optional[float] = None
        self.p_median: Optional[float] = None
        self.matching_diff: Optional[int] = None
        self.comment = ""

    def stmt_update(self):
        return """
        update """ + self.table_name + """ set
        v_id=?,
        compression=?,
        max_correlation=?,        
        p_median=?,
        matching_diff=?,
        comment=?
        where id=?
        """, (self.v_id, self.compression, self.max_correlation, self.p_median, self.matching_diff, self.comment, self.id)


class StatsCompressionPersistence:

    def __init__(self, connection: Connection, dry_run=False):
        self.__connection = connection
        self.__dry_run = dry_run

    def create_stats_compression(self, v_id: int) -> StatsCompression:
        if not self.__dry_run:
            if self.__connection is not None:
                return self.__create_new_stats_compression(v_id)
            else:
                logger.warning("No connection present")
                exit()
        else:
            logger.debug(f'dry run: creating empty StatsCompression')
            sdc = StatsCompression()
            sdc.id = None
            sdc.v_id = v_id
            return sdc

    def __create_new_stats_compression(self, v_id: int) -> StatsCompression:
        cursor = self.__connection.cursor()
        cursor.execute(StatsCompression.stmt_create, (v_id,))
        self.__connection.commit()
        cursor.execute(StatsCompression.stmt_find_by_id, (cursor.lastrowid,))
        return StatsCompressionPersistence.__convert_from_sql(cursor.fetchone())

    def save_stats_compression(self, sdc: StatsCompression):
        if not self.__dry_run:
            if self.__connection is not None:
                cursor = self.__connection.cursor()
                update = sdc.stmt_update()
                cursor.execute(update[0], update[1])
                self.__connection.commit()
            else:
                logger.warning("Couldn't save StatsCompression. Connection is None!")
        else:
            logger.debug("dry run: didn't save StatsCompression")

    @staticmethod
    def __convert_from_sql(row: sqlite3.Row) -> StatsCompression:
        sdc = StatsCompression()
        sdc.id = row["id"]
        sdc.v_id = row["v_id"]
        sdc.compression = row["compression"]
        sdc.max_correlation = row["max_correlation"]
        sdc.p_median = row["p_median"]
        sdc.matching_diff = row["matching_diff"]
        sdc.comment = row["comment"]
        return sdc
