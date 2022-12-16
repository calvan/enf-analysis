import sqlite3
from sqlite3 import Connection
from typing import Optional
from base_functions import *

logger = logging.getLogger(__file__)
logger.setLevel(LOGGER_LEVEL)


class StatsDurationCorrelation:
    table_name = "stats_duration_correlation"
    stmt_create_table = """
                CREATE TABLE IF NOT EXISTS """ + table_name + """ (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ds_enf_id,
                    max_correlation REAL,
                    matching_diff INTEGER,
                    duration INTEGER,
                    comment TEXT
                )"""
    stmt_create = f"insert into {table_name} (ds_enf_id) values (?)"
    stmt_find_by_id = f"select * from {table_name} where id=?"

    def __init__(self):
        self.id: Optional[int] = None
        self.ds_enf_id: Optional[int] = None
        self.max_correlation: Optional[float] = None
        self.duration: Optional[int] = None
        self.matching_diff: Optional[int] = None
        self.comment = ""

    def stmt_update(self):
        return """
        update """ + self.table_name + """ set
        ds_enf_id=?,
        max_correlation=?,
        duration=?,
        matching_diff=?,
        comment=?
        where id=?
        """, (self.ds_enf_id, self.max_correlation, self.duration, self.matching_diff, self.comment, self.id)


class StatsDurationCorrelationSD(StatsDurationCorrelation):
    table_name = "stats_duration_correlation_sd"
    stmt_create_table = """
                    CREATE TABLE IF NOT EXISTS """ + table_name + """ (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ds_enf_id,
                        max_correlation REAL,
                        matching_diff INTEGER,
                        duration INTEGER,
                        comment TEXT
                    )"""
    stmt_create = f"insert into {table_name} (ds_enf_id) values (?)"
    stmt_find_by_id = f"select * from {table_name} where id=?"


class StatsDurationCorrelationPersistence:

    def __init__(self, connection: Connection, dry_run=False):
        self.__connection = connection
        self.__dry_run = dry_run

    def create_stats_duration_correlation(self, ds_enf_id: int) -> StatsDurationCorrelation:
        if not self.__dry_run:
            if self.__connection is not None:
                return self.__create_new_stats_duration_correlation(ds_enf_id)
            else:
                logger.warning("No connection present")
                exit()
        else:
            logger.debug(f'dry run: creating empty StatsDurationCorrelation')
            sdc = StatsDurationCorrelation()
            sdc.id = None
            sdc.ds_enf_id = ds_enf_id
            return sdc

    def create_stats_duration_correlation_sd(self, ds_enf_id: int) -> StatsDurationCorrelationSD:
        if not self.__dry_run:
            if self.__connection is not None:
                return self.__create_new_stats_duration_correlation_sd(ds_enf_id)
            else:
                logger.warning("No connection present")
                exit()
        else:
            logger.debug(f'dry run: creating empty StatsDurationCorrelation')
            sdc = StatsDurationCorrelationSD()
            sdc.id = None
            sdc.ds_enf_id = ds_enf_id
            return sdc

    def __create_new_stats_duration_correlation(self, ds_enf_id: int) -> StatsDurationCorrelation:
        cursor = self.__connection.cursor()
        cursor.execute(StatsDurationCorrelationSD.stmt_create, (ds_enf_id,))
        self.__connection.commit()
        cursor.execute(StatsDurationCorrelationSD.stmt_find_by_id, (cursor.lastrowid,))
        sdc = StatsDurationCorrelation()
        StatsDurationCorrelationPersistence.__convert_from_sql(cursor.fetchone(), sdc)
        return sdc

    def __create_new_stats_duration_correlation_sd(self, ds_enf_id: int) -> StatsDurationCorrelationSD:
        cursor = self.__connection.cursor()
        cursor.execute(StatsDurationCorrelationSD.stmt_create, (ds_enf_id,))
        self.__connection.commit()
        cursor.execute(StatsDurationCorrelationSD.stmt_find_by_id, (cursor.lastrowid,))
        sdcsd = StatsDurationCorrelationSD()
        StatsDurationCorrelationPersistence.__convert_from_sql(cursor.fetchone(), sdcsd)
        return sdcsd

    def save_stats_duration_correlation(self, sdc: StatsDurationCorrelation):
        if not self.__dry_run:
            if self.__connection is not None:
                cursor = self.__connection.cursor()
                update = sdc.stmt_update()
                cursor.execute(update[0], update[1])
                self.__connection.commit()
            else:
                logger.warning("Couldn't save StatsDurationCorrelation. Connection is None!")
        else:
            logger.debug(f"dry run: didn't save StatsDurationCorrelation: {sdc.stmt_update()}")

    @staticmethod
    def __convert_from_sql(row: sqlite3.Row, sdc: StatsDurationCorrelation):
        sdc.id = row["id"]
        sdc.ds_enf_id = row["ds_enf_id"]
        sdc.max_correlation = row["max_correlation"]
        sdc.duration = row["duration"]
        sdc.comment = row["comment"]
        return sdc
