import sqlite3
from sqlite3 import Connection
from typing import Optional
from base_functions import *
from ENFMetric import ENFMetricResult
from persistence.DatasetVideoSuperpixel import DatasetVideoSuperpixel

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)


class DatasetEnfSuperpixel:
    table_name = "ds_enf_sp"
    stmt_create_table = """
            CREATE TABLE IF NOT EXISTS """ + table_name + """ (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ds_video_sp_id INTEGER,
                bandpass_order INTEGER,
                bandpass_width REAL,
                p_max REAL,
                p_mean REAL,
                p_median REAL,
                p_top_two REAL,
                max_correlation REAL,
                matching_diff INTEGER,
                csv TEXT,
                comment TEXT
            )"""

    def __init__(self):
        self.id: Optional[int] = None
        self.ds_video_sp_id: Optional[int] = None
        self.bandpass_order: Optional[int] = None
        self.bandpass_width: Optional[float] = None
        self.max_correlation: Optional[float] = None
        self.matching_diff: Optional[int] = None
        self.csv = ""
        self.comment = ""
        self.metrics = ENFMetricResult()


class DatasetEnfSuperpixelPersistence:

    def __init__(self, connection: Connection, dry_run=False):
        self.__connection = connection
        self.__dry_run = dry_run

    def create_entry(self, ds_video_sp: DatasetVideoSuperpixel) -> DatasetEnfSuperpixel:
        if not self.__dry_run:
            if self.__connection is not None:
                return self.__create_new_dataset_enf_superpixel(ds_video_sp)
            else:
                logger.warning("No connection present")
                exit()
        else:
            logger.debug(f'dry run: creating empty DatasetEnfSuperpixel')
            return DatasetEnfSuperpixel()

    def save(self, des: DatasetEnfSuperpixel):
        if not self.__dry_run:
            if self.__connection is not None:
                cursor = self.__connection.cursor()
                stmt = """update """ + DatasetEnfSuperpixel.table_name + """ set
                       ds_video_sp_id=?,
                       bandpass_order=?,
                       bandpass_width=?,
                       p_max=?,
                       p_mean=?,
                       p_median=?,
                       p_top_two=?,
                       max_correlation=?,
                       matching_diff=?,
                       csv=?,
                       comment=?
                       where id=?
                       """
                cursor.execute(stmt,
                               (des.ds_video_sp_id,
                                des.bandpass_order,
                                des.bandpass_width,
                                des.metrics.max,
                                des.metrics.mean,
                                des.metrics.median,
                                des.metrics.top_two,
                                des.max_correlation,
                                des.matching_diff,
                                des.csv,
                                des.comment,
                                des.id)
                               )
                self.__connection.commit()
            else:
                msg = "Couldn't save DatasetEnfSuperpixel. Connection is None!"
                logger.warning(msg)
                raise Exception(msg)
        else:
            logger.debug("dry run: didn't save DatasetEnfSuperpixel")

    def __create_new_dataset_enf_superpixel(self, ds_video_sp: DatasetVideoSuperpixel) -> DatasetEnfSuperpixel:
        cursor = self.__connection.cursor()
        cursor.execute(f"insert into {DatasetEnfSuperpixel.table_name} (ds_video_sp_id) values (?)", (ds_video_sp.id,))
        self.__connection.commit()
        cursor.execute(f"select * from {DatasetEnfSuperpixel.table_name} where id=?", (cursor.lastrowid,))
        return self.__convert_row_2_enf_superpixel(cursor.fetchone(), ds_video_sp)

    def __convert_row_2_enf_superpixel(self, row: sqlite3.Row,
                                       ds_video_sp: DatasetVideoSuperpixel = None) -> DatasetEnfSuperpixel:
        v = DatasetEnfSuperpixel()
        v.id = row['id']
        v.ds_video_sp_id = row['ds_video_sp_id']
        v.bandpass_order = row['bandpass_order']
        v.bandpass_width = row['bandpass_width']
        v.metrics.max = row['p_max']
        v.metrics.mean = row['p_mean']
        v.metrics.median = row['p_median']
        v.metrics.top_two = row['p_top_two']
        v.max_correlation = row['max_correlation']
        v.matching_diff = row['matching_diff']
        v.csv = row['csv']
        v.comment = row['comment']
        return v

    def find_enf_superpixel_by_id(self, ds_enf_sp_id: int) -> DatasetEnfSuperpixel:
        if self.__connection is not None:
            try:
                cursor = self.__connection.cursor()
                cursor.execute(f"select * from {DatasetEnfSuperpixel.table_name} where id=?", (ds_enf_sp_id,))
                enf_superpixel = self.__convert_row_2_enf_superpixel(cursor.fetchone())
                return enf_superpixel
            except Exception as ex:
                logger.error(ex)
        else:
            logger.warning("No connection present.")
            raise Exception("No connection present.")

    def delete(self, id):
        if not self.__dry_run:
            if self.__connection is not None:
                cursor = self.__connection.cursor()
                cursor.execute(f"delete from {DatasetEnfSuperpixel.table_name} where id=?", (id,))
                self.__connection.commit()
            else:
                logger.warning("Couldn't delete enf entry. Connection is None!")
        else:
            logger.debug("dry run: didn't delete enf entry")


def init_DatasetEnfSuperpixel_without_ids(ds_src: DatasetEnfSuperpixel, ds_dest: DatasetEnfSuperpixel):
    ds_dest.bandpass_order = ds_src.bandpass_order
    ds_dest.bandpass_width = ds_src.bandpass_width
    ds_dest.csv = ds_src.csv
    ds_dest.metrics = ds_src.metrics
    ds_dest.max_correlation = ds_src.max_correlation
    ds_dest.matching_diff = ds_src.matching_diff
    ds_dest.comment = ds_src.comment