import sqlite3
from sqlite3 import Connection
from base_functions import *
from persistence.DatasetEnfMean import DatasetEnfMean
from persistence.DatasetVideoMeanShutter import DatasetVideoMeanShutter, DatasetVideoMeanShutterPersistence

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)


class DatasetEnfMeanShutter(DatasetEnfMean):
    table_name = "ds_enf_mean_shutter"
    stmt_create_table = """
            CREATE TABLE IF NOT EXISTS """ + table_name + """ (
                id INTEGER PRIMARY KEY AUTOINCREMENT,                 
                ds_video_m_id INTEGER,
                bandpass_order INTEGER,
                bandpass_width REAL,
                max_correlation REAL,
                matching_diff INTEGER,
                csv TEXT,
                comment TEXT
            )"""

    def __init__(self):
        super().__init__()


class DatasetEnfMeanShutterPersistence:

    def __init__(self, connection: Connection, dry_run=False):
        self.__connection = connection
        self.__dry_run = dry_run

    def save(self, dem: DatasetEnfMeanShutter):
        if not self.__dry_run:
            if self.__connection is not None:
                cursor = self.__connection.cursor()
                stmt = """update """ + DatasetEnfMeanShutter.table_name + """ set
                       ds_video_m_id=?,
                       bandpass_order=?,
                       bandpass_width=?,
                       max_correlation=?,
                       matching_diff=?,
                       csv=?,
                       comment=?
                       where id=?
                       """
                cursor.execute(stmt,
                               (dem.ds_video_m_id,
                                dem.bandpass_order,
                                dem.bandpass_width,
                                dem.max_correlation,
                                dem.matching_diff,
                                dem.csv,
                                dem.comment,
                                dem.id)
                               )
                self.__connection.commit()
            else:
                msg = "Couldn't save DatasetEnfMeanShutter. Connection is None!"
                logger.warning(msg)
                raise Exception(msg)
        else:
            logger.debug("dry run: didn't save DatasetEnfMeanShutter")

    def create_entry(self, ds_v_m: DatasetVideoMeanShutter) -> DatasetEnfMeanShutter:
        if not self.__dry_run:
            if self.__connection is not None:
                return self.__create_new_dataset_enf_mean(ds_v_m)
            else:
                logger.warning("No connection present")
                exit()
        else:
            logger.debug(f'dry run: creating empty DatasetEnfMeanShutter')
            return DatasetEnfMeanShutter()

    def __create_new_dataset_enf_mean(self, ds_v_m: DatasetVideoMeanShutter) -> DatasetEnfMeanShutter:
        cursor = self.__connection.cursor()
        cursor.execute(f"insert into {DatasetEnfMeanShutter.table_name} (ds_video_m_id) values (?)", (ds_v_m.id,))
        self.__connection.commit()
        cursor.execute(f"select * from {DatasetEnfMeanShutter.table_name} where id=?", (cursor.lastrowid,))
        return self.__convert_row_2_enf_mean(cursor.fetchone(), ds_v_m)

    def __convert_row_2_enf_mean(self, row: sqlite3.Row,
                                 ds_v_m: DatasetVideoMeanShutter = None) -> DatasetEnfMeanShutter:
        v = DatasetEnfMeanShutter()
        v.id = row['id']
        v.ds_video_m_id = row['ds_video_m_id']
        v.max_correlation = row['max_correlation']
        v.matching_diff = row['matching_diff']
        v.csv = row['csv']
        v.comment = row['comment']
        v.bandpass_order = row['bandpass_order']
        v.bandpass_width = row['bandpass_width']
        return v

    def find_ds_enf_by_id(self, v_m_id: int) -> DatasetEnfMeanShutter:
        if self.__connection is not None:
            try:
                cursor = self.__connection.cursor()
                cursor.execute(f"select * from {DatasetEnfMeanShutter.table_name} where id=?", (v_m_id,))
                d_enf_mean = self.__convert_row_2_enf_mean(cursor.fetchone())
                ds_v_m_p = DatasetVideoMeanShutterPersistence(connection=self.__connection)
                video_mean = ds_v_m_p.find_video_mean_shutter_by_id(d_enf_mean.ds_video_m_id)
                d_enf_mean.ds_video_mean = video_mean
                return d_enf_mean
            except Exception as ex:
                logger.error(ex)
        else:
            logger.warning("No connection present.")
            raise Exception("No connection present.")

    def delete(self, id):
        if not self.__dry_run:
            if self.__connection is not None:
                cursor = self.__connection.cursor()
                cursor.execute(f"delete from {DatasetEnfMeanShutter.table_name} where id=?", (id,))
                self.__connection.commit()
            else:
                logger.warning("Couldn't delete enf entry. Connection is None!")
        else:
            logger.debug("dry run: didn't delete enf entry")


def init_DatasetEnfMean_without_ids(ds_src: DatasetEnfMeanShutter, ds_dest: DatasetEnfMeanShutter):
    ds_dest.bandpass_order = ds_src.bandpass_order
    ds_dest.bandpass_width = ds_src.bandpass_width
    ds_dest.csv = ds_src.csv
    ds_dest.max_correlation = ds_src.max_correlation
    ds_dest.matching_diff = ds_src.matching_diff
    ds_dest.comment = ds_src.comment
