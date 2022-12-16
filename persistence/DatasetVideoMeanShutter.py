import sqlite3
from sqlite3 import Connection
from typing import Optional
from base_functions import *
from persistence.DatasetVideoMean import DatasetVideoMean
from persistence.Video import Video, VideoPersistence

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)


class DatasetVideoMeanShutter(DatasetVideoMean):
    table_name = "ds_video_mean_shutter"
    stmt_create_table = """
            CREATE TABLE IF NOT EXISTS """ + table_name + """ (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                v_id INTEGER,
                lightness_threshold INTEGER,
                motion_threshold REAL,
                hint TEXT
            )"""

    def __init__(self):
        super().__init__()


class DatasetVideoMeanShutterPersistence:

    def __init__(self, connection: Connection, dry_run=False):
        self.__connection = connection
        self.__dry_run = dry_run

    def create_entry(self, video: Video) -> DatasetVideoMeanShutter:
        if not self.__dry_run:
            if self.__connection is not None:
                return self.__create_new_dataset_video_mean_shutter(video)
            else:
                logger.warning("No connection present")
                exit()
        else:
            logger.debug(f'dry run: creating empty DatasetVideoMean')
            return DatasetVideoMeanShutter()

    def save(self, dvm: DatasetVideoMeanShutter):
        if not self.__dry_run:
            if self.__connection is not None:
                cursor = self.__connection.cursor()
                stmt = """update """ + DatasetVideoMeanShutter.table_name + """ set
                       v_id=?,
                       lightness_threshold=?,
                       motion_threshold=?,
                       hint=?
                       where id=?
                       """
                cursor.execute(stmt,
                               (dvm.v_id,
                                dvm.lightness_threshold,
                                dvm.motion_threshold,
                                dvm.hint,
                                dvm.id)
                               )
                self.__connection.commit()
            else:
                msg = "Couldn't save DatasetVideoMeanShutter. Connection is None!"
                logger.warning(msg)
                raise Exception(msg)
        else:
            logger.debug("dry run: didn't save DatasetVideoMeanShutter")

    def __create_new_dataset_video_mean_shutter(self, video: Video) -> DatasetVideoMeanShutter:
        cursor = self.__connection.cursor()
        cursor.execute(f"insert into {DatasetVideoMeanShutter.table_name} (v_id) values (?)", (video.id,))
        self.__connection.commit()
        cursor.execute(f"select * from {DatasetVideoMeanShutter.table_name} where id=?", (cursor.lastrowid,))
        return self.__convert_row_2_video_mean_shutter(cursor.fetchone(), video)

    def __convert_row_2_video_mean_shutter(self, row: sqlite3.Row, video: Video = None) -> DatasetVideoMeanShutter:
        v = DatasetVideoMeanShutter()
        v.id = row['id']
        v.v_id = row['v_id']
        v.lightness_threshold = row['lightness_threshold']
        v.motion_threshold = row['motion_threshold']
        v.hint = row['hint']
        v.video = video
        return v

    def find_video_mean_shutter_by_id(self, video_mean_id: int) -> DatasetVideoMeanShutter:
        if self.__connection is not None:
            try:
                cursor = self.__connection.cursor()
                cursor.execute(f"select * from {DatasetVideoMeanShutter.table_name} where id=?", (video_mean_id,))
                video_mean = self.__convert_row_2_video_mean_shutter(cursor.fetchone())
                vp = VideoPersistence(connection=self.__connection)
                video = vp.find_video_by_id(video_mean.v_id)
                video_mean.video = video
                return video_mean
            except Exception as ex:
                logger.error(ex)
        else:
            logger.warning("No connection present.")
            raise Exception("No connection present.")

    def find_all_video_mean_shutter(self) -> [DatasetVideoMeanShutter]:
        if self.__connection is not None:
            try:
                cursor = self.__connection.cursor()
                cursor.execute(f"select * from {DatasetVideoMeanShutter.table_name}")
                result = []
                for row in cursor.fetchall():
                    video_mean = self.__convert_row_2_video_mean_shutter(row)
                    vp = VideoPersistence(connection=self.__connection)
                    video = vp.find_video_by_id(video_mean.v_id)
                    video_mean.video = video
                    result.append(video_mean)
                return result
            except Exception as ex:
                logger.error(ex)
        else:
            logger.warning("No connection present.")
            raise Exception("No connection present.")

    def find_all_dataset_mean_shutter(self, comment, csv="GDE%"):
        if self.__connection is not None:
            try:
                cursor = self.__connection.cursor()
                cursor.execute(f"select * from dataset_mean_shutter where comment like ? and csv like ?", (comment,csv))
                return cursor.fetchall()
            except Exception as ex:
                logger.error(ex)
        else:
            logger.warning("No connection present.")
            raise Exception("No connection present.")
