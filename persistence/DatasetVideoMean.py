import sqlite3
from sqlite3 import Connection
from typing import Optional
from base_functions import *
from persistence.Video import Video, VideoPersistence

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)


class DatasetVideoMean:
    table_name = "ds_video_mean"
    stmt_create_table = """
            CREATE TABLE IF NOT EXISTS """ + table_name + """ (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                v_id INTEGER,
                lightness_threshold INTEGER,
                motion_threshold REAL,
                hint TEXT
            )"""

    def __init__(self):
        self.id: Optional[int] = None
        self.v_id: Optional[int] = None
        self.lightness_threshold: Optional[int] = None
        self.motion_threshold: Optional[float] = None
        self.hint = ""

        self.video: Optional[Video] = None


class DatasetVideoMeanPersistence:

    def __init__(self, connection: Connection, dry_run=False):
        self.__connection = connection
        self.__dry_run = dry_run

    def create_entry(self, video: Video) -> DatasetVideoMean:
        if not self.__dry_run:
            if self.__connection is not None:
                return self.__create_new_dataset_video_mean(video)
            else:
                logger.warning("No connection present")
                exit()
        else:
            logger.debug(f'dry run: creating empty DatasetVideoMean')
            return DatasetVideoMean()

    def save(self, dvm: DatasetVideoMean):
        if not self.__dry_run:
            if self.__connection is not None:
                cursor = self.__connection.cursor()
                stmt = """update """ + DatasetVideoMean.table_name + """ set
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
                msg = "Couldn't save DatasetVideoMean. Connection is None!"
                logger.warning(msg)
                raise Exception(msg)
        else:
            logger.debug("dry run: didn't save DatasetVideoMean")

    def __create_new_dataset_video_mean(self, video: Video) -> DatasetVideoMean:
        cursor = self.__connection.cursor()
        cursor.execute(f"insert into {DatasetVideoMean.table_name} (v_id) values (?)", (video.id,))
        self.__connection.commit()
        cursor.execute(f"select * from {DatasetVideoMean.table_name} where id=?", (cursor.lastrowid,))
        return self.__convert_row_2_video_mean(cursor.fetchone(), video)

    def __convert_row_2_video_mean(self, row: sqlite3.Row, video: Video = None) -> DatasetVideoMean:
        v = DatasetVideoMean()
        v.id = row['id']
        v.v_id = row['v_id']
        v.lightness_threshold = row['lightness_threshold']
        v.motion_threshold = row['motion_threshold']
        v.hint = row['hint']
        v.video = video
        return v

    def find_video_mean_by_id(self, video_mean_id: int) -> DatasetVideoMean:
        if self.__connection is not None:
            try:
                cursor = self.__connection.cursor()
                cursor.execute(f"select * from {DatasetVideoMean.table_name} where id=?", (video_mean_id,))
                video_mean =  self.__convert_row_2_video_mean(cursor.fetchone())
                vp = VideoPersistence(connection=self.__connection)
                video = vp.find_video_by_id(video_mean.v_id)
                video_mean.video = video
                return video_mean
            except Exception as ex:
                logger.error(ex)
        else:
            logger.warning("No connection present.")
            raise Exception("No connection present.")

    def find_all_dataset_mean(self):
        if self.__connection is not None:
            try:
                cursor = self.__connection.cursor()
                cursor.execute(f"select * from dataset_mean")
                return cursor.fetchall()
            except Exception as ex:
                logger.error(ex)
        else:
            logger.warning("No connection present.")
            raise Exception("No connection present.")
