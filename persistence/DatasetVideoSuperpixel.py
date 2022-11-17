import sqlite3
from sqlite3 import Connection
from typing import Optional
from base_functions import *
from persistence.Video import Video, VideoPersistence

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)


class DatasetVideoSuperpixel:
    table_name = "ds_video_sp"
    stmt_create_table = """
            CREATE TABLE IF NOT EXISTS """ + table_name + """ (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                v_id INTEGER,
                superpixel INTEGER,
                region_size INTEGER,
                lightness_threshold INTEGER,
                motion_threshold REAL,
                hint TEXT
            )"""

    def __init__(self):
        self.id: Optional[int] = None
        self.v_id: Optional[int] = None
        self.superpixel: Optional[int] = None
        self.region_size: Optional[int] = None
        self.lightness_threshold: Optional[int] = None
        self.motion_threshold: Optional[float] = None
        self.hint = ""

        self.video: Optional[Video] = None


class DatasetVideoSuperpixelPersistence:

    def __init__(self, connection: Connection, dry_run=False):
        self.__connection = connection
        self.__dry_run = dry_run

    def create_entry(self, video: Video) -> DatasetVideoSuperpixel:
        if not self.__dry_run:
            if self.__connection is not None:
                return self.__create_new_dataset_video_superpixel(video)
            else:
                logger.warning("No connection present")
                exit()
        else:
            logger.debug(f'dry run: creating empty DatasetVideoSuperpixel')
            return DatasetVideoSuperpixel()

    def save(self, dvs: DatasetVideoSuperpixel):
        if not self.__dry_run:
            if self.__connection is not None:
                cursor = self.__connection.cursor()
                stmt = """update """ + DatasetVideoSuperpixel.table_name + """ set
                       v_id=?,
                       superpixel=?,
                       region_size=?,
                       lightness_threshold=?,
                       motion_threshold=?,
                       hint=?
                       where id=?
                       """
                cursor.execute(stmt,
                               (dvs.v_id,
                                dvs.superpixel,
                                dvs.region_size,
                                dvs.lightness_threshold,
                                dvs.motion_threshold,
                                dvs.hint,
                                dvs.id)
                               )
                self.__connection.commit()
            else:
                msg = "Couldn't save DatasetVideoSuperpixel. Connection is None!"
                logger.warning(msg)
                raise Exception(msg)
        else:
            logger.debug("dry run: didn't save DatasetVideoSuperpixel")

    def __create_new_dataset_video_superpixel(self, video: Video) -> DatasetVideoSuperpixel:
        cursor = self.__connection.cursor()
        cursor.execute(f"insert into {DatasetVideoSuperpixel.table_name} (v_id) values (?)", (video.id,))
        self.__connection.commit()
        cursor.execute(f"select * from {DatasetVideoSuperpixel.table_name} where id=?", (cursor.lastrowid,))
        return self.__convert_row_2_video_superpixel(cursor.fetchone(), video)

    def __convert_row_2_video_superpixel(self, row: sqlite3.Row, video: Video = None) -> DatasetVideoSuperpixel:
        v = DatasetVideoSuperpixel()
        v.id = row['id']
        v.v_id = row['v_id']
        v.superpixel = row['superpixel']
        v.region_size = row['region_size']
        v.lightness_threshold = row['lightness_threshold']
        v.motion_threshold = row['motion_threshold']
        v.hint = row['hint']
        v.video = video
        return v

    def find_video_superpixel_by_id(self, video_superpixel_id: int) -> DatasetVideoSuperpixel:
        if self.__connection is not None:
            try:
                cursor = self.__connection.cursor()
                cursor.execute(f"select * from {DatasetVideoSuperpixel.table_name} where id=?", (video_superpixel_id,))
                video_superpixel = self.__convert_row_2_video_superpixel(cursor.fetchone())
                vp = VideoPersistence(connection=self.__connection)
                video = vp.find_video_by_id(video_superpixel.v_id)
                video_superpixel.video = video
                return video_superpixel
            except Exception as ex:
                logger.error(ex)
        else:
            logger.warning("No connection present.")
            raise Exception("No connection present.")

    def find_all_dataset_sp(self):
        if self.__connection is not None:
            try:
                cursor = self.__connection.cursor()
                cursor.execute(f"select * from dataset_sp")
                return cursor.fetchall()
            except Exception as ex:
                logger.error(ex)
        else:
            logger.warning("No connection present.")
            raise Exception("No connection present.")
