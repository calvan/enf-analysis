import re
import sqlite3
from sqlite3 import Connection
from typing import Optional
from base_functions import *
from datetime import timedelta

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)


class Video:
    regex_video_filename = r"\d{4}-\d{2}-\d{2}T\d{6}Z"
    table_name = "video"
    stmt_create_table = """
                CREATE TABLE IF NOT EXISTS """ + table_name + """ (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    filename TEXT,
                    duration INTEGER,
                    expected_enf_frequency REAL,
                    fps INTEGER,
                    fps_real REAL,
                    motion INTEGER,
                    hint TEXT
                )"""

    def __init__(self):
        self.id: Optional[int] = None
        self.filename = ""
        self.expected_enf_frequency: Optional[float] = None
        self.duration: Optional[int] = None
        self.fps: Optional[int] = None
        self.fps_real: Optional[float] = None
        self.motion: Optional[int] = None
        self.hint = ""

    def get_video_timestamp(self, skipped_seconds=0):
        matches = re.search(self.regex_video_filename, self.filename)

        if matches:
            delta = timedelta(seconds=skipped_seconds)
            return datetime.strptime(matches.group(0), "%Y-%m-%dT%H%M%SZ") + delta
        return None


class VideoPersistence:

    def __init__(self, connection: Connection, dry_run=False):
        self.__connection = connection
        self.__dry_run = dry_run

    def create_entry(self) -> Video:
        if not self.__dry_run:
            if self.__connection is not None:
                return self.__create_new_dataset_video()
            else:
                logger.warning("No connection present")
                exit()
        else:
            logger.debug(f'dry run: creating empty Video')
            return Video()

    def save(self, video: Video):
        if not self.__dry_run:
            if self.__connection is not None:
                cursor = self.__connection.cursor()
                stmt = """update """ + Video.table_name + """ set
                       filename=?,
                       duration=?,
                       expected_enf_frequency=?,
                       fps=?,
                       fps_real=?,
                       motion=?,
                       hint=?
                       where id=?
                       """
                cursor.execute(stmt,
                               (video.filename,
                                video.duration,
                                video.expected_enf_frequency,
                                video.fps,
                                video.fps_real,
                                video.motion,
                                video.hint,
                                video.id)
                               )
                self.__connection.commit()
            else:
                msg = "Couldn't save Dataset. Connection is None!"
                logger.warning(msg)
                raise Exception(msg)
        else:
            logger.debug("dry run: didn't save Video")

    def __create_new_dataset_video(self) -> Video:
        cursor = self.__connection.cursor()
        cursor.execute(f"insert into {Video.table_name} (filename) values (?)", ("",))
        self.__connection.commit()
        cursor.execute(f"select * from {Video.table_name} where id=?", (cursor.lastrowid,))
        return self.__convert_row_2_video(cursor.fetchone())

    def __convert_row_2_video(self, row: sqlite3.Row) -> Video:
        v = Video()
        v.id = row['id']
        v.filename = row['filename']
        v.duration = row['duration']
        v.expected_enf_frequency = row['expected_enf_frequency']
        v.fps = row['fps']
        v.fps_real = row['fps_real']
        v.motion = bool(row['motion'])
        v.hint = row['hint']
        return v

    def find_video_by_id(self, video_id: int) -> Video:
        if self.__connection is not None:
            try:
                cursor = self.__connection.cursor()
                cursor.execute(f"select * from {Video.table_name} where id=?", (video_id,))
                return self.__convert_row_2_video(cursor.fetchone())
            except Exception as ex:
                logger.error(ex)
        else:
            logger.warning("No connection present.")
            raise Exception("No connection present.")

    def find_unprocessed_sp_videos(self) -> [Video]:
        if self.__connection is not None:
            try:
                cursor = self.__connection.cursor()
                cursor.execute(f"select * from unprocessed_sp_videos")
                result = []
                for row in cursor.fetchall():
                    result.append(self.__convert_row_2_video(row))
                return result
            except Exception as ex:
                logger.error(ex)
        else:
            logger.warning("No connection present.")
            raise Exception("No connection present.")

    def find_unprocessed_mean_videos(self) -> [Video]:
        if self.__connection is not None:
            try:
                cursor = self.__connection.cursor()
                cursor.execute(f"select * from unprocessed_mean_videos")
                result = []
                for row in cursor.fetchall():
                    result.append(self.__convert_row_2_video(row))
                return result
            except Exception as ex:
                logger.error(ex)
        else:
            logger.warning("No connection present.")
            raise Exception("No connection present.")

    def find_unprocessed_mean_shutter_videos(self) -> [Video]:
        if self.__connection is not None:
            try:
                cursor = self.__connection.cursor()
                cursor.execute(f"select * from unprocessed_mean_shutter_videos")
                result = []
                for row in cursor.fetchall():
                    result.append(self.__convert_row_2_video(row))
                return result
            except Exception as ex:
                logger.error(ex)
        else:
            logger.warning("No connection present.")
            raise Exception("No connection present.")
