import sqlite3
from sqlite3 import Error, Connection

from base_functions import *
from persistence.DatasetEnfMean import DatasetEnfMean
from persistence.DatasetEnfMeanShutter import DatasetEnfMeanShutter
from persistence.DatasetEnfSuperpixel import DatasetEnfSuperpixel
from persistence.DatasetVideoMeanShutter import DatasetVideoMeanShutter
from persistence.DatasetVideoSuperpixel import DatasetVideoSuperpixel
from persistence.StatsCompression import StatsCompression
from persistence.StatsDurationCorrelation import StatsDurationCorrelation, StatsDurationCorrelationSD
from persistence.Video import Video
from persistence.DatasetVideoMean import DatasetVideoMean

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)


class Persistence:

    def __init__(self, dry_run=False):
        self.__dry_run = dry_run
        self.__connection: Connection = self.__create_connection()
        self.__create_tables()

    def get_connection(self):
        return self.__connection

    def __create_connection(self):
        db_file = f'{get_base_path()}/enf_analysis.db'
        try:
            self.__connection = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            # self.__connection = sqlite3.connect(db_file)
            self.__connection.row_factory = sqlite3.Row
            return self.__connection
        except Error as e:
            logger.warning(e)
            exit()

    def __create_tables(self):
        self.__connection.execute(Video.stmt_create_table)
        self.__connection.execute(DatasetVideoMean.stmt_create_table)
        self.__connection.execute(DatasetVideoMeanShutter.stmt_create_table)
        self.__connection.execute(DatasetVideoSuperpixel.stmt_create_table)
        self.__connection.execute(DatasetEnfSuperpixel.stmt_create_table)
        self.__connection.execute(DatasetEnfMean.stmt_create_table)
        self.__connection.execute(DatasetEnfMeanShutter.stmt_create_table)
        self.__connection.execute(StatsDurationCorrelation.stmt_create_table)
        self.__connection.execute(StatsDurationCorrelationSD.stmt_create_table)
        self.__connection.execute(StatsCompression.stmt_create_table)
        self.__connection.execute("""
            create view if not exists dataset_sp as
            select v.id as v_id, v.filename, v.duration, v.motion, v.hint, 
                sp.id as v_sp_id, lightness_threshold, motion_threshold, sp.hint as sp_hint,
                ds_enf_sp.p_max, ds_enf_sp.p_mean, ds_enf_sp.p_median, ds_enf_sp.p_top_two, ds_enf_sp.max_correlation, ds_enf_sp.matching_diff, ds_enf_sp.csv, ds_enf_sp.comment, ds_enf_sp.id as enf_id
            from video as v 
            left join ds_video_sp as sp on v.id = sp.v_id
            left join ds_enf_sp on sp.id = ds_enf_sp.ds_video_sp_id
            where v.hint is null or v.hint not like 'shutter%'
        """)
        self.__connection.execute("""
            create view if not exists dataset_mean as
            select v.id as v_id, v.filename, v.duration, v.motion, v.hint, 
                vm.id as v_m_id, lightness_threshold, motion_threshold, 
                ds_enf_mean.max_correlation, ds_enf_mean.matching_diff, ds_enf_mean.csv, ds_enf_mean.comment, ds_enf_mean.id as enf_id
            from video as v 
            left join ds_video_mean as vm on v.id = vm.v_id
            left join ds_enf_mean on vm.id = ds_enf_mean.ds_video_m_id
            where v.hint is null or v.hint not like 'shutter%'
            """)
        self.__connection.execute("""
            create view if not exists dataset_mean_shutter as
            select v.id as v_id, v.filename, v.duration, v.motion, v.hint, 
                vm.id as v_m_id, lightness_threshold, motion_threshold, 
                ds_enf_mean_shutter.max_correlation, ds_enf_mean_shutter.matching_diff, ds_enf_mean_shutter.csv, ds_enf_mean_shutter.comment, ds_enf_mean_shutter.id as enf_id
            from video as v 
            left join ds_video_mean_shutter as vm on v.id = vm.v_id
            left join ds_enf_mean_shutter on vm.id = ds_enf_mean_shutter.ds_video_m_id
            where v.hint like 'shutter%'
            """)
        self.__connection.execute("""create view if not exists unprocessed_sp_videos as
            select video.* from video left outer join ds_video_sp 
            on video.id = ds_video_sp.v_id 
            where ds_video_sp.id is null and video.hint not like 'shutter%'""")
        self.__connection.execute("""create view if not exists unprocessed_mean_videos as
            select video.* from video left outer join ds_video_mean 
            on video.id = ds_video_mean.v_id 
            where ds_video_mean.id is null and video.hint not like 'shutter%'""")
        self.__connection.execute("""create view if not exists unprocessed_mean_shutter_videos as
            select video.* from video left outer join ds_video_mean_shutter
            on video.id = ds_video_mean_shutter.v_id 
            where ds_video_mean_shutter.id is null and video.hint like 'shutter%'""")
        self.__connection.execute("""create view if not exists dataset_compression as
            select sc.*, filename, motion, hint from video as v
            inner join stats_compression as sc on v.id = sc.v_id""")
        self.__connection.commit()

    def query(self, query):
        if not self.__dry_run:
            if self.__connection is not None:
                cursor = self.__connection.cursor()
                cursor.execute(query)
                self.__connection.commit()
                return cursor.fetchall()
            else:
                logger.warning("Couldn't save StatsFalseDayMatch. Connection is None!")
        else:
            logger.debug(f"dry run: didn't execute query: {query}")

    def close(self):
        if self.__connection is not None:
            self.__connection.close()


if __name__ == "__main__":
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.DEBUG)
    persistence = Persistence()

    persistence.close()
    # ds_v = DatasetVideo()
    # ds_v.filename = "C2022-08-15T203618Z.MP4"
    # print(ds_v.get_video_timestamp())
