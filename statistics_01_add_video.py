from pathlib import Path
from persistence.Persistence import Persistence
from VideoGrabber import VideoGrabber
from persistence.Video import VideoPersistence
from base_functions import *

logger = logging.getLogger(__file__)
logger.setLevel(LOGGER_LEVEL)


def get_video_stats(frame_nr, frame):
    pass


def add_video(videopath: Path, network_frequency, video_contains_motion, hint):
    vg = VideoGrabber(videofile=str(videopath), end_frame=1)
    vg.first_frame(get_video_stats)

    persistence = Persistence()
    vp = VideoPersistence(connection=persistence.get_connection())
    video = vp.create_entry()
    video.duration = round(vg.total_duration())
    video.fps = round(vg.get_fps())
    video.fps_real = vg.get_fps()
    video.motion = video_contains_motion
    video.expected_enf_frequency = calc_alias(network_frequency, video.fps_real)
    video.filename = videopath.name
    video.hint = hint
    vp.save(video)
    persistence.close()


if __name__ == '__main__':
    network_frequency = 50
    video_contains_motion = 0
    filename = "A2022-09-17T183638Z_1024.m4v"
    hint = ""  # "shutter, 1/750"  # "daylight, w light"  # "1/60"  # "white wall" #"daylight, wo light"
    videopath = Path(f'{get_input_path()}/{filename}')

    if videopath.exists():
        add_video(videopath, network_frequency, video_contains_motion, hint)
        logger.info(f"added video: {videopath.name}")
    else:
        logger.error(f"couldn't access videofile: {str(videopath)}")
