from concurrent.futures import ThreadPoolExecutor, wait
from ENFMeanVideoProcessor import ENFMeanVideoProcessor
from persistence.Persistence import Persistence
from base_functions import *
from persistence.DatasetVideoMean import DatasetVideoMeanPersistence, DatasetVideoMean
from persistence.Video import VideoPersistence


def process(video_id, lightness_threshold, hint):
    persistence = Persistence()
    try:
        vp = VideoPersistence(persistence.get_connection())
        dvmp = DatasetVideoMeanPersistence(persistence.get_connection())

        video = vp.find_video_by_id(video_id)
        video_path = f'{get_input_path()}/{video.filename}'
        dvm: DatasetVideoMean = dvmp.create_entry(video)
        dvm.lightness_threshold = lightness_threshold
        dvm.hint = hint
        dvmp.save(dvm)
        destination = get_destination_path(dvm.id, DESTINATION_MEAN)
        enf_m_vp = ENFMeanVideoProcessor(dvm.video.filename, lightness_threshold=lightness_threshold,
                                         data_dir=destination)
        enf_m_vp.process_video(video_path)
    except Exception as ex:
        logger.warning(f'Exception: {ex}')
    finally:
        persistence.close()


if __name__ == "__main__":
    logger = logging.getLogger(__file__)
    logger.setLevel(LOGGER_LEVEL)
    show_final_image = False
    lightness_threshold = 120
    hint = ""  # "wo detection"

    video_ids = []  # if empty, unprocessed videos will be processed
    if not video_ids:
        persistence = Persistence()
        vp = VideoPersistence(persistence.get_connection())
        videos = vp.find_unprocessed_mean_videos()
        persistence.close()
        video_ids = list(map(lambda vid: vid.id, videos))
    logger.info(f'processing videos with ids: {video_ids}')

    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(process, v_id, lightness_threshold, hint) for v_id in video_ids]
        wait(futures)
    # process(1, lightness_threshold, motion_threshold, hint)

    logger.info("processed all videos")
