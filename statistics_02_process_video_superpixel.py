from ENFSuperpixelVideoProcessor import ENFSuperpixelVideoProcessor
from persistence.Persistence import Persistence
from base_functions import *
from persistence.DatasetVideoSuperpixel import DatasetVideoSuperpixelPersistence, DatasetVideoSuperpixel
from persistence.Video import VideoPersistence


def process(video_id, lightness_threshold, motion_detection, motion_threshold, hint, dry_run=False):
    persistence = Persistence()
    logger.info(f"processing video_id: {video_id}, motion_detection: {motion_detection}")
    try:
        vp = VideoPersistence(persistence.get_connection(), dry_run=dry_run)
        dvsp = DatasetVideoSuperpixelPersistence(persistence.get_connection(), dry_run=dry_run)

        video = vp.find_video_by_id(video_id)
        video_path = f'{get_input_path()}/{video.filename}'
        dvs: DatasetVideoSuperpixel = dvsp.create_entry(video)
        logger.info(f"created DatasetVideoSuperpixel: {dvs.id}")
        dvs.lightness_threshold = lightness_threshold
        dvs.hint = hint
        if motion_detection:
            dvs.motion_threshold = motion_threshold
        dvsp.save(dvs)
        destination = get_destination_path(dvs.id, DESTINATION_SUPERPIXEL)
        if not dry_run:
            create_directories(destination)
        enf_sp_vp = ENFSuperpixelVideoProcessor(show_final_image=show_final_image, show_background=show_final_image,
                                                show_motion_free_image=show_final_image,
                                                lightness_threshold=dvs.lightness_threshold,
                                                motion_threshold_factor=dvs.motion_threshold,
                                                data_dir=destination, motion_detection=motion_detection,
                                                dataset_video=dvs, dry_run=dry_run)
        enf_sp_vp.process_video(video_path)
        dvsp.save(dvs)
        logger.info(f"processed video_id: {video_id}")
    except Exception as ex:
        logger.warning(f'Exception: {ex}')
    finally:
        persistence.close()


if __name__ == "__main__":
    logger = logging.getLogger(__file__)
    logger.setLevel(LOGGER_LEVEL)

    dry_run = False
    show_final_image = False

    lightness_threshold = 120
    motion_threshold = .2
    hint = ""  # "wo detection"
    video_ids = []  # if empty, unprocessed videos will be processed
    if not video_ids:
        persistence = Persistence(dry_run=dry_run)
        vp = VideoPersistence(persistence.get_connection())
        videos = vp.find_unprocessed_sp_videos()
        persistence.close()
        logger.info(f'processing videos with ids: {list(map(lambda vid: vid.id, videos))}')
        for video in videos:
            motion_detection = video.motion
            process(video.id, lightness_threshold, motion_detection, motion_threshold, hint, dry_run=dry_run)
    else:
        logger.info(f'processing videos with ids: {video_ids}')
        motion_detection = True
        for video_id in video_ids:
            process(video_id, lightness_threshold, motion_detection, motion_threshold, hint, dry_run=dry_run)

    logger.info("processed all videos")
