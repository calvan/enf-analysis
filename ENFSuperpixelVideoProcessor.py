from pathlib import Path
from typing import Optional

from MeanMedianSuperpixelCalculator import MeanMedianSuperpixelCalculator
from MotionDetectorGSOC import MotionDetectorGSOC
from VideoGrabber import VideoGrabber
import numpy as np
from os import makedirs
import cv2
from base_functions import *
from persistence.DatasetVideoSuperpixel import DatasetVideoSuperpixel

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)


class ENFSuperpixelVideoProcessor:

    def __init__(self, show_final_image=False, show_background=False, show_motion_free_image=False,
                 motion_threshold_factor=.2, lightness_threshold=None, data_dir=None, motion_detection=True,
                 dataset_video: DatasetVideoSuperpixel = None, dry_run=False, save_img=True):
        self.__dataset_video = dataset_video
        self.__save_img = save_img
        self.__dry_run = dry_run
        self.__motion_detection = motion_detection
        self.__lightness_threshold = lightness_threshold
        self.__data_dir = data_dir
        self.__video_filename = ""
        self.__vg: Optional[VideoGrabber] = None
        self.__show_final_image = show_final_image
        self.__wait_key = show_background or show_final_image or show_motion_free_image
        self.__md = MotionDetectorGSOC(show_background=show_background, show_motion_free_image=show_motion_free_image,
                                       motion_threshold_factor=motion_threshold_factor, dataset_video=dataset_video)
        self.__mmc = MeanMedianSuperpixelCalculator(threshold=lightness_threshold, mode='mean',
                                                    ds_video_sp=dataset_video)

    def get_mean_data_filename(self, video_file=None):
        if video_file is None:
            return f"{self.__data_dir}/{self.__video_filename}_mean_per_spx.npy"
        else:
            return f"{self.__data_dir}/{video_file}_mean_per_spx.npy"

    def process_video(self, video_file, start_frame_nr=0, end_frame_nr=None):
        self.__video_filename = Path(video_file).name
        self.__vg = VideoGrabber(videofile=video_file, start_frame_nr=start_frame_nr, end_frame=end_frame_nr)
        self.__vg.first_frame(callback=self.__process_first_frame)
        self.__vg.grab(callback=self.__process_frame)
        mean_per_superpixel = self.__mmc.get_mean_per_superpixel(self.__md.get_steady_superpixel_indices())
        if not self.__dry_run and self.__data_dir:
            makedirs(self.__data_dir, exist_ok=True)
            np.save(self.get_mean_data_filename(), mean_per_superpixel)
        if self.__show_final_image:
            self.__md.show_image(self.__md.get_steady_superpixel_indices(), title="Segmented image result")
            cv2.waitKey()
            cv2.destroyAllWindows()
        if not self.__dry_run and self.__data_dir and self.__save_img:
            self.__md.save_image(f'{self.__data_dir}/{self.__video_filename}-final.png',
                                 self.__md.get_steady_superpixel_indices())
        self.__md.stop()
        logger.info(f"done: {self.__video_filename}")
        return mean_per_superpixel

    def __process_first_frame(self, frame_nr, frame):
        self.__md.first_frame(frame)
        self.__mmc.initialize(segmented_superpixel=self.__md.segmented_superpixel(),
                              total_nr_frames=self.__vg.total_frames())
        self.__mmc.first_frame(frame)
        if self.__dataset_video is not None:
            self.__dataset_video.lightness_threshold = self.__mmc.get_threshold()
        self.__md.apply_disabled_superpixel(self.__mmc.get_disabled_superpixels())
        if not self.__dry_run and self.__save_img:
            self.__md.save_image(f'{self.__data_dir}/{self.__video_filename}-first.jpg')

    def __process_frame(self, frame_nr, frame):
        if self.__motion_detection:
            self.__md.next_frame(frame)
            self.__md.show_motionless_image()
        self.__mmc.next_frame(frame)
        if frame_nr % 50 == 0:
            logger.debug(f'frame {frame_nr + self.__vg.get_start_frame_nr()} / {self.__vg.total_frames()}')
        # cv2.waitKey()
        if self.__wait_key:
            if cv2.waitKey(12) == ord('q'):
                self.__md.stop()
        else:
            self.__md.stop()
