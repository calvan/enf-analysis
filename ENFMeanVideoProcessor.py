from typing import Optional

from MeanCalculator import MeanCalculator
from VideoGrabber import VideoGrabber
import numpy as np
import cv2
from base_functions import *
from persistence.DatasetVideoMean import DatasetVideoMean

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)#
#
# def slice_csv_data(csv_data, timestamp, enf_length, offset=30):
#     start = csv_data.index.get_loc(timestamp)
#     end = start + enf_length + offset
#     start -= offset
#     return csv_data[start:end]


class ENFMeanVideoProcessor:

    def __init__(self, video_filename: str, lightness_threshold=None, data_dir="data", save_images=True):
        self.__lightness_threshold = lightness_threshold
        self.__data_dir = data_dir
        self.__video_filename = video_filename
        self.__vg: Optional[VideoGrabber] = None
        self.__mean_per_frame = []
        self.__save_images = save_images
        self.__mc = MeanCalculator(lightness_threshold=self.__lightness_threshold)

    def get_mean_data_filename(self):
        return f"{self.__data_dir}/{self.__video_filename}_mean_per_frame.npy"

    def process_video(self, video_file, start_frame_nr=0, end_frame_nr=None):
        create_directories(self.__data_dir)
        self.__vg = VideoGrabber(videofile=video_file, start_frame_nr=start_frame_nr, end_frame=end_frame_nr)
        self.__vg.first_frame(callback=self.__process_first_frame)
        self.__vg.grab(callback=self.__process)
        np.save(self.get_mean_data_filename(), self.__mc.mean)
        logger.info(f"done: {self.__video_filename}")

    def __process_first_frame(self, frame_nr, frame):
        img_name = f'{self.__data_dir}/{self.__video_filename}-first.jpg'
        if self.__save_images:
            cv2.imwrite(img_name, frame)
        img_name = f'{self.__data_dir}/{self.__video_filename}-threshold.jpg'
        frame_threshold = cv2.cvtColor(np.copy(frame), cv2.COLOR_BGR2GRAY)
        if self.__lightness_threshold is not None:
            indices = np.where(frame_threshold < self.__lightness_threshold)
            frame_threshold[indices] = 0
        if self.__save_images:
            cv2.imwrite(img_name, frame_threshold)
        self.__mc.process_first_frame(frame)

    def __process(self, frame_nr, frame):
        self.__mc.process(frame)
        if frame_nr % 100 == 0:
            logger.debug(f'{frame_nr + self.__vg.get_start_frame_nr()} / {self.__vg.total_frames()}')
