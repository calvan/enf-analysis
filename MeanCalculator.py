import cv2
from base_functions import *
import numpy as np

from persistence.DatasetVideoMean import DatasetVideoMean

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)


class MeanCalculator:

    def __init__(self, lightness_threshold=None, ds_video_mean: DatasetVideoMean = None):
        self.__ds_video_mean = ds_video_mean
        self.lightness_threshold = lightness_threshold
        self.mean = []

    def process_first_frame(self, frame):
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        if self.lightness_threshold is not None:
            if np.max(gray) < self.lightness_threshold:
                logger.warning(
                    f"threshold greater than brightest spot! threshold: {self.lightness_threshold}, brightest spot: {np.max(gray)}")
                self.lightness_threshold = int(np.median(gray))  # int(np.median(luminance) / 3)
                logger.warning(f"using auto threshold {self.lightness_threshold}")
                if self.__ds_video_mean is not None:
                    self.__ds_video_mean.lightness_threshold = self.lightness_threshold
                    self.__ds_video_mean.hint = f"{self.__ds_video_mean}, auto threshold"
            self.mean.append(gray[gray > self.lightness_threshold].mean())
            logger.debug(f"using threshold {self.lightness_threshold}")
        else:
            self.mean.append(np.mean(gray))

    def process(self, frame):
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        if self.lightness_threshold is not None:
            self.mean.append(gray[gray > self.lightness_threshold].mean())
        else:
            self.mean.append(np.mean(gray))
