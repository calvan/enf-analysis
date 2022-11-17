import cv2
import numpy as np

from base_functions import *
from persistence.DatasetVideoSuperpixel import DatasetVideoSuperpixel

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)


class MeanMedianSuperpixelCalculator:

    # TODO: remove DatasetVideoSuperpixel
    def __init__(self, threshold=None, mode='mean', ds_video_sp: DatasetVideoSuperpixel = None):
        self.__threshold = threshold
        self.__ds_video_sp = ds_video_sp
        self.__mode = mode
        self.__selected_superpixels = []
        self.__pixel_locations = {}
        self.__current_frame = 0
        self.__superpixel_indices = None
        self.__superpixel_regions_mean: np.ndarray = None
        self.__superpixel_regions_median: np.ndarray = None

    def get_disabled_superpixels(self):
        disabled_superpixels = []
        for i in self.__superpixel_indices:
            if i not in self.__selected_superpixels:
                disabled_superpixels.append(i)
        return disabled_superpixels

    def initialize(self, segmented_superpixel, total_nr_frames):
        # self.__mean = np.zeros()
        segmented_superpixel_1d = np.reshape(segmented_superpixel, -1)
        self.__superpixel_indices = np.unique(segmented_superpixel_1d)
        for i in self.__superpixel_indices:
            self.__pixel_locations[i] = np.where(segmented_superpixel_1d == i)[0]
        self.__superpixel_regions_mean = np.zeros(shape=(len(self.__superpixel_indices) + 1, total_nr_frames),
                                                  dtype=np.float16)
        self.__superpixel_regions_median = np.zeros(shape=(len(self.__superpixel_indices) + 1, total_nr_frames),
                                                    dtype=np.float16)

    def get_threshold(self):
        return self.__threshold

    def first_frame(self, frame):
        # lab = cv2.cvtColor(frame, cv2.COLOR_BGR2Lab)
        # luminance = lab[:, :, 0].flatten()
        # v = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
        # luminance = v[:, :, 2].flatten()
        g = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        luminance = g.flatten()
        if self.__threshold is None:
            self.__threshold = int(np.median(luminance))  # int(np.median(luminance) / 3)
        elif np.max(luminance) < self.__threshold:
            logger.warning(
                f"threshold greater than brightest spot! threshold: {self.__threshold}, brightest spor: {np.max(luminance)}")
            self.__threshold = int(np.median(luminance))  # int(np.median(luminance) / 3)
            logger.warning(f"using auto threshold {self.__threshold}")
            if self.__ds_video_sp is not None:
                self.__ds_video_sp.lightness_threshold = self.__threshold
                self.__ds_video_sp.hint = f"{self.__ds_video_sp}, auto threshold"
        logger.debug(f"using threshold {self.__threshold}")
        for i in self.__superpixel_indices:
            superpixel_mean_intensity = np.mean(luminance[self.__pixel_locations[i]])
            superpixel_median_intensity = np.median(luminance[self.__pixel_locations[i]])
            if self.__mode == 'mean':
                if superpixel_mean_intensity > self.__threshold:
                    self.__add_mean_median(i, superpixel_mean_intensity, superpixel_median_intensity)
                    self.__selected_superpixels.append(i)
            else:
                if superpixel_median_intensity > self.__threshold:
                    self.__add_mean_median(i, superpixel_mean_intensity, superpixel_median_intensity)
                    self.__selected_superpixels.append(i)
        self.__current_frame += 1

    def next_frame(self, frame):
        # lab = cv2.cvtColor(frame, cv2.COLOR_BGR2Lab)
        # luminance = lab[:, :, 0].flatten()
        # v = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
        # luminance = v[:, :, 2].flatten()
        g = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        luminance = g.flatten()
        for i in self.__selected_superpixels:
            superpixel_mean_intensity = np.mean(luminance[self.__pixel_locations[i]])
            superpixel_median_intensity = np.median(luminance[self.__pixel_locations[i]])
            self.__add_mean_median(i, superpixel_mean_intensity, superpixel_median_intensity)
        self.__current_frame += 1

    def __add_mean_median(self, i, superpixel_mean_intensity, superpixel_median_intensity):
        self.__superpixel_regions_mean[(i, self.__current_frame)] = superpixel_mean_intensity
        self.__superpixel_regions_median[(i, self.__current_frame)] = superpixel_median_intensity

    def __calc_lab(self, image_lab):
        luminance = image_lab[:, :, 0].flatten()
        return self.__calc(luminance)

    def __calc_hsv(self, image_hsv):
        value_channel = image_hsv[:, :, 2].flatten()
        return self.__calc(value_channel)

    def get_median_per_superpixel(self, superpixel_indices):
        return self.__superpixel_regions_median[np.add(np.where(superpixel_indices), 1)[0]]

    def get_mean_per_superpixel(self, superpixel_indices):
        return self.__superpixel_regions_mean[np.add(np.where(superpixel_indices), 1)[0]]
