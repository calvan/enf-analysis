from base_functions import *
import queue
import time
import cv2
import numpy as np
from skimage.segmentation import mark_boundaries
import threading
import logging

from persistence.DatasetVideoSuperpixel import DatasetVideoSuperpixel

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)


class MotionDetectorGSOC:

    # TODO: remove DatasetVideoSuperpixel
    def __init__(self, show_background=True, show_motion_free_image=False, motion_threshold_factor=.3,
                 dataset_video: DatasetVideoSuperpixel = None):
        self.__dataset_video = dataset_video
        self.__region_size = 20
        self.__first_frame = None
        self.__segmented_superpixel = None
        self.__superpixels_indices = None
        self.__superpixel_size_denominator = 18  # 25
        self.__number_of_superpixels = 0
        self.__superpixels_indices_without_motion = None
        self.__total_pixel_per_superpixel = None
        self.__slic_ratio = .08  # barber: .08 # default 0.075f
        self.__queue = queue.Queue(maxsize=1)
        self.__stop = False
        self.__blur = (5, 5)
        self.__contour_mask = None
        self.__bgSubtractor = cv2.bgsegm.createBackgroundSubtractorGSOC(beta=.03, alpha=.05, replaceRate=.01)
        self.__show_background = show_background
        self.__show_motion_free_image = show_motion_free_image
        self.__motion_threshold_factor = motion_threshold_factor

    def apply_disabled_superpixel(self, disabled_superpixel_indices):
        if len(disabled_superpixel_indices) > 0:
            self.__superpixels_indices_without_motion[np.subtract(np.array(disabled_superpixel_indices), 1)] = False

    def segmented_superpixel(self):
        return self.__segmented_superpixel

    def superpixel_ids(self):
        return self.__superpixels_indices

    def first_frame(self, frame):
        self.__first_frame = frame
        self.__bgSubtractor.apply(cv2.GaussianBlur(frame, self.__blur, 0))
        self.__region_size = int(min(frame.shape[0], frame.shape[1]) / self.__superpixel_size_denominator)
        frame_lab = cv2.cvtColor(frame, cv2.COLOR_BGR2Lab)
        blurred_frame = cv2.GaussianBlur(frame_lab, self.__blur, 0)
        sp: cv2.ximgproc_SuperpixelLSC = cv2.ximgproc.createSuperpixelLSC(blurred_frame, region_size=self.__region_size,
                                                                          ratio=self.__slic_ratio)  # , ratio=.04
        sp.iterate(10)
        self.__number_of_superpixels = sp.getNumberOfSuperpixels()
        self.__contour_mask = sp.getLabelContourMask()
        logger.info(
            f'# superpixel: {self.__number_of_superpixels}, region-size: {self.__region_size}, motion_threshold: {self.__motion_threshold_factor}')
        if self.__dataset_video is not None:
            self.__dataset_video.superpixel = self.__number_of_superpixels
            self.__dataset_video.region_size = self.__region_size
            self.__dataset_video.motion_threshold = self.__motion_threshold_factor
        # add 1 to prevent zero based index
        self.__segmented_superpixel = np.add(sp.getLabels(), 1)
        self.__superpixels_indices = np.unique(self.__segmented_superpixel).tolist()
        self.__superpixels_indices_without_motion = np.ones(len(self.__superpixels_indices))
        self.__total_pixel_per_superpixel = np.bincount(self.__segmented_superpixel.flatten(),
                                                        minlength=self.__number_of_superpixels + 1)
        if self.__show_motion_free_image:
            threading.Thread(target=self.__calc_motionless_image).start()

    def next_frame(self, frame):
        motion_mask = self.__bgSubtractor.apply(cv2.GaussianBlur(frame, self.__blur, 0))
        if self.__show_background:
            cv2.imshow("bg", mark_boundaries(motion_mask, self.__segmented_superpixel))
            if cv2.waitKey(12) == ord('q'):
                self.__stop = True
                exit()
        if 255 in motion_mask:
            superpixel_motion_mask = np.logical_and(motion_mask, self.__segmented_superpixel)
            affected_superpixel = self.__segmented_superpixel[superpixel_motion_mask]
            affected_pixel = np.bincount(affected_superpixel, minlength=self.__number_of_superpixels + 1)
            affected_pixel_percent = np.divide(affected_pixel[1:], self.__total_pixel_per_superpixel[1:])
            superpixel_with_motion = np.where(affected_pixel_percent > self.__motion_threshold_factor)[0]
            self.__superpixels_indices_without_motion[superpixel_with_motion] = False

    def __calc_motionless_image(self):
        while True and not self.__stop:
            if self.__queue.empty():
                superpixel_indices_without_motion = np.add(np.where(self.__superpixels_indices_without_motion)[0], 1)
                uu = np.zeros(self.__first_frame.shape, dtype='uint8')
                for i in superpixel_indices_without_motion:
                    loc = np.where(self.__segmented_superpixel == i)
                    uu[loc] = self.__first_frame[loc]
                try:
                    self.__queue.put(uu, block=False)
                except queue.Full:
                    pass
            time.sleep(.3)

    def get_superpixel_indices(self):
        return self.__superpixels_indices

    def show_image(self, segmented_superpixel):
        uu = np.zeros(self.__first_frame.shape, dtype='uint8')
        indices = np.add(np.where(segmented_superpixel), 1)[0]
        for i in indices:
            loc = np.where(self.__segmented_superpixel == i)
            uu[loc] = self.__first_frame[loc]
        cv2.imshow('result', mark_boundaries(uu, self.__segmented_superpixel, color=(0, 0, 1)))

    def save_image(self, filename, segmented_superpixel=None):
        if segmented_superpixel is not None:
            uu = np.zeros(self.__first_frame.shape, dtype='uint8')
            indices = np.add(np.where(segmented_superpixel), 1)[0]
            for i in indices:
                loc = np.where(self.__segmented_superpixel == i)
                uu[loc] = self.__first_frame[loc]
            mask_inv = cv2.bitwise_not(self.__contour_mask)
            result_bg = cv2.bitwise_and(uu, uu, mask=mask_inv)
            sp_border = np.zeros(self.__first_frame.shape, np.uint8)
            sp_border[:] = (0, 0, 255)
            result_fg = cv2.bitwise_and(sp_border, sp_border, mask=self.__contour_mask)
            result = cv2.add(result_bg, result_fg)
            cv2.imwrite(filename, result)
        else:
            cv2.imwrite(filename, self.__first_frame)

    def show_motionless_image(self):
        if self.__show_motion_free_image:
            try:
                frame = self.__queue.get(False)
                cv2.imshow('motionless', mark_boundaries(frame, self.__segmented_superpixel, color=(0, 0, 1)))
                if cv2.waitKey(12) == ord('q'):
                    self.__stop = True
                    exit()
            except queue.Empty:
                pass

    def get_steady_superpixel_indices(self):
        return self.__superpixels_indices_without_motion

    def stop(self):
        self.__stop = True
