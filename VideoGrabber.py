from pathlib import Path

import cv2

from base_functions import *
from persistence.Video import Video

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)


class VideoGrabber:

    # TODO: get video data, not by reference
    def __init__(self, videofile, start_frame_nr=0, end_frame=None, video: Video = None):
        self.__fps = 0
        self.__frames_to_process = 0
        self.__videofile = videofile
        self.__start_frame_nr = start_frame_nr
        self.__cap: cv2.VideoCapture = cv2.VideoCapture(self.__videofile)
        self.__fps = round(self.__cap.get(cv2.CAP_PROP_FPS), 2)
        self.__total_frames = int(self.__cap.get(cv2.CAP_PROP_FRAME_COUNT))
        self.__frames_to_process = int(self.__cap.get(cv2.CAP_PROP_FRAME_COUNT)) - start_frame_nr
        if end_frame:
            self.__frames_to_process = end_frame - start_frame_nr
        self.__cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame_nr)
        self.__width = int(self.__cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        self.__height = int(self.__cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        if video is not None:
            video.fps_real = self.__fps
            video.fps = round(self.__fps)
            video.duration = round(self.duration())
        logger.debug(f'videofile: {videofile}')
        logger.debug(f'fps_real: {self.__fps}')
        logger.debug(f'fps: {round(self.__fps)}')
        logger.debug(f'frames to process: {self.__frames_to_process}')
        logger.debug(f'duration: {self.duration()}')
        logger.debug(f'total frames: {self.__total_frames}')
        logger.debug(f'total duration: {self.total_duration()}')

    def videofile(self):
        return Path(self.__videofile).name

    def get_width(self):
        return self.__width

    def get_height(self):
        return self.__height

    def get_start_frame_nr(self):
        return self.__start_frame_nr

    def total_frames(self):
        return self.__frames_to_process

    def duration(self):
        if self.__frames_to_process <= 0:
            logger.warning(f'__frames_to_process <= 0: {self.__frames_to_process}')
            return None
        return self.__frames_to_process / self.__fps

    def total_duration(self):
        if self.__total_frames <= 0:
            logger.warning(f'__total_frames <= 0: {self.__total_frames}')
            return None
        return self.__total_frames / self.__fps

    def get_fps(self):
        return self.__fps

    def first_frame(self, callback):
        ret, frame = self.__cap.read()
        callback(1, frame)

    def grab(self, callback):
        for frame_nr in range(2, (self.__frames_to_process + 1)):
            ret, frame = self.__cap.read()
            if not ret:
                logger.warning(f"Can't read frame {frame_nr} of {self.__frames_to_process}")
            else:
                callback(frame_nr, frame)
        self.__cap.release()
