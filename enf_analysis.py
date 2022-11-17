import argparse
from pathlib import Path
import numpy as np
from matplotlib import pyplot as plt

from ENFSuperpixelAnalyzer import ENFSuperpixelAnalyzer
from ENFSuperpixelVideoProcessor import ENFSuperpixelVideoProcessor
from VideoGrabber import VideoGrabber
from base_functions import *
from persistence.DatasetVideoSuperpixel import DatasetVideoSuperpixel
from persistence.Video import Video


def parse_arguments():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("video_file", help="video file name to analyse")
    argparser.add_argument("-sr", "--samples-representative-enf", default=512, type=int,
                           help="number of samples (window size) for STFT (method: representative ENF), default 512")
    argparser.add_argument("-sm", "--samples-max-energy", default=1024,
                           help="number of samples (window size) for STFT (method: max energy), default 1024", type=int)
    argparser.add_argument("-dmd", "--disable-motion-detection", help="disable motion detection",
                           action="store_true")
    argparser.add_argument("-fvd", "--flush-video-data", help="don't use cached processed video data",
                           action="store_true")
    argparser.add_argument("-fed", "--flush-enf-data", help="don't use cached enf data",
                           action="store_true")
    argparser.add_argument("-lt", "--lightness_threshold", type=int,
                           help="controls which lightness value 0..255 is required to be included in an analysis, default: median / 2")
    argparser.add_argument("-bo", "--bandpass-order", type=int, default=8, help="default: 8")
    argparser.add_argument("-bw", "--bandpass-width", type=float, default=.2, help="default: 0.2")
    argparser.add_argument("-nf", "--network-frequency", type=int, default=50, choices=[50, 60],
                           help="network frequency in Hz, default 50")
    argparser.add_argument("-gt", "--ground-truth", help="csv file with enf ground truth")
    argparser.add_argument("-dp", "--disable-plots", help="disable plots and images")
    return argparser.parse_args()


def process_video(video_file: str, video: Video, motion_detection: bool,
                  lightness_threshold: int, disable_plots: bool, use_video_data_cache: bool):
    video_processor = ENFSuperpixelVideoProcessor(motion_detection=motion_detection, data_dir='.',
                                                  show_final_image=not disable_plots,
                                                  lightness_threshold=lightness_threshold, save_img=False)
    if use_video_data_cache and Path(video_processor.get_mean_data_filename(Path(video_file).name)).is_file():
        vg = VideoGrabber(videofile=video_file, start_frame_nr=0, end_frame=1)
        video.duration = round(vg.total_duration())
        video.fps = round(vg.get_fps())
        video.fps_real = vg.get_fps()
        video.motion = motion_detection
        video.expected_enf_frequency = calc_alias(network_frequency, video.fps_real)
        video.filename = filename
        video.hint = hint
        self.__vg.first_frame(callback=self.__process_first_frame)
        return np.load(video_processor.get_mean_data_filename(Path(video_file).name))
    else:
        return video_processor.process_video(video_file)


def process_enf_analysis():
    enf_analyzer = ENFSuperpixelAnalyzer()


if __name__ == "__main__":
    plt.rcParams['figure.figsize'] = [9.8, 5.5]
    args = parse_arguments()
    video_file = args.video_file
    if not Path(video_file).is_file():
        logger.warning(f"couldn't access viedo file: {video_file}")
        exit()
    samples_representative_enf = args.samples_representative_enf
    samples_max_energy = args.samples_max_energy
    motion_detection = not args.disable_motion_detection
    use_video_data_cache = not args.flush_video_data
    use_enf_data_cache = not args.flush_enf_data
    lightness_threshold = args.lightness_threshold
    bandpass_order = args.bandpass_order
    bandpass_width = args.bandpass_width
    network_frequency = args.network_frequency
    ground_truth = args.ground_truth
    disable_plots = args.disable_plots
    if ground_truth is not None and not Path(ground_truth).is_file():
        logger.warning(f"couldn't access ground truth csv file: {ground_truth}")
        exit()
    dataset_video = DatasetVideoSuperpixel()
    mean_per_superpixel = process_video(video_file, dataset_video, motion_detection, lightness_threshold, disable_plots,
                                        use_video_data_cache)
    process_enf_analysis()
