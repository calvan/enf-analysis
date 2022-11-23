import argparse
from sys import version_info
import numpy as np
from matplotlib import pyplot as plt

from ENFAnalysisConfig import ENFAnalysisConfig
from ENFSuperpixelAnalyzer import ENFSuperpixelAnalyzer
from ENFSuperpixelVideoProcessor import ENFSuperpixelVideoProcessor
from VideoGrabber import VideoGrabber
from base_functions import *
from persistence.Video import Video


def parse_arguments() -> ENFAnalysisConfig:
    argparser = argparse.ArgumentParser()
    argparser.add_argument("video_file", help="video file name to analyse")
    argparser.add_argument("-sr", "--samples-representative-enf", default=512, type=int,
                           help="number of samples (window size) for STFT (method: representative ENF), default: 512")
    argparser.add_argument("-sm", "--samples-max-energy", default=1024,
                           help="number of samples (window size) for STFT (method: max energy), default: 1024",
                           type=int)
    argparser.add_argument("-dmd", "--disable-motion-detection", help="disable motion detection",
                           action="store_true")
    argparser.add_argument("-fvd", "--flush-video-data", help="don't use cached processed video data",
                           action="store_true")
    argparser.add_argument("-lt", "--lightness_threshold", type=int,
                           help="controls which lightness value 0..255 is required to be included in an analysis, default: median")
    argparser.add_argument("-bo", "--bandpass-order", type=int, default=8, help="default: 8")
    argparser.add_argument("-bw", "--bandpass-width", type=float, default=.2, help="default: 0.2")
    argparser.add_argument("-nf", "--network-frequency", type=int, default=50, choices=[50, 60],
                           help="network frequency in Hz, default 50")
    argparser.add_argument("-gt", "--ground-truth", help="csv file with enf ground truth")
    argparser.add_argument("-dp", "--disable-plots", help="disable plots and images", action="store_true")
    argparser.add_argument("-ss", "--skip-seconds",
                           help="skips number of seconds for timestamp correlation, default: 4", type=int, default=4)
    args = argparser.parse_args()
    config = ENFAnalysisConfig()
    config.video_file = args.video_file
    config.samples_representative_enf = args.samples_representative_enf
    config.samples_max_energy = args.samples_max_energy
    config.motion_detection = not args.disable_motion_detection
    config.use_video_data_cache = not args.flush_video_data
    config.lightness_threshold = args.lightness_threshold
    config.bandpass_order = args.bandpass_order
    config.bandpass_width = args.bandpass_width
    config.network_frequency = args.network_frequency
    config.ground_truth = args.ground_truth
    config.disable_plots = args.disable_plots
    config.skip_seconds = args.skip_seconds
    if config.ground_truth is not None and not Path(config.ground_truth).is_file():
        logger.error(f"couldn't access ground truth csv file: {config.ground_truth}")
        exit()
    return config


def process_video(video_file: str, config: ENFAnalysisConfig):
    video_processor = ENFSuperpixelVideoProcessor(motion_detection=config.motion_detection, data_dir='.',
                                                  show_final_image=not config.disable_plots,
                                                  lightness_threshold=config.lightness_threshold, save_img=False)
    video = Video()
    config.video = video
    video.filename = Path(video_file).name
    video.motion = config.motion_detection
    vg = VideoGrabber(videofile=video_file, start_frame_nr=0, end_frame=1, video=video, silent=True)
    vg.first_frame(callback=lambda nr, frame: None)
    if config.use_video_data_cache and Path(video_processor.get_mean_data_filename(Path(video_file).name)).is_file():
        mean_per_superpixel = np.load(video_processor.get_mean_data_filename(video.filename))
    else:
        mean_per_superpixel = video_processor.process_video(video_file)
    video.expected_enf_frequency = calc_alias(config.network_frequency, video.fps_real)
    logger.info(f'Calculated alias frequency for ENF at {video.expected_enf_frequency} Hz')
    if video.expected_enf_frequency <= 0:
        logger.warning(f"ENF analysis not possible. Alias frequency is at {video.expected_enf_frequency} Hz")
        exit()
    return mean_per_superpixel


def process_enf_analysis(mean_per_superpixel, config: ENFAnalysisConfig):
    if config.video.duration < 420:
        logger.warning(f'Video duration is < 420 s. Matched timestamp might be wrong.')
    enf_analyzer = ENFSuperpixelAnalyzer(fps=config.video.fps, fps_real=config.video.fps_real,
                                         expected_enf_frequency_in_hz=config.video.expected_enf_frequency,
                                         show_plots=not config.disable_plots, bandpass_order=config.bandpass_order,
                                         bandpass_width=config.bandpass_width, destination='.',
                                         samples_stft=config.samples_representative_enf, save_data=False,
                                         samples_stft_me=config.samples_max_energy)
    enf_metric, representative_enf = enf_analyzer.detect_enf(mean_per_superpixel)
    max_energy_enf, weighted_energy_enf = enf_analyzer.extract_enf(mean_per_superpixel)
    logger.info(f'ENF metrics: {enf_metric}')
    if enf_metric.median < .6:
        logger.warning(f'ENF metric: median is < 0.6, there are probably no ENF traces.')

    if config.ground_truth is not None:
        ground_truth = read_csv(config.ground_truth)
        logger.info("correlate representative ENF")
        enf_result_representative_enf = enf_analyzer.correlate(representative_enf[config.skip_seconds:], ground_truth,
                                                               "representative ENF")
        logger.info("correlate max. energy ENF")
        enf_result_max_energy = enf_analyzer.correlate(max_energy_enf[config.skip_seconds:], ground_truth,
                                                       "max. energy")

        logger.info(f'representative ENF: {enf_result_representative_enf}')
        logger.info(f'max. energy ENF: {enf_result_max_energy}')
        if not config.disable_plots:
            enf_result_representative_enf.show_plot(margin_in_sec=10, title="representative ENF", block=False)
            enf_result_max_energy.show_plot(margin_in_sec=10, title="max. energy ENF")


if __name__ == "__main__":
    if version_info < (3, 9):
        logger.error("Minimum required Python version is 3.9")
        exit()
    plt.rcParams['figure.figsize'] = [9.8, 5.5]
    config = parse_arguments()
    video_file = config.video_file
    if not Path(video_file).is_file():
        logger.error(f"couldn't access video file: {video_file}")
        exit()
    mean_per_superpixel = process_video(video_file, config)
    process_enf_analysis(mean_per_superpixel, config)
