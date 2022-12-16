import matplotlib.pyplot as plt
import numpy as np
from ENFMeanAnalyzer import ENFMeanAnalyzer
from persistence.Persistence import Persistence
from base_functions import *
from persistence.DatasetEnfMean import DatasetEnfMeanPersistence, init_DatasetEnfMean_without_ids
from persistence.DatasetVideoMean import DatasetVideoMeanPersistence

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)


def load_mean_per_frame(filename, start=None, end=None):
    data = np.load(filename)
    if end is not None and end < data.shape[1]:
        data = data[:, :end]
    if start is not None and start < data.shape[1]:
        data = data[:, start:]
    return data


def process(ds_video_mean_id, csv_file=None, show_plots=False, dry_run=False, skip_seconds=0,
            correlate_only_recording_time=False, comment="", nfft=8192, samples=256):
    logger.info(f'processing ds_video_mean_id: {ds_video_mean_id}')
    persistence = Persistence(dry_run=dry_run)
    dvmp = DatasetVideoMeanPersistence(persistence.get_connection(), dry_run=dry_run)
    dvm = dvmp.find_video_mean_by_id(ds_video_mean_id)
    if csv_file is None:
        csv_file = f"{dvm.video.filename[1:11]}.csv"

    if Path(f'{get_enf_truth_path()}/{csv_file}').is_file():
        demp = DatasetEnfMeanPersistence(persistence.get_connection(), dry_run=dry_run)
        dem = demp.create_entry(dvm)
        dem.csv = csv_file
        dem.bandpass_order = 8
        dem.bandpass_width = .2

        filename = dvm.video.filename
        destination = get_destination_path(dvm.id, DESTINATION_MEAN)
        file_to_process = f'{destination}/{filename}_mean_per_frame.npy'

        mean_per_frame = load_mean_per_frame(file_to_process)
        ea = ENFMeanAnalyzer(destination=destination, show_plots=show_plots,
                             filename_prefix=dem.id, save_data=not dry_run,
                             expected_enf_frequency_in_hz=dvm.video.expected_enf_frequency,
                             bandpass_order=dem.bandpass_order, bandpass_width=dem.bandpass_width,
                             fps_real=dvm.video.fps_real, samples_stft=samples, nfft=nfft)
        demp.save(dem)
        enf_max_freq, enf_weighted = ea.extract_enf_mean_per_frame(mean_per_frame)

        enf_truth = read_csv(f'{get_enf_truth_path()}/{dem.csv}')

        if correlate_only_recording_time:
            enf_truth = slice_csv_data(enf_truth, dvm.video.get_video_timestamp(), enf_max_freq.size)
        enf_result = ea.correlate(enf_max_freq[skip_seconds:], enf_truth, "max frequency")
        # enf_result = ea.correlate(enf, data['wien'])
        logger.info(enf_result)
        if show_plots:
            enf_result.show_plot(margin_in_sec=10, title="max frequency")
            enf_result.show_plot_total(title="max frequency")

        dem.max_correlation = enf_result.max_correlation
        matched_timestamp = convert_panda_timestamp_to_timestamp(enf_result.get_timestamp())
        dem.matching_diff = get_timestamp_diff(dvm.video.get_video_timestamp(skip_seconds), matched_timestamp)
        logger.info(f"time difference matching vs video: {dem.matching_diff} s")

        dem.comment = f'{comment} ' if len(comment) > 0 else ""
        dem.comment += f"max_freq"
        if skip_seconds > 0:
            dem.comment = f'{dem.comment}, {skip_seconds}'
        ea.save_numpy_data(enf_max_freq, "max_freq_enf", dem.id)
        demp.save(dem)

        ds_enf_weighted = demp.create_entry(dvm)
        init_DatasetEnfMean_without_ids(dem, ds_enf_weighted)
        ea.set_filename_prefix(ds_enf_weighted.id)
        enf_result = ea.correlate(enf_weighted[skip_seconds:], enf_truth, "weighted")
        logger.info(f'weighted: {enf_result}')

        matched_timestamp = convert_panda_timestamp_to_timestamp(enf_result.get_timestamp())
        ds_enf_weighted.matching_diff = get_timestamp_diff(dvm.video.get_video_timestamp(skip_seconds),
                                                           matched_timestamp)
        ds_enf_weighted.max_correlation = enf_result.max_correlation
        ds_enf_weighted.comment = f'{comment} ' if len(comment) > 0 else ""
        ds_enf_weighted.comment += f"weighted"
        if skip_seconds > 0:
            ds_enf_weighted.comment = f'{ds_enf_weighted.comment}, {skip_seconds}'
        ea.save_numpy_data(enf_weighted, "weighted_enf", ds_enf_weighted.id)
        demp.save(ds_enf_weighted)
        logger.info(f"time difference matching vs video: {ds_enf_weighted.matching_diff} s")
    else:
        logger.warning(f'aborting ds_video_sp_id: {ds_video_mean_id}. csv file does not exist: {csv_file}')


if __name__ == "__main__":
    plt.rcParams['figure.figsize'] = [9.8, 5.5]

    dry_run = False

    skip_seconds = 4
    show_plots = False
    csv_file = None  # None: automatic by video filename, or "2022-09-17.csv"
    correlate_only_recording_time = False
    samples_list = [256, 512, 1024]  # which sample sizes should be used

    ds_video_mean_ids = [1]  # ds_video_mean.id
    for samples in samples_list:
        comment = f"{samples}"
        for video_mean_id in ds_video_mean_ids:
            process(video_mean_id, csv_file, show_plots, dry_run, skip_seconds=skip_seconds, samples=samples,
                    correlate_only_recording_time=correlate_only_recording_time, comment=comment)
