import matplotlib.pyplot as plt
import numpy as np
from ENFSuperpixelAnalyzer import ENFSuperpixelAnalyzer
from persistence.Persistence import Persistence
from base_functions import *
from persistence.DatasetEnfSuperpixel import DatasetEnfSuperpixel, DatasetEnfSuperpixelPersistence, \
    init_DatasetEnfSuperpixel_without_ids
from persistence.DatasetVideoSuperpixel import DatasetVideoSuperpixelPersistence, DatasetVideoSuperpixel

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)


def load_mean_per_spx(filename, start=None, end=None):
    data = np.load(filename)
    if end is not None and end < data.shape[1]:
        data = data[:, :end]
    if start is not None and start < data.shape[1]:
        data = data[:, start:]
    return data


def process(ds_video_sp_id, csv_file=None, show_plots=False, dry_run=False, skip_seconds=0,
            correlate_only_recording_time=False, nfft=8192, samples=256, comment=""):
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.DEBUG)
    logger.info(f'processing ds_video_sp_id: {ds_video_sp_id}, samples: {samples}')
    persistence = Persistence(dry_run=dry_run)
    dvspp = DatasetVideoSuperpixelPersistence(persistence.get_connection(), dry_run=dry_run)
    dvsp = dvspp.find_video_superpixel_by_id(ds_video_sp_id)
    if csv_file is None:
        csv_file = f"{dvsp.video.filename[1:11]}.csv"

    if Path(f'{get_enf_truth_path()}/{csv_file}').is_file():
        desp = DatasetEnfSuperpixelPersistence(persistence.get_connection(), dry_run=dry_run)
        des = desp.create_entry(dvsp)
        des.csv = csv_file
        des.bandpass_order = 8
        des.bandpass_width = .2

        filename = dvsp.video.filename
        destination = get_destination_path(dvsp.id, DESTINATION_SUPERPIXEL)
        file_to_process = f'{destination}/{filename}_mean_per_spx.npy'

        mean_per_superpixel = load_mean_per_spx(file_to_process)
        if mean_per_superpixel.shape[0] == 0:
            logger.warning("no data present: mean_per_superpixel is empty")
            return None
        ea = ENFSuperpixelAnalyzer(destination=destination, show_plots=show_plots,
                                   filename_prefix=des.id, save_data=not dry_run,
                                   expected_enf_frequency_in_hz=dvsp.video.expected_enf_frequency,
                                   bandpass_order=des.bandpass_order, bandpass_width=des.bandpass_width,
                                   fps_real=dvsp.video.fps_real, samples_stft=samples, nfft=nfft)
        enf_metrics, representative_enf = ea.detect_enf(mean_per_superpixel)
        des.metrics = enf_metrics
        logger.info(f'potential enf stats: {enf_metrics}')
        if des.metrics.median < .6:
            des.comment = samples
            desp.save(des)
            logger.warning(f'there are probably no ENF traces - median: {des.metrics.median}')
        else:
            enf_max_freq, enf_weighted = ea.extract_enf(mean_per_superpixel)
            desp.save(des)

            enf_truth = read_csv(f'{get_enf_truth_path()}/{des.csv}')
            if correlate_only_recording_time:
                enf_truth = slice_csv_data(enf_truth, dvsp.video.get_video_timestamp(), representative_enf.size)

            if show_plots:
                plt.figure()
                plt.plot(representative_enf[skip_seconds:], label="representative per superpixel")
                plt.plot(enf_weighted[skip_seconds:], label="enf weighted per frame")
                plt.legend()
                plt.show()

            enf_result = ea.correlate(representative_enf[skip_seconds:], enf_truth, "representative")
            logger.info(enf_result)
            if show_plots:
                enf_result.show_plot(margin_in_sec=10, title="representative")
                enf_result.show_plot_total(title="representative")

            des.max_correlation = enf_result.max_correlation
            matched_timestamp = convert_panda_timestamp_to_timestamp(enf_result.get_timestamp())
            des.matching_diff = get_timestamp_diff(dvsp.video.get_video_timestamp(skip_seconds), matched_timestamp)
            logger.info(f"time difference matching vs video: {des.matching_diff} s")

            des.comment = f'{comment} ' if len(comment) > 0 else ""
            des.comment += "representative"
            if skip_seconds > 0:
                des.comment = f'{des.comment}, {skip_seconds}'
            ea.save_numpy_data(representative_enf, "representative_enf", des.id)
            desp.save(des)

            ds_enf_weighted = desp.create_entry(dvsp)
            init_DatasetEnfSuperpixel_without_ids(des, ds_enf_weighted)
            ea.set_filename_prefix(ds_enf_weighted.id)
            enf_result = ea.correlate(enf_weighted[skip_seconds:], enf_truth, "weighted")
            logger.info(f'weighted: {enf_result}')

            matched_timestamp = convert_panda_timestamp_to_timestamp(enf_result.get_timestamp())
            ds_enf_weighted.matching_diff = get_timestamp_diff(dvsp.video.get_video_timestamp(skip_seconds),
                                                               matched_timestamp)
            ds_enf_weighted.max_correlation = enf_result.max_correlation
            ds_enf_weighted.comment = f'{comment} ' if len(comment) > 0 else ""
            ds_enf_weighted.comment += "weighted"
            if skip_seconds > 0:
                ds_enf_weighted.comment = f'{ds_enf_weighted.comment}, {skip_seconds}'
            ea.save_numpy_data(enf_weighted, "weighted_enf", ds_enf_weighted.id)
            desp.save(ds_enf_weighted)
            logger.info(f"time difference matching vs video: {ds_enf_weighted.matching_diff} s")

            ds_enf_max_amplitude = desp.create_entry(dvsp)
            init_DatasetEnfSuperpixel_without_ids(des, ds_enf_max_amplitude)
            ea.set_filename_prefix(ds_enf_max_amplitude.id)
            enf_result = ea.correlate(enf_max_freq[skip_seconds:], enf_truth, "max amplitude")
            logger.info(f'max amplitude: {enf_result}')

            matched_timestamp = convert_panda_timestamp_to_timestamp(enf_result.get_timestamp())
            ds_enf_max_amplitude.matching_diff = get_timestamp_diff(dvsp.video.get_video_timestamp(skip_seconds),
                                                               matched_timestamp)
            ds_enf_max_amplitude.max_correlation = enf_result.max_correlation
            ds_enf_max_amplitude.comment = f'{comment} ' if len(comment) > 0 else ""
            ds_enf_max_amplitude.comment += "max_amplitude"
            if skip_seconds > 0:
                ds_enf_max_amplitude.comment = f'{ds_enf_max_amplitude.comment}, {skip_seconds}'
            ea.save_numpy_data(enf_max_freq, "max_amplitude_enf", ds_enf_max_amplitude.id)
            desp.save(ds_enf_max_amplitude)
            logger.info(f"time difference matching vs video: {ds_enf_max_amplitude.matching_diff} s")
    else:
        logger.warning(f'aborting ds_video_sp_id: {ds_video_sp_id}. csv file does not exist: {csv_file}')


if __name__ == "__main__":
    plt.rcParams['figure.figsize'] = [9.8, 5.5]
    skip_seconds = 4
    show_plots = False
    dry_run = False
    correlate_only_recording_time = False
    csv_file = None  # None = date from filename # "2022-08-19.csv"
    ds_video_sp_ids = [1]
    samples_list = [256, 512, 1024]
    for video_sp_id in ds_video_sp_ids:
        for samples in samples_list:
            comment = f"{samples}"

            process(video_sp_id, csv_file, show_plots, dry_run, skip_seconds=skip_seconds, samples=samples,
                    correlate_only_recording_time=correlate_only_recording_time, comment=comment)
