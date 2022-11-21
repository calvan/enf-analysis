from concurrent.futures import ThreadPoolExecutor, wait
from typing import Tuple

from scipy.fft import rfft, rfftfreq
from scipy.signal import stft, windows, butter, sosfilt
import matplotlib.pyplot as plt
import numpy as np

from ENFExtractionResult import ENFExtractionResult
from base_functions import *
from ENFMetric import ENFMetric, ENFMetricResult

logger = logging.getLogger(__file__)
logger.setLevel(LOGGER_LEVEL)


class ENFSuperpixelAnalyzer:

    def __init__(self, destination, fps=30, expected_enf_frequency_in_hz=10., show_plots=True, filename_prefix=None,
                 bandpass_order=6, bandpass_width=.3, fps_real=30., save_data=True, samples_stft=512,
                 samples_stft_me=1024, nfft=8192):
        self.__save_data = save_data
        self.__fps_real = fps_real
        self.__filename_prefix = filename_prefix
        self.__expected_enf_frequency_in_hz = expected_enf_frequency_in_hz
        self.__fps = fps
        self.__show_plots = show_plots
        self.__overlap = .5
        self.__destination = destination
        self.__nfft = nfft
        self.__samples_stft = samples_stft
        self.__samples_stft_me = samples_stft_me
        self.__bandpass_order = bandpass_order
        self.__bandpass_width = bandpass_width

    def __fft(self, data, title):
        if self.__save_data or self.__show_plots:
            freq = rfftfreq(data.size, d=(1.0 / self.__fps_real))
            fft = rfft(data)
            plt.figure("FFT")
            plt.title(title)
            plt.plot(freq, np.abs(fft))
            self.__save_plot("fft")

    def __enf_multi_plot(self, enf_plot_data, label):
        if self.__save_data or self.__show_plots:
            ax = plt.figure()
            plt.title("ENF: max energy vs. weighted energy")
            plt.plot(enf_plot_data[0], label=label[0])
            plt.plot(enf_plot_data[1], label=label[1])
            ax.legend()
            self.__save_plot("enf")

    def __enf_plot(self, enf_plot_data, label):
        if self.__save_data or self.__show_plots:
            ax = plt.figure(label)
            plt.title("ENF")
            plt.plot(enf_plot_data, label=label)
            ax.legend()
            self.__save_plot("enf")

    def __save_plot(self, suffix: str):
        if self.__filename_prefix is not None and self.__save_data:
            plt.savefig(self.__get_filename(suffix))

    def save_numpy_data(self, numpy_data, name, prefix=None):
        if self.__save_data:
            np.save(self.__get_filename(name, extension='npy', filename_prefix=prefix), numpy_data)

    def read_numpy_data(self, infix, ds_enf_id: int):
        if self.__filename_prefix is not None:
            return np.load(self.__get_filename(infix, extension='npy', filename_prefix=ds_enf_id))
        else:
            logger.warning("filename prefix is None!")
            exit()

    def __get_filename(self, suffix, extension='png', filename_prefix=None):
        if filename_prefix is not None:
            return f'{self.__destination}/{filename_prefix}_{suffix}.{extension}'
        else:
            return f'{self.__destination}/{self.__filename_prefix}_{suffix}.{extension}'

    def detect_enf(self, mean_per_superpixel) -> Tuple[ENFMetricResult, np.ndarray]:
        if mean_per_superpixel.shape[0] == 0:
            logger.warning("no data present: mean_per_superpixel is empty")
            return None
        logger.debug("calculating potential enf stats")

        superpixel_filtered = self.__filter_superpixel(mean_per_superpixel)
        if superpixel_filtered.shape[0] < 4:
            logger.warning("not enough superpixel to compute enf statistics")
            return
        enf_candidates = self.__enf_with_stft_superpixel(superpixel_filtered, self.__samples_stft)
        representative = np.mean(enf_candidates, axis=0)
        enf_metric = ENFMetric.calc_metric(enf_candidates)
        self.__enf_plot(representative, "repräsentative ENF")
        plt.show(block=False) if self.__show_plots else plt.close()
        return enf_metric, representative

    def __filter_superpixel(self, mean_per_superpixel):
        means_mean = np.mean(mean_per_superpixel, axis=1)
        mean_per_superpixel_transposed = np.transpose(mean_per_superpixel)
        enf_snr = np.transpose(mean_per_superpixel_transposed - means_mean)
        var = np.mean(abs(enf_snr) ** 2, axis=1)
        # remove Superpixel without ENF information e.g. overexposed
        return enf_snr[np.where(var > .01)]

    def extract_enf(self, mean_per_superpixel, mode='mean') -> Tuple[np.ndarray, np.ndarray]:
        if mode == 'diff':
            diff = np.diff(mean_per_superpixel, axis=1)
            representative = np.mean(diff, axis=0)
        else:
            representative = np.mean(self.__filter_superpixel(mean_per_superpixel), axis=0)
        return self.extract_enf_mean_per_frame(representative)

    def extract_enf_mean_per_frame(self, mean_per_frame) -> Tuple[np.ndarray, np.ndarray]:
        sos = butter(self.__bandpass_order,
                     [(self.__expected_enf_frequency_in_hz - self.__bandpass_width) / (.5 * self.__fps_real),
                      (self.__expected_enf_frequency_in_hz + self.__bandpass_width) / (.5 * self.__fps_real)],
                     btype='bandpass', output='sos')
        mean_per_frame_filtered = sosfilt(sos, mean_per_frame)
        max_energy_stft, weighted_energy = self.__enf_with_stft(mean_per_frame_filtered, self.__samples_stft_me)
        self.__specgram("Spektrogramm", mean_per_frame_filtered)
        self.__fft(mean_per_frame_filtered, "FFT")
        plt.show(block=False) if self.__show_plots else plt.close()
        self.__enf_multi_plot((max_energy_stft, weighted_energy), label=("max_energy", "weighted_energy"))
        plt.show() if self.__show_plots else plt.close()
        return max_energy_stft, weighted_energy

    def __specgram(self, title: str, data):
        if self.__save_data or self.__show_plots:
            plt.figure("Spektrogramm")
            plt.title(title)
            hann = plt.mlab.window_hanning(np.ones(self.__samples_stft))
            plt.specgram(data, Fs=self.__fps, window=hann, NFFT=self.__samples_stft,
                         noverlap=self.__samples_stft - self.__fps, cmap=plt.colormaps['jet'])
            plt.ylabel('Frequenz [Hz]')
            plt.xlabel('Zeit [sec]')
            plt.ylim([9, 11])
            self.__save_plot("spectrogram")
            if not self.__show_plots:
                plt.close()

    def __correlation_plot(self, correlated, description=""):
        if self.__show_plots or self.__save_data:
            plt.figure(description)
            plt.plot(correlated)
            plt.title(f"correlation {description}")
            self.__save_plot("correlation")
            if self.__show_plots:
                plt.show(block=False)
            else:
                plt.close()

    def __enf_with_stft_superpixel(self, enf_superpixel_data, stft_samples=256):
        sos = butter(self.__bandpass_order,
                     [(self.__expected_enf_frequency_in_hz - self.__bandpass_width) / (.5 * self.__fps_real),
                      (self.__expected_enf_frequency_in_hz + self.__bandpass_width) / (.5 * self.__fps_real)],
                     btype='bandpass', output='sos')
        f_low = self.__expected_enf_frequency_in_hz - self.__bandpass_width
        f_high = self.__expected_enf_frequency_in_hz + self.__bandpass_width
        overlap_stft = stft_samples - self.__fps
        hann = windows.hann(stft_samples)
        data = sosfilt(sos, enf_superpixel_data[0])
        f1, t1, Zxx = stft(data, fs=1, nperseg=stft_samples, noverlap=overlap_stft, window=hann, nfft=self.__nfft)
        result = np.zeros(shape=(enf_superpixel_data.shape[0], t1.size))
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [
                executor.submit(self.__enf_with_stft_one_superpixel, sos, sp, enf_superpixel_data[sp], stft_samples,
                                overlap_stft, hann, f_low, f_high, result) for sp in
                range(enf_superpixel_data.shape[0])]
            wait(futures)
        return result

    def __enf_with_stft_one_superpixel(self, sos, sp_idx, superpixel_data, stft_samples, overlap_stft, window, f_low,
                                       f_high, result):
        data = sosfilt(sos, superpixel_data)
        f1, t1, Zxx = stft(data, fs=1, nperseg=stft_samples, noverlap=overlap_stft, window=window, nfft=self.__nfft)
        f1_real = f1 * self.__fps_real
        result[sp_idx] = f1_real[np.argmax(np.abs(Zxx), axis=0)]

    def __enf_with_stft(self, data, stft_samples=256):
        overlap_stft = stft_samples - self.__fps
        hann = windows.hann(stft_samples)
        f1, t1, Zxx = stft(data, fs=1, nperseg=stft_samples, noverlap=overlap_stft, window=hann,
                           nfft=self.__nfft)
        f1_real = f1 * self.__fps_real
        max_energy_stft = f1_real[np.argmax(np.abs(Zxx), axis=0)]
        f_low = self.__expected_enf_frequency_in_hz - self.__bandpass_width
        f_high = self.__expected_enf_frequency_in_hz + self.__bandpass_width
        freq_idx = np.where((f1_real > f_low) & (f1_real < f_high))
        freq = f1_real[freq_idx]
        energy = np.abs(Zxx)[freq_idx]
        weighted_energy_freq = np.sum(energy * freq[:, None], axis=0) / np.sum(energy, axis=0)
        return max_energy_stft, weighted_energy_freq

    def correlate(self, extracted_enf: np.ndarray, frequency_data: pd.Series, description="") -> ENFExtractionResult:
        data_frame = pd.DataFrame(frequency_data)
        enf_series = pd.Series(extracted_enf, name=ENFExtractionResult.dataframe_name)
        size = (data_frame.size - enf_series.size) + 1
        correlated = np.zeros(size)
        for i in range(0, size):
            if i % 10000 == 0:
                logger.debug(f'correlate: {i}/{size}')
            section = frequency_data.iloc[i:i + enf_series.size].reset_index(drop=True)
            correlated[i] = enf_series.corr(section)
        self.__correlation_plot(correlated, description)
        index_max_correlation = np.argmax(correlated)
        time_aligned_enf_data = pd.DataFrame(
            {enf_series.name: np.add(enf_series.array, 50 - self.__expected_enf_frequency_in_hz)},
            index=data_frame.index[
                  index_max_correlation:index_max_correlation + enf_series.size])
        data_frame[ENFExtractionResult.dataframe_name] = time_aligned_enf_data

        enf_result = ENFExtractionResult()
        enf_result.max_correlation = round(np.max(correlated), 4)
        enf_result.max_correlation_index = index_max_correlation
        enf_result.data_frame = data_frame
        if self.__filename_prefix is not None and self.__save_data:
            enf_result.save_plot(self.__get_filename("enf_corr"), margin_in_sec=20, description=description)
            enf_result.save_plot_total(self.__get_filename("enf_corr_total"), description=description)
        return enf_result

    def set_filename_prefix(self, prefix):
        self.__filename_prefix = prefix
