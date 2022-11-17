import numpy as np
from base_functions import *

logger = logging.getLogger(__name__)
logger.setLevel(LOGGER_LEVEL)


class PearsonResult:
    max: float = -1.
    mean: float = -1.
    median: float = -1.
    top_two: float = -1.

    def __str__(self):
        return f'max: {self.max}, mean: {self.mean}, median: {self.median}, corr(top_two): {self.top_two}'


class Pearson:

    @staticmethod
    def pearson_per_superpixel(mean_intensity_per_superpixel_and_frame) -> PearsonResult:
        pearson_per_superpixel_result = Pearson.__calc_pearson_per_superpixel(mean_intensity_per_superpixel_and_frame)
        ranked_pearson_per_superpixel = {key: val for key, val in
                                         sorted(pearson_per_superpixel_result.items(), key=lambda ele: ele[0],
                                                reverse=True)}
        # TODO check if there are a reasonable number of superpixel to calc statistics!
        top_two = list(ranked_pearson_per_superpixel)[0:2]

        pearson_per_superpixel_result_list = list(pearson_per_superpixel_result.keys())
        p = PearsonResult()
        p.mean = Pearson.round(np.nanmean(pearson_per_superpixel_result_list))
        p.median = Pearson.round(np.nanmedian(pearson_per_superpixel_result_list))
        p.max = Pearson.round(np.nanmax(pearson_per_superpixel_result_list))
        p.top_two = Pearson.round(Pearson.pearson(pearson_per_superpixel_result[top_two[0]],
                                                  pearson_per_superpixel_result[top_two[1]]))
        return p

    @staticmethod
    def round(value):
        return round(value, 4)

    @staticmethod
    def __calc_pearson_per_superpixel(mean_intensity_per_superpixel_and_frame):
        representative = np.nanmean(mean_intensity_per_superpixel_and_frame, axis=0)
        pearson_result = {}
        for ek_n in mean_intensity_per_superpixel_and_frame:
            if np.std(ek_n, dtype='float32') != 0:
                pearson_result[np.corrcoef(ek_n, representative)[0][1]] = ek_n
                # pearson_result[Pearson.pearson(ek_n, representative)] = ek_n
        return pearson_result

    @staticmethod
    def pearson(i, j):
        return np.corrcoef(i, j)[0][1]