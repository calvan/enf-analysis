from typing import Optional

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


class ENFExtractionResult:
    dataframe_name = "extracted_enf"

    def __init__(self):
        self.max_correlation = None
        self.max_correlation_index = None
        self.data_frame: Optional[pd.DataFrame] = None

    def get_timestamp(self):
        return self.data_frame.index[self.max_correlation_index]

    def show_plot(self, margin_in_sec=10, title=""):
        self.__prepare_plot(margin_in_sec, title=title)
        plt.show()

    def __prepare_plot(self, margin_in_sec=10, title=""):
        show_from_index = self.max_correlation_index - margin_in_sec
        show_to_index = self.max_correlation_index + np.count_nonzero(
            -np.isnan(self.data_frame[self.dataframe_name])) + margin_in_sec
        self.data_frame.iloc[show_from_index:show_to_index].plot.line(title=title)

    def save_plot(self, filename_plot, margin_in_sec=10, description=""):
        self.__prepare_plot(margin_in_sec, description)
        plt.savefig(filename_plot)
        plt.close()

    def save_plot_total(self, filename_plot, description=""):
        self.data_frame.plot.line()
        plt.title(description)
        plt.savefig(filename_plot)
        plt.close()

    def show_plot_total(self, title=""):
        self.data_frame.plot.line(title=title)
        plt.show()

    def __str__(self):
        return f'max_correlation: {self.max_correlation}, timestamp: {self.get_timestamp()}'
