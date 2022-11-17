import os
import pandas as pd
from datetime import datetime
from sys import platform
import logging

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(module)s: %(message)s', level=logging.INFO)
LOGGER_LEVEL = logging.DEBUG
logger = logging.getLogger(__file__)
logger.setLevel(LOGGER_LEVEL)

DESTINATION_MEAN = "mean"
DESTINATION_MEAN_SHUTTER = "mean_shutter"
DESTINATION_SUPERPIXEL = "superpixel"
DESTINATION_COMPRESSION = "compression"


def get_base_path() -> str:
    if platform == "linux" or platform == "linux2":
        base_path = "/media/internet/ext_1TB/thesis/storage"
    elif platform == "win32":
        base_path = "B:/thesis/storage"
    else:
        base_path = "/Volumes/ext_1TB/thesis/storage"
    return base_path


def calc_alias(freq_power, freq_sample):
    freq_light = freq_power * 2
    if freq_sample / 2 >= freq_light:
        return freq_light
    k = 1
    while abs(freq_light - k * freq_sample) >= freq_sample / 2:
        k += 1
    return round(abs(freq_light - k * freq_sample), 2)


def get_input_path() -> str:
    return f'{get_base_path()}/input'


def get_destination_path(filename, directory=None):
    if directory is not None:
        return f'{get_base_path()}/extracted/{directory}/{filename}'
    else:
        return f'{get_base_path()}/extracted/{filename}'


def convert_panda_timestamp_to_timestamp(timestamp):
    return datetime.strptime(str(timestamp), "%Y-%m-%d %H:%M:%S")


def get_timestamp_diff(video_ts, matched_ts):
    return round(matched_ts.timestamp() - video_ts.timestamp())


def get_enf_truth_path():
    return f'{get_base_path()}/enf_data'


def read_csv(csv_file) -> pd.Series:
    logger.debug(f'csv: loading {csv_file}')
    if "GDE" in csv_file:
        recorded_enf = pd.read_csv(csv_file, parse_dates=['time'], skiprows=0, sep=',', decimal='.')
        return recorded_enf.set_index('time').squeeze()
    # else:
    #     return read_csv_netztransparenz(csv_file)


#
#
# def read_csv_netztransparenz(csv_file) -> pd.Series:
#     recorded_enf = pd.read_csv(csv_file,
#                                usecols=[0, 1, 4], parse_dates=[['Datum', 'von']], skiprows=0, sep=';', decimal=',')
#
#     recorded_enf.rename(columns={'Datum_von': 'time'}, inplace=True)
#     return recorded_enf.set_index('time').squeeze()
#
#
# def slice_csv_data(csv_data, timestamp, enf_length, offset=30):
#     start = csv_data.index.get_loc(timestamp)
#     end = start + enf_length + offset
#     start -= offset
#     return csv_data[start:end]


def create_directories(destination_dir):
    os.makedirs(destination_dir, exist_ok=True)
