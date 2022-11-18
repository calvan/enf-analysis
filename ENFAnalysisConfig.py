from typing import Optional

from persistence.Video import Video


class ENFAnalysisConfig:

    def __init__(self):
        self.video_file = None
        self.video = None
        self.samples_representative_enf = None
        self.samples_max_energy = None
        self.motion_detection = None
        self.use_video_data_cache = None
        self.use_enf_data_cache = None
        self.lightness_threshold = None
        self.bandpass_order = None
        self.bandpass_width = None
        self.network_frequency = None
        self.ground_truth = None
        self.disable_plots = None
