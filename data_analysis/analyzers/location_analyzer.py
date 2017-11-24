import os

from abstract_analyzer import AbstractAnalyzer
from utility import util, graph_util as g_util


class LocationAnalyzer(AbstractAnalyzer):
    def __init__(self, conn, config_ini):
        # Locations stats to compute
        self.stats_names = [
            "locations_info",
            "sorted_countries_count",
            "sorted_us_states_count"
        ]
        super().__init__(conn, config_ini, self.stats_names)
        self.shape_path = os.path.expanduser(self.config_ini["paths"]["shape_path"])
        # TODO: cached_locations_path should be called cached_map_locations_path because
        # it relates to map locations' coordinates
        self.cached_locations_path = self.config_ini["paths"]["cached_locations_path"]
        self.cached_locations = util.load_pickle(self.cached_locations_path)
        if self.cached_locations is None:
            self.cached_locations = {}
        self.wait_time = self.config_ini["geocoding"]["wait_time"]
        self.marker_scale = self.config_ini["basemap"]["marker_scale"]

    def reset_stats(self):
        self.salary_stats = dict(zip(self.salary_stats_names, [None] * len(self.salary_stats_names)))