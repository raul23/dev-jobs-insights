# Third-party modules
import numpy as np
# Own modules
from .analyzer import Analyzer


class JobSalariesAnalyzer(Analyzer):
    def __init__(self, conn, config):
        # Job salaries stats to compute
        self.stats_names = ["sorted_job_salaries_count"]
        super().__init__(conn, config, self.stats_names)

    def run_analysis(self):
        pass

    def generate_graphs(self):
        pass
