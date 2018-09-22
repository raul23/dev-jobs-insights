# Third-party modules
import numpy as np
# Own modules
from .analyzer import Analyzer


class JobBenefitsAnalyzer(Analyzer):
    def __init__(self, conn, db_session, config):
        # Job benefits stats to compute
        self.stats_names = ["sorted_job_benefits_count"]
        super().__init__(conn, db_session, config, self.stats_names)

    def run_analysis(self):
        pass

    def generate_graphs(self):
        pass
