# Third-party modules
import numpy as np
# Own modules
from .analyzer import Analyzer


class SkillsAnalyzer(Analyzer):
    def __init__(self, conn, db_session, config):
        # Skills stats to compute
        self.stats_names = ["sorted_skills_count"]
        super().__init__(conn, db_session, config, self.stats_names)

    def run_analysis(self):
        pass

    def generate_graphs(self):
        pass
