import numpy as np

from .abstract_analyzer import AbstractAnalyzer
from utility import graph_util as g_util


class IndustryAnalyzer(AbstractAnalyzer):
    def __init__(self, conn, config_ini):
        super().__init__(conn, config_ini)
        # Industries stats to compute
        self.stats_names = ["sorted_industries_count"]
        self.stats = {"sorted_industries_count": None}

    def reset_stats(self):
        self.stats = {"sorted_industries_count": None}

    def run_analysis(self):
        # Reset all industry stats to be computed
        self.reset_stats()
        # Get number of job posts for each industry
        # TODO: specify that the results are already sorted in decreasing order of industry's count, i.e.
        # from the most popular industry to the least one
        results = self.count_industry_occurrences()
        # TODO: Process the results by summing the similar industries (e.g. Software Development with
        # Software Development / Engineering or eCommerce with E-Commerce)
        # TODO: use Software Development instead of the longer Software Development / Engineering
        self.stats["sorted_industries_count"] = np.array(results)
        self.generate_graphs()

    def count_industry_occurrences(self):
        """
        Returns industries sorted in decreasing order of their occurrences in job posts.
        A list of tuples is returned where a tuple is of the form (industry, count).

        :return: list of tuples of the form (industry, count)
        """
        sql = '''SELECT value, COUNT(*) as CountOf from job_overview WHERE name='Industry' GROUP BY value ORDER BY CountOf DESC'''
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def generate_graphs(self):
        # Generate bar chart: industries vs number of job posts
        top_k = self.config_ini["bar_chart_industries"]["top_k"]
        config = {"x": self.stats["sorted_industries_count"][:top_k, 0],
                  "y": self.stats["sorted_industries_count"][:top_k, 1].astype(np.int32),
                  "xlabel": self.config_ini["bar_chart_industries"]["xlabel"],
                  "ylabel": self.config_ini["bar_chart_industries"]["ylabel"],
                  "title": self.config_ini["bar_chart_industries"]["title"],
                  "grid_which": self.config_ini["bar_chart_industries"]["grid_which"]}
        # TODO: place number (of job posts) on top of each bar
        g_util.generate_bar_chart(config)
