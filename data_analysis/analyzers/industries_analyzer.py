import os
import sys
# Third-party modules
import ipdb
import numpy as np
# Own modules
from .analyzer import Analyzer


class IndustriesAnalyzer(Analyzer):
    def __init__(self, conn, db_session, main_cfg, logging_cfg):
        # Industries stats to compute
        self.stats_names = ["sorted_industries_count"]
        super().__init__(conn,
                         db_session,
                         main_cfg,
                         logging_cfg,
                         self.stats_names,
                         __name__,
                         __file__,
                         os.getcwd())

    def run_analysis(self):
        # Reset all industries stats to be computed
        self.reset_stats()
        # Get number of job posts for each industry
        # NOTE: the returned results are sorted in decreasing order of
        # industries' count, i.e. from the most popular industry to the least
        # popular industry
        self._clean_industries_names()
        industries_count = self._count_industries()
        self.logger.debug(
            "There are {} distinct industries".format(len(industries_count)))
        self.logger.debug(
            "There are {} occurrences of industries in job posts".format(
                sum(j for i, j in industries_count)))
        self.stats["sorted_industries_count"] = industries_count
        bar_cfg = self.main_cfg["graphs_cfg"]["bar_chart_industries"]
        self._generate_bar_chart(
            sorted_topic_count=self.stats["sorted_industries_count"],
            bar_chart_cfg=bar_cfg)

    def _clean_industries_names(self):
        # Standardize the names of the industries
        # NOTE: only the most obvious industries names are standardized. The
        # other less obvious ones are left intact, e.g. 'IT Consulting' could
        # be renamed to 'Consulting' but 'Consulting' is a too broad category
        # and you might lose information doing so.
        # Same for 'Advertising Technology' and 'Advertising'.
        # NOTE: Typos are also fixed
        # NOTE: Some industries should not even be considered as industries
        # e.g. JavaScript, functional programming, facebook, iOS
        # 'and Compliance' seems to be an incomplete name for an industry
        industry_names = {
            'Software Development / Engineering': 'Software Development',
            'eCommerce': 'E-Commerce',
            'Retail - eCommerce': 'E-Commerce',
            'Health Care': 'Healthcare',
            'Fasion': 'Fashion',
            'fintech': 'Financial Technology',
            'blockchain': 'Blockchain',
            'higher': 'Higher Education'
        }
        self.logger.info("Cleaning names of industries")
        for old_name, new_name in industry_names.items():
            sql = "UPDATE industries SET name='{1}' WHERE name='{0}'".format(
                old_name, new_name)
            result = self.db_session.execute(sql)
            self.db_session.commit()
            if result.rowcount > 0:
                self.logger.info(
                    "The industry name '{0}' was changed to '{1}': {2} "
                    "time{3}".format(
                        old_name, new_name, result.rowcount,
                        's' if result.rowcount > 1 else ''))
            else:
                self.logger.warning(
                    "The industry name '{0}' couldn't be found".format(old_name))

    def _count_industries(self):
        """
        Returns industries sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (industry, count).

        :return: list of tuples of the form (industry, count)
        """
        sql = "SELECT name, COUNT(name) as CountOf from industries " \
              "GROUP BY name ORDER BY CountOf DESC"
        return self.db_session.execute(sql).fetchall()

    # TODO: this method should be in the parent class `Analyzer`
    # `Analyzer` will need to have access to logger. Thus logging setup should
    # be done within `Analyzer`.
    def _generate_bar_chart(self, sorted_topic_count, bar_chart_cfg):
        sorted_topic_count = np.array(sorted_topic_count)
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        # TODO: add spinner when loading this module
        self.logger.info("loading module 'utility.graphutil' ...")
        from utility.graphutil import draw_bar_chart
        self.logger.debug("finished loading module 'utility.graphutil'")
        self.logger.info(
            "Generating bar chart: {} vs {} ...".format(
                bar_chart_cfg['xlabel'], bar_chart_cfg['ylabel']))
        topk = bar_chart_cfg['topk']
        new_labels = self._shrink_labels(
            labels=sorted_topic_count[:topk, 0],
            max_length=bar_chart_cfg['max_xtick_label_length'])
        draw_bar_chart(
            x=np.array(new_labels),
            y=sorted_topic_count[:topk, 1].astype(np.int32),
            xlabel=bar_chart_cfg['xlabel'],
            ylabel=bar_chart_cfg['ylabel'],
            title=bar_chart_cfg['title'].format(topk),
            grid_which=bar_chart_cfg['grid_which'],
            color=bar_chart_cfg['color'],
            fig_width=bar_chart_cfg['fig_width'],
            fig_height=bar_chart_cfg['fig_height'])
