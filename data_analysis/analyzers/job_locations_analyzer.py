import os
import sys
# Third-party modules
import ipdb
import numpy as np
# Own modules
from .analyzer import Analyzer
# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
from utility.script_boilerplate import LoggingBoilerplate


class JobLocationsAnalyzer(Analyzer):
    def __init__(self, conn, db_session, main_config, logging_config):
        # Locations stats to compute
        self.stats_names = [
            "locations_info", "sorted_countries_count", "sorted_us_states_count",
            "sorted_european_countries_count"]
        super().__init__(conn, db_session, main_config, logging_config,
                         self.stats_names)
        sb = LoggingBoilerplate(
            module_name=__name__,
            module_file=__file__,
            cwd=os.getcwd(),
            logging_config=logging_config)
        self.logger = sb.get_logger()

    def run_analysis(self):
        # Reset all locations stats to be computed
        self.reset_stats()
        # Countries analysis
        # Get counts of countries, i.e. for each country we want to know its
        # number of occurrences in job posts
        countries_count = self._count_countries()
        self.logger.debug(
            "There are {} distinct countries from the job posts".format(
                len(countries_count)))
        self.logger.debug(
            "There are in total {} countries from the job posts".format(
                sum(j for i, j in countries_count)))
        # NOTE: these are all the countries and they are sorted in order of
        # decreasing number of occurrences (i.e. most popular country at
        # first)
        self.stats["sorted_countries_count"] = np.array(countries_count)
        bar_chart_config \
            = self.main_config["graphs_config"]["bar_chart_countries"]
        self._generate_bar_chart(
            sorted_stats_count=self.stats["sorted_countries_count"],
            bar_chart_config=bar_chart_config)

        # US states analysis
        us_states_count = self._count_us_states()
        self.logger.debug(
            "There are {} distinct US states".format(
                len(us_states_count)))
        self.logger.debug(
            "There are in total {} US states".format(
                sum(j for i, j in us_states_count)))
        # NOTE: these are all the US states and they are sorted in order of
        # decreasing number of occurrences (i.e. most popular US state at first)
        self.stats["sorted_us_states_count"] = np.array(us_states_count)
        # TODO: Pycharm complains about using '==' but if I use 'is', the
        # `np.where` statement won't work
        indices = np.where(self.stats["sorted_us_states_count"] == None)
        if indices[0]:
            assert len(indices) == 2, "There should be 2 indices"
            self.logger.debug("There are {} 'None' US state".format(
                self.stats["sorted_us_states_count"][indices[0]][0][1]))
        bar_chart_config \
            = self.main_config["graphs_config"]["bar_chart_us_states"]
        self._generate_bar_chart(
            sorted_stats_count=self.stats["sorted_us_states_count"],
            bar_chart_config=bar_chart_config)

        # European analysis
        """
        european_countries_count = self._get_european_countries()
        self.logger.debug(
            "There are {} distinct european countries".format(
                len(european_countries_count)))
        self.logger.debug(
            "There are in total {} european countries".format(
                sum(j for i, j in european_countries_count)))
        # NOTE: these are all the european countries and they are sorted in order
        # of decreasing number of occurrences (i.e. most popular european country
        # at first)
        self.stats["sorted_us_states_count"] = np.array(european_countries_count)
        bar_chart_config \
            = self.main_config["graphs_config"]["bar_chart_european_countries"]
        self._generate_bar_chart(
            sorted_stats_count=self.stats["sorted_european_countries_count"],
            bar_chart_config=bar_chart_config)
        """

    def _count_countries(self):
        """
        Returns countries sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (role, count).

        :return: list of tuples of the form (location, count)
        """
        sql = "SELECT country, COUNT(country) as CountOf FROM job_locations GROUP " \
              "BY country ORDER BY CountOf DESC"
        return self.db_session.execute(sql).fetchall()

    def _count_us_states(self):
        """
        Returns US states sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (us_state, count).

        :return: list of tuples of the form (us_state, count)
        """
        sql = "SELECT region, COUNT(country) as CountOf FROM job_locations " \
              "WHERE country='US' GROUP BY region ORDER BY CountOf DESC"
        return self.db_session.execute(sql).fetchall()

    def _count_european_countries(self):
        """
        Returns european countries sorted in decreasing order of their
        occurrences in job posts. A list of tuples is returned where a tuple is
        of the form (country, count).

        :return: list of tuples of the form (country, count)
        """
        european_countries = self._get_european_countries()
        sql = \
            "SELECT region, COUNT(region) as CountOf FROM job_locations WHERE " \
            "country in ({}) GROUP BY region ORDER BY CountOf DESC".format(
                european_countries)
        return self.db_session.execute(sql).fetchall()

    # Generate bar chart of roles vs number of job posts
    def _generate_bar_chart(self, sorted_stats_count, bar_chart_config):
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        self.logger.info("loading module 'utility.graphutil' ...")
        from utility.graphutil import generate_bar_chart
        self.logger.debug("finished loading module 'utility.graphutil'")
        self.logger.info(
            "Generating bar chart: {} vs Number of job posts ...".format(
                bar_chart_config["xlabel"]))
        top_k = bar_chart_config["top_k"]
        new_labels = self._shrink_labels(
            labels=sorted_stats_count[:top_k, 0],
            max_length=bar_chart_config["max_xtick_label_length"])
        generate_bar_chart(
            x=np.array(new_labels),
            y=sorted_stats_count[:top_k, 1].astype(np.int32),
            xlabel=bar_chart_config["xlabel"],
            ylabel=bar_chart_config["ylabel"],
            title=bar_chart_config["title"].format(top_k),
            grid_which=bar_chart_config["grid_which"],
            fig_width=bar_chart_config["fig_width"],
            fig_height=bar_chart_config["fig_height"])

    @staticmethod
    def _get_european_countries():
        european_countries = ''
        return european_countries
