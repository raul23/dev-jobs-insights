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


class JobSalariesAnalyzer(Analyzer):
    def __init__(self, conn, db_session, main_config, logging_config):
        # Salaries stats to compute
        # NOTE: not all fields are numpy arrays
        # e.g. `job_id_to_mid_range_salary` is a dict
        self.stats_names = [
            "salaries",
            "job_ids_with_salary",
            "job_id_to_mid_range_salary",
            "sorted_mid_range_salaries",
            "mean_mid_range_salary",
            "std_mid_range_salary",
            "min_mid_range_salary",
            "max_mid_range_salary",
            "avg_mid_range_salaries_by_all_countries",
            "avg_mid_range_salaries_by_european_countries",
            "avg_mid_range_salaries_by_us_states",
            "avg_mid_range_salaries_by_industries",
            "avg_mid_range_salaries_by_roles",
            "avg_mid_range_salaries_by_tags",
        ]
        super().__init__(conn, db_session, main_config, logging_config,
                         self.stats_names)
        # TODO: the logging boilerplate code should be done within the parent
        # class `Analyzer`
        sb = LoggingBoilerplate(
            module_name=__name__,
            module_file=__file__,
            cwd=os.getcwd(),
            logging_config=logging_config)
        self.logger = sb.get_logger()
        # List of topics against which to compute salary stats/graphs
        self.salary_topics = self._get_salary_topics()

    def run_analysis(self):
        # Reset all locations stats to be computed
        self.reset_stats()
        # Get all salaries
        # 3 columns returned: job_post_id, min_salary, max_salary
        # Numpy arrays are of shape: (number_of_rows_in_result_set, 3)
        salaries = np.array(self._get_salaries())
        self.stats['salaries'] = salaries
        self.stats['job_ids_with_salary'] = salaries[:, 0]
        # Compute mid-range salary for each min-max salary interval
        self._compute_mid_range_salaries()
        # Compute global stats on salaries, e.g. global max/min mid-range salaries
        self._compute_global_stats()
        # Analyze salary by different topics
        for topic in self.salary_topics:
            try:
                self._analyze_salary_by_topic(topic)
            except AttributeError as e:
                self.logger.critical(e)
                self.logger.error("The topic '{}' will be skipped!".format(topic))
            else:
                # Generate scatter plot of number of job posts vs average
                # mid-range salary for each topic (e.g. locations, roles)
                pass
        ###########################
        #        Graphs
        ###########################
        histogram_config = \
            self.main_config['graphs_config']['histogram_job_salaries']
        self._generate_histogram(
            sorted_topic_count=self.stats['sorted_mid_range_salaries'],
            hist_config=histogram_config)
        # self._generate_bar_chart()
        # self._generate_pie_chart()

    # `sorted_topic_count` is a numpy array
    def _generate_histogram(self, sorted_topic_count, hist_config):
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        self.logger.info("loading module 'utility.graphutil' ...")
        from utility.graphutil import draw_histogram
        self.logger.debug("finished loading module 'utility.graphutil'")
        self.logger.info(
            "Generating histogram: {} vs {} ...".format(
                hist_config["xlabel"], hist_config["ylabel"]))
        if hist_config['start_bins'] == "min":
            start_bins = sorted_topic_count.min()
        else:
            start_bins = hist_config['start_bins']
        if hist_config['end_bins'] == "max":
            end_bins = sorted_topic_count.max() + 1
        else:
            end_bins = hist_config['end_bins']
        size_bins = hist_config['size_bins']
        draw_histogram(
            data=sorted_topic_count,
            bins=np.arange(start_bins, end_bins, size_bins),
            xlabel=hist_config['xlabel'],
            ylabel=hist_config['ylabel'],
            title=hist_config['title'],
            grid_which=hist_config['grid_which'],
            color=hist_config['color'],
            xaxis_major_mutiplelocator=hist_config['xaxis_major_mutiplelocator'],
            xaxis_minor_mutiplelocator=hist_config['xaxis_minor_mutiplelocator'],
            yaxis_major_mutiplelocator=hist_config['yaxis_major_mutiplelocator'],
            yaxis_minor_mutiplelocator=hist_config['yaxis_minor_mutiplelocator'],
            fig_width=hist_config['fig_width'],
            fig_height=hist_config['fig_height'])

    def _generate_pie_chart(self, sorted_topic_count, pie_chart_config):
        pass

    def _analyze_salary_by_topic(self, topic):
        try:
            select_method = self.__getattribute__("_select_{}".format(topic))
            process_results_method = \
                self.__getattribute__("_process_{}_with_salaries".format(topic))
        except AttributeError as e:
            raise AttributeError(e)
        # Get topic's records in the db that have a salary associated with
        job_post_ids = self.stats['job_ids_with_salary']
        results = select_method(job_post_ids)
        # Sanity check on job_post_ids
        set1 = set(job_post_ids)
        set2 = set(np.array(results)[:, 0].astype(np.int))
        assert len(set2) == len(set1.intersection(set2)), \
            "set1 (all job_post_ids) should be a superset of set2"
        # Process results to extract average mid-range salaries for each topic's rows
        process_results_method(results, topic)

    def _compute_global_stats(self):
        # Get the mid range salaries only, not the job_post_id column
        sorted_mid_range_salaries = self.stats["sorted_mid_range_salaries"]
        self.stats["mean_mid_range_salary"] = round(sorted_mid_range_salaries.mean())
        # Compute std across list of mid-range salaries
        self.stats["std_mid_range_salary"] = round(sorted_mid_range_salaries.std())
        # Get min and max salaries across list of mid-range salaries
        # Since the `sorted_mid_range_salaries` are in ascending order, we can
        # easily get the min and max salaries from the first and last elements,
        # respectively.
        self.stats["min_mid_range_salary"] = sorted_mid_range_salaries[0]
        self.stats["max_mid_range_salary"] = sorted_mid_range_salaries[-1]

    def _compute_mid_range_salaries(self):
        # Compute mid-range salary for each min-max salary interval
        salary_ranges = self.stats['salaries'][:, 1:3]
        mid_range_salaries = salary_ranges.mean(axis=1)
        self.stats['job_id_to_mid_range_salary'] = \
            dict(zip(self.stats['salaries'][:, 0], mid_range_salaries))
        # Sort the mid range salaries in ascending order
        self.stats['sorted_mid_range_salaries'] = np.sort(mid_range_salaries)

    def _generate_bar_chart(self, sorted_topic_count, bar_chart_config):
        sorted_topic_count = np.array(sorted_topic_count)
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        self.logger.info("loading module 'utility.graphutil' ...")
        from utility.graphutil import draw_bar_chart
        self.logger.debug("finished loading module 'utility.graphutil'")
        self.logger.info(
            "Generating bar chart: {} vs {} ...".format(
                bar_chart_config["xlabel"], bar_chart_config["ylabel"]))
        topk = bar_chart_config["topk"]
        new_labels = self._shrink_labels(
            labels=sorted_topic_count[:topk, 0],
            max_length=bar_chart_config["max_xtick_label_length"])
        draw_bar_chart(
            x=np.array(new_labels),
            y=sorted_topic_count[:topk, 1].astype(np.int32),
            xlabel=bar_chart_config["xlabel"],
            ylabel=bar_chart_config["ylabel"],
            title=bar_chart_config["title"].format(topk),
            grid_which=bar_chart_config["grid_which"],
            fig_width=bar_chart_config["fig_width"],
            fig_height=bar_chart_config["fig_height"])

    def _get_salaries(self):
        """
        Returns all salaries with the specified currency and within an interval
        (inclusively). A list of tuples is returned where a tuple is of the form
        (job_post_id, min_salary, max_salary).

        :return: list of tuples of the form (job_post_id, min_salary, max_salary)
        """
        # TODO: use parameterized SQL expressions
        sql = "SELECT job_post_id, min_salary, max_salary FROM job_salaries " \
              "WHERE currency='{0}' and min_salary >= {1} and max_salary <= " \
              "{2}".format(
                self.main_config['salary_currency'],
                self.main_config['salary_thresholds']['min_salary'],
                self.main_config['salary_thresholds']['max_salary'])
        return self.db_session.execute(sql).fetchall()

    def _get_salary_topics(self):
        return [k for k, v in self.main_config["salary_analysis_by_topic"].items()
                if v]

    def _process_industries_with_salaries(self, industry_names, topic):
        self.stats["avg_mid_range_salaries_by_industries"] \
            = self._process_topic_with_salaries(industry_names, topic)

    def _process_topic_with_salaries(self, topic_names, topic):
        topic_name_to_salary = {}
        # TODO: simplify with pandas (group by)
        for job_id, topic_name in topic_names:
            self._add_salary(topic_name_to_salary, topic_name, job_id)
        # Keep every fields, except "cumulative_sum" and build a structured array
        # out of the dict
        topic_name_salary_list = [(k, v["average_mid_range_salary"], v["count"])
                                  for k, v in topic_name_to_salary.items()]
        # Fields (+ data types) for the structured array
        # TODO: adjust precision of float numbers
        # TODO: the length of the string field should be set in a config (for
        # each topic?)
        dtype = [(topic, "S30"),
                 ("average_mid_range_salary", float),
                 ("count", int)]
        struct_arr = np.array(topic_name_salary_list, dtype=dtype)
        # Sort the array based on the field 'average_mid_range_salary' and in
        # descending order of the given field
        struct_arr.sort(order="average_mid_range_salary")
        struct_arr = struct_arr[::-1]
        return struct_arr

    def _add_salary(self, dict_, topic_name, job_id):
        dict_.setdefault(topic_name, {"average_mid_range_salary": 0,
                                      "cumulative_sum": 0,
                                      "count": 0})
        mid_range_salary = self.stats["job_id_to_mid_range_salary"][job_id]
        # Update count
        dict_[topic_name]["count"] += 1
        cum_sum = dict_[topic_name]["cumulative_sum"]
        # Update average
        dict_[topic_name]["average_mid_range_salary"] \
            = (cum_sum + mid_range_salary) / dict_[topic_name]["count"]
        # Update cumulative sum
        dict_[topic_name]["cumulative_sum"] += mid_range_salary

    def _select_industries(self, job_post_ids):
        """
        Returns all industries with the specified `job_post_id`s. A list of
        tuples is returned where a tuple is of the form (job_post_id, name,).

        :return: list of tuples of the form (job_post_id, name,)
        """
        job_post_ids = ", ".join(
            map(lambda a: "'{}'".format(a), job_post_ids))
        sql = "SELECT job_post_id, name FROM industries WHERE job_post_id in " \
              "({})".format(job_post_ids)
        return self.db_session.execute(sql).fetchall()
