import os
# Third-party modules
import ipdb
import numpy as np
# Own modules
from .analyzer import Analyzer
from utilities.genutils import convert_list_to_str


class JobSalariesAnalyzer(Analyzer):
    def __init__(self, analysis_type, conn, db_session, main_cfg, logging_cfg):
        # Salaries stats to compute
        # NOTE: not all fields are numpy arrays
        # e.g. `job_id_to_mid_range_salary` is a dict
        self.stats_names = [
            "salaries",  # np.array: job_post_id, min_salary, max_salary
            "job_ids_with_salary",  # np.array; IMPORTANT: all job ids UNSORTED
            "job_id_to_mid_range_salary",  # dict; IMPORTANT: all job ids
            "mid_range_salaries_asc",  # ASC order
            "min_mid_range_salary",
            "max_mid_range_salary",
            "mean_mid_range_salary",
            "std_mid_range_salary",
            "avg_mid_range_salaries_in_europe",  # np.array
            "avg_mid_range_salaries_in_industries",  # np.array
            "avg_mid_range_salaries_in_roles",  # np.array
            "avg_mid_range_salaries_in_skills",  # np.array
            "avg_mid_range_salaries_in_usa",  # np.array
            "avg_mid_range_salaries_in_world",  # np.array
        ]
        self.report = {
            "currency": None,
            "min_mid_range_salary": None,
            "max_mid_range_salary": None,
            "mean_mid_range_salary": None,
            "std_mid_range_salary": None,
            'histogram': {
                'items': {
                    'labels': ['mid_range_salaries_asc'],
                    'data': [],
                    'number_of_items': None,
                },
                'job_post_ids': [],
                'number_of_job_posts': None,  
                'published_dates': [],
            },
            'scatter_europe': {
                'items': {
                    'labels': ['country',
                               'average_mid_range_salary_desc',
                               'count_'],
                    'data': [],
                    'number_of_items': None,
                },
                'job_post_ids': [],
                'number_of_job_posts': None,  
                'published_dates': [],
            },
            'scatter_industries': {
                'items': {
                    'labels': ['industry',
                               'average_mid_range_salary_desc',
                               'count_'],
                    'data': [],
                    'number_of_items': None,
                },
                'job_post_ids': [],
                'number_of_job_posts': None,  
                'published_dates': [],
            },
            'scatter_roles': {
                'items': {
                    'labels': ['role',
                               'average_mid_range_salary_desc',
                               'count_'],
                    'data': [],
                    'number_of_items': None,
                },
                'job_post_ids': [],
                'number_of_job_posts': None,
                'published_dates': [],
            },
            'scatter_skills': {
                'items': {
                    'labels': ['skill',
                               'average_mid_range_salary_desc',
                               'count_'],
                    'data': [],
                    'number_of_items': None,
                },
                'job_post_ids': [],
                'number_of_job_posts': None,  
                'published_dates': [],
            },
            'scatter_usa': {
                'items': {
                    'labels': ['us_state',
                               'average_mid_range_salary_desc',
                               'count_'],
                    'data': [],
                    'number_of_items': None,
                },
                'job_post_ids': [],
                'number_of_job_posts': None,  
                'published_dates': [],
            },
            'scatter_world': {
                'items': {
                    'labels': ['country',
                               'average_mid_range_salary_desc',
                               'count_'],
                    'data': [],
                    'number_of_items': None,
                },
                'job_post_ids': [],
                'number_of_job_posts': None,  
                'published_dates': [],
            },
        }
        super().__init__(analysis_type,
                         conn,
                         db_session,
                         main_cfg,
                         logging_cfg,
                         self.stats_names,
                         self.report,
                         __name__,
                         __file__,
                         os.getcwd())
        # List of topics against which to compute salary stats/graphs
        self.salary_topics = self._get_salary_topics()
        # If the JSON is not found, an exception is triggered
        self.us_states = self._load_json(os.path.expanduser(
            self.main_cfg['data_filepaths']['us_states']))

    def run_analysis(self):
        # Reset all locations stats to be computed
        self.reset_stats()
        # Get all salaries
        # 3 columns returned: job_post_id, min_salary, max_salary
        # Numpy arrays are of shape: (number_of_rows_in_result_set, 3)
        salaries = np.array(self._get_salaries())
        self.stats['salaries'] = salaries
        self.stats['job_ids_with_salary'] = salaries[:, 0].tolist()
        # Sanity check on `job_post_id`s to make sure only one salary per `job_post_id`
        assert len(np.unique(self.stats['job_ids_with_salary'])) == \
            len(self.stats['job_ids_with_salary']), \
            "There should be one salary per job post"
        # Compute mid-range salary for each min-max salary interval
        self._compute_mid_range_salaries()
        # Compute global stats on salaries, e.g. global max/min mid-range salaries
        self._compute_global_stats()
        # =====================================================================
        #                          Analysis by topic
        # =====================================================================
        # Analyze salary by different topics (e.g. industries, skills)
        for topic in self.salary_topics:
            try:
                key_scatter_cfg = 'scatter_salary_{}'.format(topic)
                scatter_cfg = self.main_cfg['job_salaries'][key_scatter_cfg]
                key_stats = 'avg_mid_range_salaries_in_{}'.format(topic)
                self.stats[key_stats] = self._analyze_salary_by_topic(
                    topic=topic,
                    use_fullnames=scatter_cfg.get('use_fullnames', False))
            except AttributeError as e:
                self.logger.critical(e)
                self.logger.error("The topic '{}' will be skipped!".format(topic))
            else:
                # =============================================================
                #                       Scatter plot
                # =============================================================
                # Generate scatter plot of number of job posts vs average
                # mid-range salary for each topic (e.g. locations, roles)
                self._generate_scatter_plot(
                    scatter_type=key_scatter_cfg,
                    y=self.stats[key_stats]['count'],
                    x=self.stats[key_stats]['average_mid_range_salary'],
                    text=self.stats[key_stats][topic],
                    scatter_cfg=scatter_cfg,
                    append_xlabel_title="({})".format(
                        self.main_cfg['job_salaries']['salary_currency']))
        # =====================================================================
        #                            Histogram
        # =====================================================================
        # Histogram
        hist_cfg = self.main_cfg['job_salaries']['histogram_job_salaries']
        self._generate_histogram(
            hist_type='histogram_job_salaries',
            data=self.stats['mid_range_salaries_asc'],
            hist_cfg=hist_cfg,
            append_xlabel_title="({})".format(
                self.main_cfg['job_salaries']['salary_currency']))
        # Update report for histogram
        self._update_graph_report(
            graph_report=self.report['histogram'],
            items=self.stats['mid_range_salaries_asc'].tolist(),
            job_post_ids=self.stats['job_ids_with_salary'])
        # TODO: add pie charts
        # self._generate_pie_chart()
        # =====================================================================
        #                               Report
        # =====================================================================
        # Update report for overall stats
        self.report['currency'] = self.main_cfg['job_salaries']['salary_currency']
        self.report['min_mid_range_salary'] = self.stats['min_mid_range_salary']
        self.report['max_mid_range_salary'] = self.stats['max_mid_range_salary']
        self.report['mean_mid_range_salary'] = self.stats['mean_mid_range_salary']
        self.report['std_mid_range_salary'] = self.stats['std_mid_range_salary']
        if self.main_cfg['job_salaries']['save_report']:
            self._save_report(self.main_cfg['job_salaries']['report_filename'])

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

    def _analyze_salary_by_topic(self, topic, use_fullnames=False):
        try:
            select_method = self.__getattribute__("_select_{}".format(topic))
        except AttributeError as e:
            raise AttributeError(e)
        # Get topic's records in the db that have a salary associated with
        job_post_ids = self.stats['job_ids_with_salary']
        # Convert the list of `job_post_id` as a string to be used inside SQL
        # expression
        str_job_post_ids = convert_list_to_str(job_post_ids)
        # Two columns selected: job_post_id and name
        # e.g. name of an industry or a skill
        results = select_method(str_job_post_ids, use_fullnames)
        # Sanity check on `job_post_id`s
        # `set1`: all `job_post_id`s
        set1 = set(job_post_ids)
        # `set2`: `job_post_id`s only for the topic
        set2 = set(np.array(results)[:, 0].astype(np.int).tolist())
        assert len(set2) == len(set1.intersection(set2)), \
            "set1 (all `job_post_id`s) should be a superset of set2 " \
            "(`job_post_id`s for the given topic only '{}'".format(topic)
        # Process results to extract average mid-range salaries for each
        # topic's rows
        struct_arr = self._process_topic_with_salaries(results, topic)
        # Update report for the given topic (e.g. industries, skills)
        # NOTE: `items` are sorted by the `average_mid_range_salary`
        self._update_graph_report(
            graph_report=self.report['scatter_{}'.format(topic)],
            items=struct_arr.tolist(),
            job_post_ids=list(set2))
        return struct_arr

    def _compute_global_stats(self):
        # Get the mid range salaries only, not the job_post_id column
        mid_range_salaries_asc = self.stats["mid_range_salaries_asc"]
        self.stats["mean_mid_range_salary"] = \
            round(mid_range_salaries_asc.mean())
        # Compute std across list of mid-range salaries
        self.stats["std_mid_range_salary"] = \
            round(mid_range_salaries_asc.std())
        # Get min and max salaries across list of mid-range salaries
        # Since the `mid_range_salaries_asc` are in ascending order, we can
        # easily get the min and max salaries from the first and last elements,
        # respectively.
        self.stats["min_mid_range_salary"] = mid_range_salaries_asc[0]
        self.stats["max_mid_range_salary"] = mid_range_salaries_asc[-1]

    def _compute_mid_range_salaries(self):
        # Compute mid-range salary for each min-max salary interval
        salary_ranges = self.stats['salaries'][:, 1:3]
        mid_range_salaries = salary_ranges.mean(axis=1)
        self.stats['job_id_to_mid_range_salary'] = \
            dict(zip(self.stats['salaries'][:, 0], mid_range_salaries))
        # Sort the mid range salaries in ascending order
        self.stats['mid_range_salaries_asc'] = np.sort(mid_range_salaries)

    # `sorted_topic_count` is a numpy array
    def _generate_histogram(self, hist_type, data, hist_cfg,
                            append_xlabel_title='', append_ylabel_title=''):
        if not hist_cfg['display_graph'] and not hist_cfg['save_graph']:
            self.logger.warning("The bar chart '{}' is disabled for the '{}' "
                                "analysis".format(hist_type, self.analysis_type))
            return 1
        if append_xlabel_title:
            hist_cfg['xlabel'] += " " + append_xlabel_title
        if append_ylabel_title:
            hist_cfg['ylabel'] += " " + append_ylabel_title
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        self.logger.info("loading module 'utilities.graphutil' ...")
        from utilities.graphutils import draw_histogram
        self.logger.debug("finished loading module 'utilities.graphutil'")
        self.logger.info("Generating histogram: {} ...".format(hist_type))
        if hist_cfg['start_bins'] == "min":
            start_bins = data.min()
        else:
            start_bins = hist_cfg['start_bins']
        if hist_cfg['end_bins'] == "max":
            end_bins = data.max() + 1
        else:
            end_bins = hist_cfg['end_bins']
        size_bins = hist_cfg['size_bins']
        draw_histogram(
            data=data,
            bins=np.arange(start_bins, end_bins, size_bins),
            xlabel=hist_cfg['xlabel'],
            ylabel=hist_cfg['ylabel'],
            title=hist_cfg['title'],
            grid_which=hist_cfg['grid_which'],
            color=hist_cfg['color'],
            xaxis_major_mutiplelocator=hist_cfg['xaxis_major_mutiplelocator'],
            xaxis_minor_mutiplelocator=hist_cfg['xaxis_minor_mutiplelocator'],
            yaxis_major_mutiplelocator=hist_cfg['yaxis_major_mutiplelocator'],
            yaxis_minor_mutiplelocator=hist_cfg['yaxis_minor_mutiplelocator'],
            fig_width=hist_cfg['fig_width'],
            fig_height=hist_cfg['fig_height'],
            display_graph=hist_cfg['display_graph'],
            save_graph=hist_cfg['save_graph'],
            fname=os.path.join(self.main_cfg['saving_dirpath'],
                               hist_cfg['fname']))

    def _generate_pie_chart(self, sorted_topic_count, pie_chart_cfg):
        pass

    # TODO: check if we can add the currency directly within the YAML config file
    # like we can do it with config.ini using the '%(currency)s' operator
    def _generate_scatter_plot(self, scatter_type, x, y, text, scatter_cfg,
                               append_xlabel_title='', append_ylabel_title=''):
        plot_cfg = scatter_cfg['plot']
        plotly_cfg = scatter_cfg['plotly']
        if plot_cfg['output_type'] == 'None':
            self.logger.warning(
                "The scatter plot '{}' is disabled for the '{}' analysis".format(
                    scatter_type, self.analysis_type))
            return 1
        if append_xlabel_title:
            plotly_cfg['layout']['xaxis']['title'] += " " + append_xlabel_title
        if append_ylabel_title:
            plotly_cfg['layout']['yaxis']['title'] += " " + append_ylabel_title
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        self.logger.info("loading module 'utilities.graphutil' ...")
        from utilities.graphutils import draw_scatter_plot
        self.logger.debug("finished loading module 'utilities.graphutil'")
        self.logger.info("Generating scatter plot '{}' ...".format(scatter_type))
        # Add full path to plot's filename
        plot_cfg['filename'] = os.path.join(
            self.main_cfg['saving_dirpath'], plot_cfg['filename'])
        draw_scatter_plot(x=x,
                          y=y,
                          text=text,
                          scatter_cfg=plotly_cfg['scatter'],
                          layout_cfg=plotly_cfg['layout'],
                          plot_cfg=plot_cfg)

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
                self.main_cfg['job_salaries']['salary_currency'],
                self.main_cfg['job_salaries']['salary_thresholds']['min_salary'],
                self.main_cfg['job_salaries']['salary_thresholds']['max_salary'])
        return self.db_session.execute(sql).fetchall()

    def _get_salary_topics(self):
        return [k for k, v in self.main_cfg['job_salaries']["topics"].items()
                if v]

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
        # In Python3, if I use S30, the string will be considered as bytestrings
        # with the b' appended at the beginning of each string
        # ref.: https://stackoverflow.com/a/50050905
        # TODO: labels for structured array should be saved in an instance
        # variable in case they might change, no need to change different
        # parts in the source code
        dtype = [(topic, "U30"),
                 ("average_mid_range_salary", float),
                 ("count", int)]
        struct_arr = np.array(topic_name_salary_list, dtype=dtype)
        # Sort the array based on the field 'average_mid_range_salary' and in
        # descending order of the given field
        struct_arr.sort(order="average_mid_range_salary")
        struct_arr = struct_arr[::-1]
        return struct_arr

    def _select_europe(self, job_post_ids, use_fullnames=False):
        """
        Returns all European countries with the specified `job_post_ids`. A list
        of tuples is returned where a tuple is of the form (job_post_id, country).

        :return: list of tuples of the form (job_post_id, country)
        """
        sql = "SELECT job_post_id, country FROM job_locations WHERE" \
              " job_post_id in ({}) and country in ({})".format(
                job_post_ids, self._get_european_countries_as_str())
        results = self.db_session.execute(sql).fetchall()
        if use_fullnames:
            results = self._convert_countries_names(
                list_countries=results,
                converter=self._get_country_name)
        return results

    def _select_industries(self, job_post_ids, *args):
        """
        Returns all industries with the specified `job_post_id`s. A list of
        tuples is returned where a tuple is of the form (job_post_id, name).

        :return: list of tuples of the form (job_post_id, name)
        """
        sql = "SELECT job_post_id, name FROM industries WHERE job_post_id in " \
              "({})".format(job_post_ids)
        return self.db_session.execute(sql).fetchall()

    def _select_roles(self, job_post_ids, *args):
        """
        Returns all roles with the specified `job_post_id`s. A list of
        tuples is returned where a tuple is of the form (job_post_id, name).

        :return: list of tuples of the form (job_post_id, name)
        """
        sql = "SELECT job_post_id, name FROM roles WHERE job_post_id in " \
              "({})".format(job_post_ids)
        return self.db_session.execute(sql).fetchall()

    def _select_skills(self, job_post_ids, *args):
        """
        Returns all skills with the specified `job_post_id`s. A list of
        tuples is returned where a tuple is of the form (job_post_id, name).

        :return: list of tuples of the form (job_post_id, name)
        """
        sql = "SELECT job_post_id, name FROM skills WHERE job_post_id in " \
              "({})".format(job_post_ids)
        return self.db_session.execute(sql).fetchall()

    def _select_usa(self, job_post_ids, use_fullnames=False):
        """
        Returns all US states with the specified `job_post_id`s. A list of
        tuples is returned where a tuple is of the form (job_post_id, country).
        US locations where region is 'NULL' are ignored.

        :return: list of tuples of the form (job_post_id, region)
        """
        sql = "SELECT job_post_id, region FROM job_locations WHERE job_post_id " \
              "in ({}) and country='US' and region!='NULL'".format(job_post_ids)
        results = self.db_session.execute(sql).fetchall()
        if use_fullnames:
            results = self._convert_countries_names(
                list_countries=results,
                converter=self.us_states.get)
        return results

    def _select_world(self, job_post_ids, use_fullnames=False):
        """
        Returns all countries with the specified `job_post_id`s. A list of
        tuples is returned where a tuple is of the form (job_post_id, country).

        :return: list of tuples of the form (job_post_id, country)
        """
        sql = "SELECT job_post_id, country FROM job_locations WHERE job_post_id " \
              "in ({})".format(job_post_ids)
        results = self.db_session.execute(sql).fetchall()
        if use_fullnames:
            results = self._convert_countries_names(
                list_countries=results,
                converter=self._get_country_name)
        return results

