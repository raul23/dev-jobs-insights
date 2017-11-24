import time

import numpy as np
import ipdb

from utility import util, graph_util as g_util


class SalaryAnalyzer:
    def __init__(self, conn, salary_topics, config_ini):
        # Connection to jobs_insights.sqlite db
        self.conn = conn
        # List of topics against which to compute salary insights
        self.salary_topics = salary_topics
        self.config_ini = config_ini
        countries_path = self.config_ini["paths"]["countries_path"]
        us_states_path = self.config_ini["paths"]["us_states_path"]
        cached_transl_countries_path = self.config_ini["paths"]["cached_transl_countries_path"]
        self.salary_topics = self.get_salary_topics()
        self.topic_to_titles = self.get_topic_titles()
        try:
            self.stack_loc = util.StackOverflowLocation(countries_path, us_states_path, cached_transl_countries_path)
        except FileNotFoundError:
            print("ERROR: analyze_salary will be skipped because the StackOverflow"
                  "object could not be created")
            self.stack_loc = None
        # Salary stats to compute
        self.salary_stats_names = [
            "min_salaries",
            "max_salaries",
            "job_ids_with_salary",
            "job_id_to_salary_mid_ranges",
            "sorted_salary_mid_ranges",
            "min_job_id",
            "max_job_id",
            "global_mean_salary",
            "global_std_salary",
            "global_min_salary",
            "global_max_salary",
            "avg_mid_range_salaries_by_countries",
            "avg_mid_range_salaries_by_us_states",
            "avg_mid_range_salaries_by_industries",
            "avg_mid_range_salaries_by_roles",
            "avg_mid_range_salaries_by_tags",
        ]
        self.salary_stats = dict(zip(self.salary_stats_names, [None] * len(self.salary_stats_names)))

    def get_salary_topics(self):
        return [k for k,v in self.config_ini["salary_analysis_by_topic"].items() if v]

    def get_topic_titles(self):
        topic_to_titles = {}
        for topic in self.salary_topics:
            # TODO: remove the if; it is a hack while locations (in
            # salary_analysis_by_topic config.ini) is not broken up into
            # countries and us_states
            if topic == "locations":
                title = self.config_ini["scatter_salary_topic"]["countries_title"]
                # Build complete title
                complete_title = "Average mid-range salary of {}".format(title)
                topic_to_titles["countries"] = complete_title

                title = self.config_ini["scatter_salary_topic"]["us_states_title"]
                # Build complete title
                complete_title = "Average mid-range salary of {}".format(title)
                topic_to_titles["us_states"] = complete_title
            else:
                title = self.config_ini["scatter_salary_topic"]["{}_title".format(topic)]
                # Build complete title
                title = "Average mid-range salary of {}".format(title)
                topic_to_titles[topic] = title
        return topic_to_titles

    def reset_stats(self):
        self.salary_stats = dict(zip(self.salary_stats_names, [None] * len(self.salary_stats_names)))

    def run_analysis(self):
        ipdb.set_trace()
        # Reset salary stats
        self.reset_stats()
        # Compute salary mid-range for each min-max interval
        self.compute_salary_mid_ranges()
        # Compute global stats on salaries, e.g. global max/min mid-range salaries
        self.compute_global_stats()

        # Analyze salary by different topics
        # TODO: see if you can divide locations into countries and us_states
        for topic in self.salary_topics:
            retval = self.analyze_salary_by_topic(topic)
            if retval is None:
                print("ERROR: the topic '{}' will be skipped because an error "
                      "occurred while processing it".format(topic))

        # Generate all the graphs (histogram, scatter plots)
        self.generate_graphs()
        return self.salary_stats

    def compute_salary_mid_ranges(self):
        # Get list of min/max salary, i.e. for each job id we want its
        # corresponding min/max salary
        # TODO: return the min/max salaries in ascending order so you don't have
        # to sort `salary_mid_ranges` later on. You will have to modify the SQL
        # SELECT request in `select_all_min_salaries()` and `select_all_max_salaries()`
        # TODO: check if there are other cases where you could sort the data at the source
        # instead of doing it here after retrieving the data from the db
        job_ids_1, min_salaries = self.get_salaries("min")
        job_ids_2, max_salaries = self.get_salaries("max")
        self.salary_stats["min_salaries"] = min_salaries
        self.salary_stats["max_salaries"] = max_salaries
        # Sanity check on `job_ids_*`
        assert np.array_equal(job_ids_1, job_ids_2), \
            "The two returned job_ids don't match"
        self.salary_stats["job_ids_with_salary"] = job_ids_1
        del job_ids_2

        # Compute salary mid-range for each min-max interval
        salary_ranges = np.hstack((min_salaries, max_salaries))
        # TODO: check precision for `salary_mid_ranges`
        salary_mid_ranges = salary_ranges.mean(axis=1)
        self.salary_stats["job_id_to_salary_mid_ranges"] = dict(zip(self.salary_stats["job_ids_with_salary"],
                                                                   salary_mid_ranges))
        sorted_indices = np.argsort(salary_mid_ranges)
        self.salary_stats["sorted_salary_mid_ranges"] = salary_mid_ranges[sorted_indices]
        # Get job_id's associated with these global min and max salaries
        min_index = sorted_indices[0]
        max_index = sorted_indices[-1]
        self.salary_stats["min_job_id"] = self.salary_stats["job_ids_with_salary"][min_index]
        self.salary_stats["max_job_id"] = self.salary_stats["job_ids_with_salary"][max_index]

    def compute_global_stats(self):
        # TODO: compute_global_stats() can only be called if compute_salary_mid_ranges
        # was previously called because we make use of sorted_salary_mid_ranges
        # Compute salary mean across list of mid-range salaries. Thus, test that
        # sorted_salary_mid_ranges is already computed before going with the rest of
        # the computations
        global_mean_salary = self.salary_stats["sorted_salary_mid_ranges"].mean()
        # Precision to two decimals
        self.salary_stats["global_mean_salary"] = float(format(global_mean_salary, ".2f"))
        # Compute std across list of mid-range salaries
        global_std_salary = self.salary_stats["sorted_salary_mid_ranges"].std()
        # Precision to two decimals
        self.salary_stats["global_std_salary"] = float(format(global_std_salary, ".2f"))
        # Get min and max salaries across list of mid-range salaries
        # TODO: Is it better (i.e. less computations) to use min()/max() instead
        # of indices ([0] and [-1]) to retrieve the min/max of a numpy array?
        # TODO: It is here that we should validate the min/max
        self.salary_stats["global_min_salary"] = self.salary_stats["sorted_salary_mid_ranges"].min()
        self.salary_stats["global_max_salary"] = self.salary_stats["sorted_salary_mid_ranges"].max()

    def analyze_salary_by_topic(self, topic):
        # TODO: job_ids_with_salary is needed prior to calling this method which is computed
        # in compute_salary_mid_ranges(), see note at the beginning of compute_global_stats()
        # (same problem as here)
        try:
            select_method = self.__getattribute__("select_{}".format(topic))
            process_results_method = self.__getattribute__("process_{}_with_salaries".format(topic))
        except AttributeError:
            util.print_exception("AttributeError")
            return None
        # Get topic's rows that have a salary associated with
        results = select_method(tuple(self.salary_stats["job_ids_with_salary"]))
        # Process results to extract average mid-range salaries for each topic's rows
        process_results_method(results, topic)
        return 0

    def get_salaries(self, which="min"):
        valid_which = ["min", "max"]
        if which not in valid_which:
            print("ERROR: input entered '{}' for 'which' is invalid. Valid which is {}"
                  .format(which, valid_which))
            return None
        # TODO: Is it better to use __getattribute__ or a dict to choose the
        # right select method?
        select_method = self.__getattribute__("select_all_{}_salaries".format(which))
        salaries = select_method()
        salaries = np.array(salaries)
        # Extract the job ids
        job_ids = salaries[:, 0]
        # Extract the corresponding min salaries
        salaries = salaries[:, 1].astype(np.float64)
        # Reshape salaries arrays
        salaries = salaries.reshape((len(salaries), 1))
        return job_ids, salaries

    def select_all_min_salaries(self):
        """
        Returns all minimum salaries.
        A list of tuples is returned where a tuple is of the form (job_id, min_salary).

        :return: list of tuples of the form (job_id, min_salary)
        """
        sql = """SELECT job_id, value FROM job_salary WHERE name LIKE 'min%'"""
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def select_all_max_salaries(self):
        """
        Returns all maximum salaries.
        A list of tuples is returned where a tuple is of the form (job_id, max_salary).

        :return: list of tuples of the form (job_id, max_salary)
        """
        sql = """SELECT job_id, value FROM job_salary WHERE name LIKE 'max%'"""
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def select_locations(self, job_ids):
        # Sanity check on input
        # TODO: remove too much info in assert message, i.e. the name of the method
        assert type(job_ids) is tuple, "job_ids is not a tuple"
        sql = '''SELECT id, location FROM job_posts WHERE id IN ({})'''
        sql = self.build_sql_request(sql, len(job_ids))
        cur = self.conn.cursor()
        cur.execute(sql, job_ids)
        return cur.fetchall()

    def select_industries(self, job_ids):
        # Sanity check on input
        assert type(job_ids) is tuple, "job_ids is not a tuple"
        sql = '''SELECT job_id, value FROM job_overview WHERE job_id IN ({}) AND name="Industry"'''
        sql = self.build_sql_request(sql, len(job_ids))
        cur = self.conn.cursor()
        cur.execute(sql, job_ids)
        return cur.fetchall()

    def select_roles(self, job_ids):
        # Sanity check on input
        assert type(job_ids) is tuple, "job_ids is not a tuple"
        sql = '''SELECT job_id, value FROM job_overview WHERE job_id IN ({}) AND name="Role"'''
        sql = self.build_sql_request(sql, len(job_ids))
        cur = self.conn.cursor()
        cur.execute(sql, job_ids)
        return cur.fetchall()

    def select_tags(self, job_ids):
        # Sanity check on input
        assert type(job_ids) is tuple, "job_ids is not a tuple"
        sql = '''SELECT * FROM entries_tags WHERE id IN ({})'''
        sql = self.build_sql_request(sql, len(job_ids))
        cur = self.conn.cursor()
        cur.execute(sql, job_ids)
        return cur.fetchall()

    @staticmethod
    # TODO: rename it to add_sql_placeholders
    def build_sql_request(sql, n_items):
        placeholders = list(n_items * "?")
        placeholders = str(placeholders)
        placeholders = placeholders.replace("[", "")
        placeholders = placeholders.replace("]", "")
        placeholders = placeholders.replace("'", "")
        return sql.format(placeholders)

    def process_locations_with_salaries(self, locations, topic):
        # Temp dicts
        countries_to_salary = {}
        us_states_to_salary = {}
        # TODO: factorization of for loop with process_locations() and generate_map()
        # However, the other cases use (i, (location, count)) in the for loop, maybe
        # only the other two will be factorized
        for (i, (job_id, location)) in enumerate(locations):
            print("[{}/{}]".format((i + 1), len(locations)))
            # Check if valid location
            if not util.is_valid_location(location):
                # NOTE: We ignore the case where `location` is empty (None)
                # or refers to "No office location"
                # TODO: add logging
                continue
            # Sanitize input: this should be done at the source, i.e. in the
            # script that is loading data into the database
            elif ";" in location:
                ipdb.set_trace()
                # TODO: it should be done in a separate method since we are also
                # doing the same thing in process_locations()
                new_locations = location.split(";")
                for new_loc in new_locations:
                    locations.append((new_loc.strip(), 1))
                continue
            else:
                # Get country or US state from `location`
                last_part_loc = util.get_last_part_loc(location)
                # Sanity check
                assert last_part_loc is not None, "last_part_loc is None"
                # Is the location referring to a country or a US state?
                if self.is_a_us_state(last_part_loc):
                    # `location` refers to a US state
                    # Save last part of `location` along with some data for
                    # computing the average mid-range salary for the given US state
                    self.add_salary(us_states_to_salary, last_part_loc, job_id)
                    # Also since it is a US state, save 'United States' along with
                    # some data for computing the average mid-range salary for
                    # the given US country
                    # NOTE: in the job posts, the location for a US state is
                    # given without the country at the end, e.g. Fort Meade, MD
                    self.add_salary(countries_to_salary, "United States", job_id)
                else:
                    # `location` refers to a country
                    # Check for countries written in other languages, and keep
                    # only the english translation
                    # NOTE: sometimes, a country is not given in English e.g.
                    # Deutschland and Germany
                    # Save the location and along with some data for computing
                    # the average mid-range salary for the given country
                    transl_country = self.get_english_country_transl(last_part_loc)
                    assert transl_country in self.countries, \
                        "The country '{}' is not found".format(transl_country)
                    self.add_salary(countries_to_salary, transl_country, job_id)
        # For each dict, keep every fields, except "cumulative_sum" and build
        # a structured array out of the dicts
        # TODO: use a structured array like the following, so you can have columns
        # in a numpy array with different data types, and also it is easier to sort
        # this kind of array based on the name of a field
        # TODO: factorization: same as in process_topic_with_salaries and
        # countries and us_states same code
        temp_countries = [(k, v["average_mid_range_salary"], v["count"])
                          for k, v in countries_to_salary.items()]
        temp_us_states = [(k, v["average_mid_range_salary"], v["count"])
                          for k, v in us_states_to_salary.items()]
        # Fields (+data types) for the structured array
        # TODO: use the input `topic` to label the fields, call the method twice for
        # countries and us_states
        dtype = [("countries", "S20"), ("average_mid_range_salary", float), ("count", int)]
        temp_countries = np.array(temp_countries, dtype=dtype)
        dtype = [("us_states", "S10"), ("average_mid_range_salary", float), ("count", int)]
        temp_us_states = np.array(temp_us_states, dtype=dtype)
        # Sort each array based on the field 'average_mid_range_salary' and in
        # descending order of the given field
        temp_countries.sort(order="average_mid_range_salary")
        temp_us_states.sort(order="average_mid_range_salary")
        temp_countries = temp_countries[::-1]
        temp_us_states = temp_us_states[::-1]
        self.salary_stats["avg_mid_range_salaries_by_countries"] = temp_countries
        self.salary_stats["avg_mid_range_salaries_by_us_states"] = temp_us_states

    def process_industries_with_salaries(self, industries, topic):
        self.salary_stats["avg_mid_range_salaries_by_industries"] \
            = self.process_topic_with_salaries(industries, topic)

    def process_roles_with_salaries(self, roles, topic):
        self.salary_stats["avg_mid_range_salaries_by_roles"] \
            = self.process_topic_with_salaries(roles, topic)

    def process_tags_with_salaries(self, tags, topic):
        self.salary_stats["avg_mid_range_salaries_by_tags"]\
            = self.process_topic_with_salaries(tags, topic)

    def process_topic_with_salaries(self, input_data, topic_name):
        topic_to_salary = {}
        for job_id, name in input_data:
            self.add_salary(topic_to_salary, name, job_id)
        # Keep every fields, except "cumulative_sum" and build a structured array
        # out of the dict
        struct_arr = [(k, v["average_mid_range_salary"], v["count"])
                      for k, v in topic_to_salary.items()]
        # Fields (+data types) for the structured array
        # TOOD: adjust precision of float numbers
        # TODO: the length of the string field should be set in a config (for each topic?)
        dtype = [(topic_name, "S30"), ("average_mid_range_salary", float), ("count", int)]
        struct_arr = np.array(struct_arr, dtype=dtype)
        # Sort the array based on the field 'average_mid_range_salary' and in
        # descending order of the given field
        struct_arr.sort(order="average_mid_range_salary")
        struct_arr = struct_arr[::-1]
        return struct_arr

    def add_salary(self, dictionary, name, job_id):
        # TODO: job_id_to_salary_mid_ranges is needed prior caling this method
        # which is computed in compute_salary_mid_ranges(), see note at the
        # beginning of compute_global_stats() (same problem as here)
        dictionary.setdefault(name, {"average_mid_range_salary": 0,
                                     "cumulative_sum": 0,
                                     "count": 0})
        mid_range_salary = self.salary_stats["job_id_to_salary_mid_ranges"][job_id]
        dictionary[name]["count"] += 1  # update count
        cum_sum = dictionary[name]["cumulative_sum"]
        dictionary[name]["average_mid_range_salary"] \
            = (cum_sum + mid_range_salary) / dictionary[name]["count"]  # update average
        dictionary[name]["cumulative_sum"] += mid_range_salary  # update cumulative sum

    def generate_graphs(self):
        # Generate histogram of salary mid ranges vs number of job posts
        # TODO: you can use self.max_salary_threshold only after running
        # compute_global_stats() where the global max and min salaries are computed
        config = {"data": self.filter_mid_range_salaries(),
                  "bins": np.arange(0, self.max_salary_threshold, 10000),
                  "xlabel": self.config_ini["histogram_salary"]["xlabel"],
                  "ylabel": self.config_ini["histogram_salary"]["ylabel"],
                  "title": self.config_ini["histogram_salary"]["title"],
                  "grid_which": self.config_ini["histogram_salary"]["grid_which"],
                  "xaxis_major_mutiplelocator": self.config_ini["histogram_salary"]["xaxis_major_mutiplelocator"],
                  "xaxis_minor_mutiplelocator": self.config_ini["histogram_salary"]["xaxis_minor_mutiplelocator"],
                  "yaxis_major_mutiplelocator": self.config_ini["histogram_salary"]["yaxis_major_mutiplelocator"],
                  "yaxis_minor_mutiplelocator": self.config_ini["histogram_salary"]["yaxis_minor_mutiplelocator"]
                  }
        g_util.generate_histogram(config)

        # Generate scatter plots of number of job posts vs average mid-range salary
        # for each topic (e.g. locations, roles)
        # TODO: find another way than the use of config dict when preparing the different graphs
        # it is kind of confusing, maybe create a separate method that prepares the config dict
        # and returns it to be used ar argument to the graph generate method
        config = {"x": None,
                  "y": None,
                  "mode": self.config_ini["scatter_salary_topic"]["mode"],
                  "text": None,
                  "title": "",
                  "yaxis_tickformat": self.config_ini["scatter_salary_topic"]["yaxis_tickformat"]
                  }
        # Sanity check on topics and corresponding titles
        # TODO: uncomment when lcations is broken up into countries and us states
        """
        if len(self.salary_topics) != len(self.topic_to_titles):
            msg = "ERROR: Number of topics ({}) and titles ({}) don't match"\
                .format(len(self.salary_topics), len(self.topic_to_titles))
            exit_script(msg)
        """
        for topic, title in self.topic_to_titles.items():
            # TODO: add sanity check on eval(), i.e. make sure that the data array
            # exists before using it
            data = self.__getattribute__("avg_mid_range_salaries_by_{}".format(topic))
            # TODO: sanity check on data["average_mid_range_salary"], do the sanity
            # check within filter_data()
            indices = util.filter_data(data["average_mid_range_salary"],
                                       min_threshold=self.min_salary_threshold,
                                       max_threshold=self.max_salary_threshold)
            # TODO: sanity check on data keys
            # Filter the arrays to keep only the filtered data
            config["x"] = data["count"][indices]
            config["y"] = data["average_mid_range_salary"][indices]
            config["text"] = data[topic][indices]
            config["title"] = title
            g_util.generate_scatter_plot(config)
            # TODO: remove this waiting time between scatter plots browser-showing
            time.sleep(2)

    # TODO: it is better to return the indices of salaries to keep; thus use
    # the filter_data() function instead which can filter any kind of data, not only
    # salaries but also counts for instance
    def filter_mid_range_salaries(self):
        # TODO: change all *salary_mid_ranges* to *mid_range_salaries*,
        # even methods
        # Sanity check on the salary thresholds
        # TODO: we already have computed max/min with global_min_salary and
        # global_max_salary, use them instead of recomputing them
        max_salary = self.salary_stats["sorted_salary_mid_ranges"].max()
        min_salary = self.salary_stats["sorted_salary_mid_ranges"].min()
        # Sanity check on mid-range salary thresholds
        # TODO: these checks should be done when computing the global max and min in compute_global_stats()
        if not (max_salary >= self.min_salary_threshold >= min_salary):
            self.min_salary_threshold = min_salary
        if not (max_salary >= self.max_salary_threshold >= min_salary):
            self.max_salary_threshold = max_salary
        first_cond = (self.salary_stats["sorted_salary_mid_ranges"] >= self.min_salary_threshold)
        second_cond = (self.salary_stats["sorted_salary_mid_ranges"] <= self.max_salary_threshold)
        return self.salary_stats["sorted_salary_mid_ranges"][first_cond & second_cond]
