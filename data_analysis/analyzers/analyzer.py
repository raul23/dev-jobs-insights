import os
# Third-party modules
import ipdb
import numpy as np
from pycountry_convert import country_alpha2_to_continent_code, \
    country_alpha2_to_country_name, map_countries
# Own modules
from utility.genutil import convert_list_to_str, dump_json, dump_pickle, \
    load_json, load_pickle
from utility.logging_boilerplate import LoggingBoilerplate


class Analyzer:
    # `stats_names` must be a list of names of stats to compute
    def __init__(self, analysis_type, conn, db_session, main_cfg, logging_cfg,
                 stats_names, report, module_name, module_file, cwd):
        self.analysis_type = analysis_type
        # Connection to SQLite db
        self.conn = conn
        self.db_session = db_session
        self.main_cfg = main_cfg
        self.logging_cfg = logging_cfg
        # Stats to compute
        self.stats_names = stats_names
        self.report = report
        self.stats = {}
        self.reset_stats()
        lb = LoggingBoilerplate(module_name,
                                module_file,
                                cwd,
                                logging_cfg)
        self.logger = lb.get_logger()

    # TODO: add decorator to call `reset_stats` at first
    def run_analysis(self):
        raise NotImplementedError

    # Graph: barh chart and scatter plot (not map figures)
    def _update_graph_report(self, graph_report, items, job_post_ids, skipped=None,
                             duplicates=None):
        # Sanity check on input data
        assert isinstance(items, list), \
            "wrong type for `items`: '{}'. `items` must of type list".format(
             type(items))
        assert type(job_post_ids) in [list, set], \
            "wrong type for `job_post_ids`: `{}`. `job_post_ids` must be of " \
            "type `list`".format(type(job_post_ids))
        assert isinstance(job_post_ids[0], int), \
            "wrong type for `job_post_id`s: `{}`. `job_post_id`s must be of " \
            "type `int`".format(type(job_post_ids[0]))
        # Update report for barh
        self.logger.info("Updating graph report")
        # Update `items` field
        graph_report['items']['data'] = items
        graph_report['items']['number_of_items'] = len(items)
        labels = graph_report['items']['labels']
        idx = [i for i, l in enumerate(labels) if l.startswith('count_')]
        if idx and (isinstance(items[0], list) or isinstance(items[0], tuple)):
            graph_report['items']['sum_of_counts'] = sum([i[idx[0]] for i in items])
        # Update `job_post_ids` field
        graph_report['job_post_ids'] = job_post_ids
        # Update `number_of_job_posts` field
        graph_report['number_of_job_posts'] = len(job_post_ids)
        # Update `published_dates` field
        min_date, max_date = self._get_min_max_published_dates(job_post_ids)
        graph_report['published_dates'] = [min_date, max_date]
        if skipped:
            graph_report['skipped'] = skipped
        if duplicates:
            graph_report['duplicates'] = duplicates

    def _dump_pickle(self, filepath, data):
        self.logger.info("Saving path: {}".format(filepath))
        try:
            dump_pickle(filepath, data)
        except FileNotFoundError as e:
            self.logger.exception(e)
            self.logger.error("The file '{}' couldn't be saved".format(filepath))
            raise FileExistsError(e)
        else:
            self.logger.debug("Saved!")

    def _convert_countries_names(self, list_countries, converter):
        new_list_countries = []
        for job_post_id, country in list_countries:
            assert country is not None, "Country shouldn't be 'None'"
            try:
                self.logger.debug(
                    "Converting '{}' to its fullname".format(country))
                country_fullname = converter(country)
            except KeyError as e:
                self.logger.exception(e)
                self.logger.critical(
                    "No fullname found for '{}'".format(country))
                self.logger.warning(
                    "The country '{}' will be skipped".format(country))
                continue
            else:
                if country_fullname is None:
                    self.logger.critical(
                        "No fullname found for '{}'".format(country))
                    self.logger.warning(
                        "The country '{}' will be skipped".format(country))
                    continue
                self.logger.debug("Converted '{}' to '{}'".format(
                    country, country_fullname))
                country = country_fullname
            new_list_countries.append((job_post_id, country))
        return new_list_countries

    # `list_items` is a list of tuples where tuple[0] is the `job_post_id` and
    # tuple[1] is the item's short name
    # `converter` is a method that converts the item's short name to its full
    # name
    def _count_items(self, list_items, converter=None, ignore_none=False,
                     ignore_duplicates=False):
        items_data = {}
        set_ids = set()
        duplicates = []
        skipped = []
        counts = 0
        for i, (job_post_id, item) in enumerate(list_items, start=1):
            self.logger.debug("#{} job_post_id='{}', item '{}'".format(
                              i, job_post_id, item))
            if item is None and ignore_none:
                self.logger.warning("The item 'None' will be skipped")
                skipped.append((job_post_id, item))
                continue
            if converter is not None and item is not None:
                try:
                    self.logger.debug(
                        "Converting '{}' to its fullname".format(item))
                    item_fullname = converter(item)
                except KeyError as e:
                    self.logger.exception(e)
                    self.logger.critical(
                        "No fullname found for '{}'".format(item))
                    self.logger.warning(
                        "The item '{}' will be skipped".format(item))
                    skipped.append((job_post_id, item))
                    continue
                else:
                    if item_fullname is None:
                        self.logger.critical(
                            "No fullname found for '{}'".format(item))
                        self.logger.warning(
                            "The item '{}' will be skipped".format(item))
                        skipped.append((job_post_id, item))
                        continue
                    self.logger.debug(
                        "Converted '{}' to '{}'".format(item, item_fullname))
                    item = item_fullname
            items_data.setdefault(item, [0, []])
            if not ignore_duplicates and job_post_id in items_data[item][1]:
                self.logger.warning(
                    "Found duplicate item '{}' for job_post_id '{}'".format(
                        item, job_post_id))
                self.logger.warning(
                    "Duplicate item '{}' will not be counted since it was "
                    "already counted once for job_post_id '{}'".format(
                        item, job_post_id))
                duplicates.append((job_post_id, item))
            else:
                # TODO: remove hack!!
                if job_post_id == 203389 and 203389 in items_data[item][1]:
                    self.logger.warning("Poland was already added for 203389")
                    continue
                if job_post_id in items_data[item][1]:
                    self.logger.warning(
                        "Found duplicate item '{}' for job_post_id '{}'".format(
                            item, job_post_id))
                    duplicates.append((job_post_id, item))
                items_data[item][0] += 1
                items_data[item][1].append(job_post_id)
                set_ids.add(job_post_id)
                counts += 1
                self.logger.debug("Item '{}' added".format(item))
        results = sorted([(k, v[0]) for k, v in items_data.items()],
                         key=lambda tup: tup[1],
                         reverse=True)
        return results, list(set_ids), duplicates, skipped

    # Generate HORIZONTAL bar
    # `sorted_topic_count` is a numpy array and has two columns: labels and counts
    # Each row of the input array tells how many counts they are of the given
    # label.
    def _generate_barh_chart(self, barh_type, sorted_topic_count, barh_chart_cfg):
        if not barh_chart_cfg['display_graph'] and \
                not barh_chart_cfg['save_graph']:
            self.logger.warning("The bar chart '{}' is disabled for the '{}' "
                                "analysis".format(barh_type, self.analysis_type))
            return 1
        # Sanity check on input data
        assert isinstance(sorted_topic_count, type(np.array([]))), \
            "wrong type on input array 'sorted_topic_count'"
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        # TODO: add spinner when loading this module
        self.logger.info("loading module 'utility.graphutil' ...")
        from utility.graphutil import draw_barh_chart
        self.logger.debug("finished loading module 'utility.graphutil'")
        self.logger.info("Generating bar chart '{}' ...".format(barh_type))
        topk = barh_chart_cfg['topk']
        shorter_labels = self._shrink_labels(
            labels=sorted_topic_count[:topk, 0],
            max_length=barh_chart_cfg['max_label_length'])
        draw_barh_chart(
            x=sorted_topic_count[:topk, 1].astype(np.int32),
            y=np.array(shorter_labels),
            title=barh_chart_cfg['title'].format(topk),
            xlabel=barh_chart_cfg['xlabel'],
            add_text_right_bar=barh_chart_cfg['add_text_right_bar'],
            color=barh_chart_cfg['color'],
            fig_width=barh_chart_cfg['fig_width'],
            fig_height=barh_chart_cfg['fig_height'],
            grid_which=barh_chart_cfg['grid_which'],
            display_graph=barh_chart_cfg['display_graph'],
            save_graph=barh_chart_cfg['save_graph'],
            fname=os.path.join(self.main_cfg['saving_dirpath'],
                               barh_chart_cfg['fname']))
        return 0

    @staticmethod
    def _get_country_name(country_alpha2):
        try:
            country_name = country_alpha2_to_country_name(country_alpha2)
        except KeyError as e:
            raise KeyError(e)
        return country_name

    # Useful inside SQL expressions
    def _get_european_countries_as_str(self):
        european_countries = set()
        for _, values in map_countries().items():
            try:
                continent = country_alpha2_to_continent_code(values['alpha_2'])
                if continent == "EU":
                    european_countries.add(values['alpha_2'])
            except KeyError as e:
                # Possible cause: Invalid alpha_2, e.g. could be Antarctica which
                # is not associated to a continent code
                # self.logger.exception(e)
                self.logger.debug(
                    "No continent code for '{}'".format(values['alpha_2']))
                continue
        return convert_list_to_str(european_countries)

    # `job_post_ids` is a list of ids
    def _get_min_max_published_dates(self, job_post_ids):
        job_post_ids = convert_list_to_str(job_post_ids)
        sql = "SELECT date_posted FROM job_posts WHERE id in ({}) ORDER BY " \
              "date_posted ASC".format(job_post_ids)
        results = self.db_session.execute(sql).fetchall()
        return results[0][0], results[-1][0]

    def _load_json(self, filepath, encoding='utf8'):
        try:
            self.logger.info("Loading {}".format(filepath))
            data = load_json(filepath, encoding)
        except FileNotFoundError as e:
            self.logger.exception(e)
            self.logger.error("The file '{}' couldn't be saved".format(filepath))
            raise FileExistsError(e)
        else:
            self.logger.debug("Saved!")
            return data

    # If the data to be loaded is a dictionary
    def _load_dict_from_pickle(self, filepath, default=None):
        if default is None:
            dict_ = {}
        else:
            dict_ = default
        try:
            self.logger.info("Loading the dictionary '{}'".format(filepath))
            dict_ = load_pickle(filepath)
        except FileNotFoundError as e:
            self.logger.exception(e)
            self.logger.warning("The dictionary will be initialized to {}".format(
                filepath, dict_))
        else:
            self.logger.debug("Dictionary loaded!".format(filepath))
        finally:
            return dict_

    """
    def _generate_pie_chart(self, sorted_topic_count, pie_chart_config):
        raise NotImplementedError

    def _generate_histogram(self, sorted_topic_count, hist_config):
        raise NotImplementedError

    def _generate_scatter_plot(self, x, y, text, scatter_config):
        raise NotImplementedError
    """

    def _save_report(self, filename):
        filepath = os.path.join(self.main_cfg['saving_dirpath'], filename)
        try:
            self.logger.info("The report will be saved in '{}'".format(filepath))
            dump_json(filepath, self.report, sort_keys=False)
        except OSError as e:
            self.logger.exception(e)
            self.logger.error("The report couldn't be saved")
        else:
            self.logger.info("Report saved!")

    @staticmethod
    # `labels` is the input list of labels and a list is returned with shorter
    # labels
    def _shrink_labels(labels, max_length):
        new_labels = []
        for l in labels:
            # The column (e.g. region in `job_locations`) could be `None`, we
            # can't do `len(None)` since it is not a string. Thus this special
            # case.
            if l is None:
                new_labels.append('None')
            elif len(l) < max_length:
                new_labels.append(l[:max_length])
            else:
                new_labels.append('{}...'.format(l[:max_length]))
        return new_labels

    def reset_stats(self):
        self.stats = dict(zip(self.stats_names, [None] * len(self.stats_names)))
