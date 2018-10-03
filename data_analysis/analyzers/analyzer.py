import os
# Third-party modules
import ipdb
import numpy as np
from pycountry_convert import country_alpha2_to_continent_code, map_countries
# Own modules
from utility.genutil import convert_list_to_str, dump_pickle, load_json, load_pickle
from utility.logging_boilerplate import LoggingBoilerplate


class Analyzer:
    # `stats_names` must be a list of names of stats to compute
    def __init__(self, analysis_type, conn, db_session, main_cfg, logging_cfg,
                 stats_names, module_name, module_file, cwd):
        self.analysis_type = analysis_type
        # Connection to SQLite db
        self.conn = conn
        self.db_session = db_session
        self.main_cfg = main_cfg
        self.logging_cfg = logging_cfg
        # Stats to compute
        self.stats_names = stats_names
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

    # Generate HORIZONTAL bar
    # `sorted_topic_count` is a numpy array and has two columns: labels and counts
    # Each row of the input array tells how many counts they are of the given
    # label.
    def _generate_barh_chart(self, barh_type, sorted_topic_count, barh_chart_cfg):
        if not barh_chart_cfg['display_graph'] and not barh_chart_cfg['save_graph']:
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
            self.logger.info("Loading the dictionary from '{}'".format(filepath))
            dict_ = load_pickle(filepath)
        except FileNotFoundError as e:
            self.logger.exception(e)
            self.logger.warning("The dictionary '{}' will be initialized to {}".format(
                filepath, dict_))
        else:
            self.logger.debug("Dictionary '{}' loaded!".format(filepath))
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
        self.stats = dict(zip(self.stats_names, [{}] * len(self.stats_names)))