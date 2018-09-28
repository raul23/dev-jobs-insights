import os
import sys
# Third-party modules
import numpy as np
# Own modules
# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
from utility.script_boilerplate import LoggingBoilerplate


class Analyzer:
    def __init__(self, conn, db_session, main_cfg, logging_cfg, stats_names,
                 module_name, module_file, cwd):
        # `stats_names` must be a list of stats names
        # Connection to SQLite db
        self.conn = conn
        self.db_session = db_session
        self.main_cfg = main_cfg
        self.logging_cfg = logging_cfg
        # Stats to compute
        self.stats_names = stats_names
        self.stats = {}
        self.reset_stats()
        sb = LoggingBoilerplate(module_name,
                                module_file,
                                cwd,
                                logging_cfg)
        self.logger = sb.get_logger()

    # TODO: add decorator to call `reset_stats` at first
    def run_analysis(self):
        raise NotImplementedError

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

    """
    def _generate_pie_chart(self, sorted_topic_count, pie_chart_config):
        raise NotImplementedError

    def _generate_histogram(self, sorted_topic_count, hist_config):
        raise NotImplementedError

    def _generate_scatter_plot(self, x, y, text, scatter_config):
        raise NotImplementedError
    """

    @staticmethod
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