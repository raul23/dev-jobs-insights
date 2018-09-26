class Analyzer:
    def __init__(self, conn, db_session, main_config, logging_config, stats_names):
        # `stats_names` must be a list of stats names
        # Connection to SQLite db
        self.conn = conn
        self.db_session = db_session
        self.main_config = main_config
        self.logging_config = logging_config
        # Stats to compute
        self.stats_names = stats_names
        self.stats = {}
        self.reset_stats()

    def _generate_bar_chart(self, sorted_stats_count, bar_chart_config):
        raise NotImplementedError

    """
    def _generate_pie_chart(self, sorted_stats_count, bar_chart_config):
        raise NotImplementedError

    def _generate_histogram(self, sorted_stats_count, bar_chart_config):
        raise NotImplementedError

    def _generate_scatter_plot(self, sorted_stats_count, bar_chart_config):
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

    def run_analysis(self):
        raise NotImplementedError
