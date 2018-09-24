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

    def _generate_graphs(self):
        raise NotImplementedError

    @staticmethod
    def _shrink_labels(labels, max_length):
        return [label[:max_length]
                if len(label) < max_length else '{}...'.format(label[:max_length])
                for label in labels]

    def reset_stats(self):
        self.stats = dict(zip(self.stats_names, [None] * len(self.stats_names)))

    def run_analysis(self):
        raise NotImplementedError
