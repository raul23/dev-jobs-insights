class Analyzer:
    def __init__(self, conn, db_session, config, stats_names):
        # `stats_names` must be a list of stats names
        # Connection to SQLite db
        self.conn = conn
        self.db_session = db_session
        self.config = config
        # Stats to compute
        self.stats_names = stats_names
        self.stats = {}
        self.reset_stats()

    def reset_stats(self):
        self.stats = dict(zip(self.stats_names, [None] * len(self.stats_names)))

    def run_analysis(self):
        raise NotImplementedError

    def _generate_graphs(self):
        raise NotImplementedError
