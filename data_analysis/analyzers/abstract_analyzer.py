class AbstractAnalyzer:
    def __init__(self, conn, config_ini, stats_names):
        # TODO: explain that stats_names must be a list of stats names
        # Sanity check on list of stats names
        assert type(stats_names) is list, "stats_names must be a list"
        self.conn = conn
        self.config_ini = config_ini
        # Stats to compute
        self.stats_names = stats_names
        self.stats = {}
        self.reset_stats()

    def reset_stats(self):
        self.stats = dict(zip(self.stats_names, [None] * len(self.stats_names)))

    def run_analysis(self):
        raise NotImplementedError

    def generate_graphs(self):
        raise NotImplementedError