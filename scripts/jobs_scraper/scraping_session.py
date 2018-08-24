class ScrapingSession:
    def __init__(self, job_id):
        self.job_id = job_id
        self.url = None
        self.bs_obj = None
        self.data = None

    def reset(self):
        self.job_id = None
        self.url = None
        self.bs_obj = None
        self.data = None