class ScrapingSession:
    def __init__(self, job_id, url, data):
        self.job_id = job_id
        self.url = url
        self.data = data
        self.bs_obj = None

    def reset(self):
        self.job_id = None
        self.url = None
        self.data = None
        self.bs_obj = None