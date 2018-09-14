class ScrapingSession:
    def __init__(self, job_post_id, url, data):
        self.job_post_id = job_post_id
        self.url = url
        self.data = data
        self.bs_obj = None

    def reset(self):
        for k, v in self.__dict__.items():
            self.__setattr__(k, None)
