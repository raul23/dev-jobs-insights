import os
import sys
try:
    import job_analyzer as ja
except ImportError:
    sys.path.insert(0, os.path.expanduser("~/PycharmProjects/job-insights/dev_jobs_insights"))
    from data_analysis import job_analyzer as ja


if __name__ == '__main__':
    data_analyzer = ja.JobDataAnalyzer(settings_path="config.ini")
    data_analyzer.run_analysis()
