import ipdb
from data_analysis import job_data_analyzer as jda


if __name__ == '__main__':
    data_analyzer = jda.JobDataAnalyzer(settings_filename="./data_analysis/config.ini")
    data_analyzer.run_analysis()
