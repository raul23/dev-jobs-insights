import data_analysis.job_analyzer as ja


if __name__ == '__main__':
    data_analyzer = ja.JobDataAnalyzer(settings_path="config.ini")
    data_analyzer.run_analysis()
