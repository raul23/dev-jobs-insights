import os
import sys
try:
    import analyzers
except ImportError:
    sys.path.insert(0, os.path.expanduser("~/PycharmProjects/job-insights/dev_jobs_insights/data_analysis"))
    import analyzers
