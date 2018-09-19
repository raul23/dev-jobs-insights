import logging
import os
# Own modules
from analyzers.companies_analyzer import CompaniesAnalyzer
from analyzers.industries_analyzer import IndustriesAnalyzer
from analyzers.job_benefits_analyzer import JobBenefitsAnalyzer
from analyzers.job_locations_analyzer import JobLocationsAnalyzer
from analyzers.job_posts_analyzer import JobPostsAnalyzer
from analyzers.job_salaries_analyzer import JobSalariesAnalyzer
from analyzers.roles_analyzer import RolesAnalyzer
from analyzers.skills_analyzer import SkillsAnalyzer

# Get the loggers
# TODO: can this be done in a genutil.function (i.e. outside of this file)?
if __name__ == '__main__':
    # When run as a script
    flogger = logging.getLogger('{}.{}.file'.format(
        os.path.basename(os.getcwd()), os.path.splitext(__file__)[0]))
    clogger = logging.getLogger('{}.{}.console'.format(
        os.path.basename(os.getcwd()), os.path.splitext(__file__)[0]))
else:
    # When imported as a module
    # TODO: test this part when imported as a module
    flogger = logging.getLogger('{}.{}.file'.format(
        os.path.basename(os.path.dirname(__file__)), __name__))
    clogger = logging.getLogger('{}.{}.console'.format(
        os.path.basename(os.path.dirname(__file__)), __name__))


class JobDataAnalyzer:
    def __init__(self, config_path):
        # Read the YAML configuration file
        self.config = None
        self.types_of_analysis = None
        self.conn = None

    def get_analyses(self):
        return [k for k, v in self.config["analysis_types"].items() if v]

    def run_analysis(self):
        with self.conn:
            for analysis_type in self.types_of_analysis:
                try:
                    analyze_method = self.__getattribute__(analysis_type)
                    analyze_method()
                except (AttributeError, FileNotFoundError) as e:
                    # print("ERROR: {} will be skipped because of an {}".format(analysis_type, err_name))
                    pass

    def analyze_companies(self):
        """
        Analysis of companies

        :return:
        """
        ca = CompaniesAnalyzer(self.conn, self.config)
        ca.run_analysis()

    def analyze_industries(self):
        """
        Analysis of industries

        :return:
        """
        ia = IndustriesAnalyzer(self.conn, self.config)
        ia.run_analysis()

    def analyze_job_benefits(self):
        """
        Analysis of job benefits which consist in ...

        :return:
        """
        jla = JobBenefitsAnalyzer(self.conn, self.config)
        jla.run_analysis()

    def analyze_job_locations(self):
        """
        Analysis of job locations which consist in ...

        :return:
        """
        jla = JobLocationsAnalyzer(self.conn, self.config)
        jla.run_analysis()

    def analyze_job_posts(self):
        """
        Analysis of job posts which consist in ...

        :return:
        """
        jla = JobPostsAnalyzer(self.conn, self.config)
        jla.run_analysis()

    def analyze_job_salaries(self):
        jsa = JobSalariesAnalyzer(self.conn, self.config)
        jsa.run_analysis()

    def analyze_roles(self):
        """
        Analysis of roles

        :return:
        """
        ra = RolesAnalyzer(self.conn, self.config)
        ra.run_analysis()

    def analyze_skills(self):
        """
        Analysis of skills (i.e. technologies such as java, python) which consist in ...

        :return:
        """
        sa = SkillsAnalyzer(self.conn, self.config)
        sa.run_analysis()

    def generate_report(self):
        # TODO: complete method
        raise NotImplementedError
