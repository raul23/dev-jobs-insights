import os
import sys
# Third-party modules
import ipdb
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
# Own modules
# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser(
    "~/PycharmProjects/github_projects/dev_jobs_insights/data_analysis"))
from analyzers.companies_analyzer import CompaniesAnalyzer
from analyzers.industries_analyzer import IndustriesAnalyzer
from analyzers.job_benefits_analyzer import JobBenefitsAnalyzer
from analyzers.job_locations_analyzer import JobLocationsAnalyzer
from analyzers.job_posts_analyzer import JobPostsAnalyzer
from analyzers.job_salaries_analyzer import JobSalariesAnalyzer
from analyzers.roles_analyzer import RolesAnalyzer
from analyzers.skills_analyzer import SkillsAnalyzer
# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
from utility.genutil import read_yaml_config
from utility.script_boilerplate import LoggingBoilerplate
sys.path.insert(0, os.path.expanduser(
    "~/PycharmProjects/github_projects/dev_jobs_insights/database"))
from tables import Base


class JobDataAnalyzer:
    def __init__(self, main_config_path, logging_config_path,
                 use_default_colors=False, use_pycharm_colors=False):
        self.main_config_path = main_config_path
        self.logging_config_path = logging_config_path
        sb = LoggingBoilerplate(
            module_name=__name__,
            module_file=__file__,
            cwd=os.getcwd(),
            logging_config=logging_config_path,
            use_default_colors=use_default_colors,
            use_pycharm_colors=use_pycharm_colors)
        self.logger = sb.get_logger()
        self.config = self._load_main_config()
        self.types_of_analysis = self._get_analyses()
        # TODO: implement db connection with SQLite
        # Db connection to be used with SQLite
        # self.conn = gu.connect_db("")
        self.conn = None
        self.db_session = self._get_db_session()

    def _get_analyses(self):
        return [k for k, v in self.config["analysis_types"].items() if v]

    def _load_main_config(self):
        # Read YAML configuration file
        try:
            self.logger.info("Loading the YAML configuration file '{}'".format(
                self.main_config_path))
            config_dict = read_yaml_config(self.main_config_path)
        except OSError as e:
            self.logger.exception(e)
            raise SystemExit("Configuration file '{}' couldn't be read. Program "
                             "will exit.".format(self.main_config_path))
        else:
            self.logger.debug("Successfully loaded the YAML configuration file")
            return config_dict

    def _get_db_session(self):
        ipdb.set_trace()
        # SQLAlchemy database setup
        self.logger.info("Database setup")
        db_url = self.config['db_url']
        db_url['database'] = os.path.expanduser(db_url['database'])
        engine = create_engine(URL(**db_url))
        Base.metadata.bind = engine
        # Setup database session
        DBSession = sessionmaker(bind=engine)
        db_session = DBSession()
        return db_session

    def run_analysis(self):
        for analysis_type in self.types_of_analysis:
            try:
                analyze_method = self.__getattribute__(
                    "_analyze_{}".format(analysis_type))
                analyze_method()
            except (AttributeError, FileNotFoundError) as e:
                self.logger.exception(e)
                self.logger.error("The analysis type '{}' will be skipped")

    def _analyze_companies(self):
        """
        Analysis of companies

        :return:
        """
        ca = CompaniesAnalyzer(self.conn, self.db_session, self.config)
        ca.run_analysis()

    def _analyze_industries(self):
        """
        Analysis of industries

        :return:
        """
        ia = IndustriesAnalyzer(self.conn, self.db_session, self.config)
        ia.run_analysis()

    def _analyze_job_benefits(self):
        """
        Analysis of job benefits which consist in ...

        :return:
        """
        jla = JobBenefitsAnalyzer(self.conn, self.db_session, self.config)
        jla.run_analysis()

    def _analyze_job_locations(self):
        """
        Analysis of job locations which consist in ...

        :return:
        """
        jla = JobLocationsAnalyzer(self.conn, self.db_session, self.config)
        jla.run_analysis()

    def _analyze_job_posts(self):
        """
        Analysis of job posts which consist in ...

        :return:
        """
        jla = JobPostsAnalyzer(self.conn, self.db_session, self.config)
        jla.run_analysis()

    def _analyze_job_salaries(self):
        jsa = JobSalariesAnalyzer(self.conn, self.db_session, self.config)
        jsa.run_analysis()

    def _analyze_roles(self):
        """
        Analysis of roles

        :return:
        """
        ra = RolesAnalyzer(self.conn, self.db_session, self.config)
        ra.run_analysis()

    def _analyze_skills(self):
        """
        Analysis of skills (i.e. technologies such as java, python) which consist in ...

        :return:
        """
        sa = SkillsAnalyzer(self.conn, self.db_session, self.config)
        sa.run_analysis()

    def generate_report(self):
        # TODO: complete method
        raise NotImplementedError


if __name__ == '__main__':
    # JobDataAnalyzer(config_path="config.yaml").run_analysis()
    pass
