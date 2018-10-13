import os
# Third-party modules
import ipdb
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
# Own modules
# from analyzers.companies_analyzer import CompaniesAnalyzer
from analyzers.industries_analyzer import IndustriesAnalyzer
from analyzers.job_benefits_analyzer import JobBenefitsAnalyzer
from analyzers.job_locations_analyzer import JobLocationsAnalyzer
# from analyzers.job_posts_analyzer import JobPostsAnalyzer
from analyzers.job_salaries_analyzer import JobSalariesAnalyzer
from analyzers.roles_analyzer import RolesAnalyzer
from analyzers.skills_analyzer import SkillsAnalyzer
from tables import Base
from utilities.genutils import create_timestamped_directory, read_yaml_config
from utilities.script_boilerplate import LoggingBoilerplate


class JobDataAnalyzer:
    def __init__(self, main_cfg_path, logging_cfg):
        self.main_cfg_path = main_cfg_path
        self.logging_cfg = logging_cfg
        sb = LoggingBoilerplate(
            module_name=__name__,
            module_file=__file__,
            cwd=os.getcwd(),
            logging_cfg=logging_cfg)
        self.logger = sb.get_logger()
        self.main_cfg = self._load_main_cfg()
        # Create saving directory
        # Folder name will begin with the date+time
        try:
            self.logger.info("Creating the report directory ...")
            report_dirpath = create_timestamped_directory(
                "report", os.path.expanduser(self.main_cfg['saving_dirpath']))
        except PermissionError as e:
            self.logger.critical(e)
            self.logger.error("The report folder couldn't be created. "
                              "Program will exit.")
            raise SystemExit
        else:
            self.logger.info("Directory '{}' created!".format(report_dirpath))
        # Update the main config with the newly create saving directory
        self.main_cfg.update({'saving_dirpath': report_dirpath})
        self.types_of_analyses = self._get_analyses()
        # TODO: implement db connection with SQLite
        # Db connection to be used with SQLite
        # self.conn = gu.connect_db("")
        self.conn = None
        self.db_session = self._get_db_session()

    def _get_analyses(self):
        return [k for k, v in self.main_cfg.items()
                if isinstance(v, dict) and self.main_cfg[k].get('run_analysis')]

    def _load_main_cfg(self):
        # Read YAML configuration file
        try:
            self.logger.info("Loading the YAML configuration file '{}'".format(
                self.main_cfg_path))
            config_dict = read_yaml_config(self.main_cfg_path)
        except OSError as e:
            self.logger.critical(e)
            raise SystemExit("Configuration file '{}' couldn't be read. Program "
                             "will exit.".format(self.main_cfg_path))
        else:
            self.logger.debug("Successfully loaded the YAML configuration file")
            return config_dict

    def _get_db_session(self):
        # SQLAlchemy database setup
        db_url = self.main_cfg['db_url']
        db_url['database'] = os.path.expanduser(db_url['database'])
        self.logger.info("Database setup of {}".format(db_url['database']))
        engine = create_engine(URL(**db_url))
        Base.metadata.bind = engine
        # Setup database session
        DBSession = sessionmaker(bind=engine)
        db_session = DBSession()
        return db_session

    def run_analysis(self):
        for analysis_type in self.types_of_analyses:
            try:
                self.logger.info(
                    "Starting the '{}' analysis".format(analysis_type))
                analyze_method = self.__getattribute__(
                    "_analyze_{}".format(analysis_type))
                analyze_method(analysis_type)
            except (AttributeError, FileNotFoundError) as e:
                self.logger.exception(e)
                self.logger.error(
                    "The '{}' analysis will be skipped".format(analysis_type))
            else:
                self.logger.info(
                    "End of the '{}' analysis".format(analysis_type))

    def _analyze_companies(self, analysis_type):
        """
        Analysis of companies

        :return:
        """
        # ca = CompaniesAnalyzer(self.conn, self.db_session, self.config)
        # ca.run_analysis()
        raise NotImplementedError

    def _analyze_industries(self, analysis_type):
        """
        Analysis of industries

        :return:
        """
        ia = IndustriesAnalyzer(analysis_type,
                                self.conn,
                                self.db_session,
                                self.main_cfg,
                                self.logging_cfg)
        ia.run_analysis()

    def _analyze_job_benefits(self, analysis_type):
        """
        Analysis of job benefits which consist in ...

        :return:
        """
        jla = JobBenefitsAnalyzer(analysis_type,
                                  self.conn,
                                  self.db_session,
                                  self.main_cfg,
                                  self.logging_cfg)
        jla.run_analysis()

    def _analyze_job_locations(self, analysis_type):
        """
        Analysis of job locations which consist in ...

        :return:
        """
        jla = JobLocationsAnalyzer(analysis_type,
                                   self.conn,
                                   self.db_session,
                                   self.main_cfg,
                                   self.logging_cfg)
        jla.run_analysis()

    def _analyze_job_posts(self, analysis_type):
        """
        Analysis of job posts which consist in ...

        :return:
        """
        # jla = JobPostsAnalyzer(self.conn, self.db_session, self.config)
        # jla.run_analysis()
        raise NotImplementedError

    def _analyze_job_salaries(self, analysis_type):
        jsa = JobSalariesAnalyzer(analysis_type,
                                  self.conn,
                                  self.db_session,
                                  self.main_cfg,
                                  self.logging_cfg)
        jsa.run_analysis()

    def _analyze_roles(self,analysis_type):
        """
        Analysis of roles

        :return:
        """
        ra = RolesAnalyzer(analysis_type,
                           self.conn,
                           self.db_session,
                           self.main_cfg,
                           self.logging_cfg)
        ra.run_analysis()

    def _analyze_skills(self, analysis_type):
        """
        Analysis of skills (i.e. technologies such as java, python) which
        consist in ...

        :return:
        """
        sa = SkillsAnalyzer(analysis_type,
                            self.conn,
                            self.db_session,
                            self.main_cfg,
                            self.logging_cfg)
        sa.run_analysis()

    def generate_report(self):
        raise NotImplementedError
