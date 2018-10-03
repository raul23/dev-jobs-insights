import functools
import os
# Third-party modules
import ipdb
# Own modules
from tables import (Company, JobPost, ExperienceLevel, Industry, JobBenefit,
                    JobSalary, JobLocation, Role, Skill)
from tables import ValueOverrideError
from utility.logging_boilerplate import LoggingBoilerplate
from utility.logging_wrapper import LoggingWrapper


class DuplicateRecordError(Exception):
    """Raised when a duplicate record (i.e. table instance) is being added to a
    list of records."""


class NoOfficeLocationFoundError(Exception):
    """Raised when a location is being added to a list of records that already
    has a 'No office location' as a record. No more locations should be added if
    'No office location' is indicated in the job post."""


class JobData:
    def __init__(self, job_post_id, logger):
        if isinstance(logger, dict):
            sb = LoggingBoilerplate(__name__,
                                    __file__,
                                    os.getcwd(),
                                    logger)
            self.logger = sb.get_logger()
        else:
            # Sanity check on `logger`
            assert isinstance(logger, LoggingWrapper), \
                "`logger` must be of type `LoggingWrapper`"
            self.logger = logger
        self.job_post_id = job_post_id
        self.company = Company()
        self.job_post = JobPost()
        self.job_post.id = self.job_post_id
        self.experience_levels = []
        self.industries = []
        self.job_benefits = []
        self.job_locations = []
        self.job_salaries = []
        self.roles = []
        self.skills = []

    def __getstate__(self):
        d = dict(self.__dict__)
        del d['logger']
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)

    @staticmethod
    def get_logging_info():
        return __name__, __file__, os.getcwd()

    def _check_kwargs_all_none(func):
        @functools.wraps(func)
        def wrapper_check_kwargs_all_none(self, *args, **kwargs):
            set_values = set(kwargs.values())
            if len(set_values) == 1 and set_values.pop() is None:
                self.logger.debug("[{}] The table '{}' is all empty".format(
                    self.job_post_id, args[0].__tablename__))
            else:
                func(self, *args, **kwargs)

        return wrapper_check_kwargs_all_none

    @_check_kwargs_all_none
    # IMPORTANT: this method along with other methods starting with `set_` are
    # called only once per table instance (aka record)
    def _setup_record(self, rec, table_records, **kwargs):
        for key, value in kwargs.items():
            try:
                self.logger.debug(
                    "Setting the column '{}' from table '{}' with value "
                    "'{}'".format(
                        key,
                        rec.__tablename__,
                        value if not isinstance(value, str) else value[:50]))
                rec.set_column(key, value)
            except ValueOverrideError as e:
                # It shouldn't happen but in case two or more identical
                # {key, value} are found in `kwargs`
                self.logger.critical(e)
            else:
                self.logger.debug("Column set!")
        # Check if the current table record's attributes are not already found
        # in another table record already saved in the list
        records_attributes = [r.__str__() for r in table_records]
        if rec.__str__() in records_attributes:
            raise DuplicateRecordError(
                "The current record '{}' from table '{}' is a duplicate! Thus it "
                "won't be saved.".format(rec.__str__(), rec.__tablename__))
        else:
            table_records.append(rec)
            # IMPORTANT: the table instance's tablename is used to get the
            # job_post's relationship list where the table instance will be added
            job_post_relationship = self.job_post.__getattribute__(
                rec.__tablename__)
            job_post_relationship.append(rec)

    # IMPORTANT: this method can be called more than once with the same `company`
    def set_company(self, **kwargs):
        for key, value in kwargs.items():
            try:
                self.logger.debug(
                    "Setting the column '{}' from table '{}' with value "
                    "'{}'".format(
                        key,
                        self.company.__tablename__,
                        value if not isinstance(value, str) else value[:50]))
                self.company.set_column(key, value)
            except ValueOverrideError as e:
                self.logger.warning(e)
            else:
                self.logger.debug("Column set!")

    # IMPORTANT: this method can be called more than once with the same `job_post`
    def set_job_post(self, **kwargs):
        for key, value in kwargs.items():
            try:
                self.logger.debug(
                    "Setting the column '{}' from table '{}' with value "
                    "'{}'".format(
                        key,
                        self.job_post.__tablename__,
                        value if not isinstance(value, str) else value[:50]))
                self.job_post.set_column(key, value)
            except ValueOverrideError as e:
                self.logger.warning(e)
            else:
                self.logger.debug("Column set!")
        # Setup the `company`'s relationship with `job_post` but only if the
        # said relationship is empty (list)
        if not self.company.job_posts:
            self.company.job_posts.append(self.job_post)

    def set_experience_level(self, **kwargs):
        exp_level = ExperienceLevel()
        exp_level.job_post_id = self.job_post_id
        self._setup_record(exp_level, self.experience_levels, **kwargs)

    def set_industry(self, **kwargs):
        industry = Industry()
        industry.job_post_id = self.job_post_id
        self._setup_record(industry, self.industries, **kwargs)

    def set_job_benefit(self, **kwargs):
        job_benefit = JobBenefit()
        job_benefit.job_post_id = self.job_post_id
        self._setup_record(job_benefit, self.job_benefits, **kwargs)

    def set_job_location(self, **kwargs):
        no_office_locations = [l for l in self.job_locations
                               if l.country == 'No office location']
        if no_office_locations:
            raise NoOfficeLocationFoundError(
                "'No office location'. Thus no more location can be added to the "
                "job post.")
        else:
            job_location = JobLocation()
            job_location.job_post_id = self.job_post_id
            self._setup_record(job_location, self.job_locations, **kwargs)

    def set_job_salary(self, **kwargs):
        salary = JobSalary()
        salary.job_post_id = self.job_post_id
        self._setup_record(salary, self.job_salaries, **kwargs)

    def set_role(self, **kwargs):
        role = Role()
        role.job_post_id = self.job_post_id
        self._setup_record(role, self.roles, **kwargs)

    def set_skill(self, **kwargs):
        skill = Skill()
        skill.job_post_id = self.job_post_id
        self._setup_record(skill, self.skills, **kwargs)
