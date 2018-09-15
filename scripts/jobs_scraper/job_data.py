import functools
# Own modules
from tables import Base
from tables import (Company, JobPost, ExperienceLevel, Industry, JobBenefit,
                    JobSalary, JobLocation, Role, Skill)
from tables import ValueOverrideError


class JobData:

    def __init__(self, job_post_id):
        self.job_post_id = job_post_id
        self.company = Company()
        self.job_post = JobPost()
        self.job_post.id = job_post_id
        self.experience_levels = []
        self.industries = []
        self.job_benefits = []
        self.job_locations = []
        self.job_salaries = []
        self.roles = []
        self.skills = []

    # Returns: `tables_metadata`, dict
    #           {table_name (str): attribute_name (str)}
    # e.g. {'companies': 'company', 'job_posts': 'job_post'}
    def _get_tables_metadata(self):
        tables_metadata = {}
        for attr_k, attr_v in self.__dict__.items():
            if isinstance(attr_v, list):
                if attr_v:
                    item = attr_v[0]
                    if hasattr(item, '__tablename__'):
                        tables_metadata.setdefault(item.__tablename__, attr_k)
                else:
                    # TODO: use print_log/logging, check other places within file
                    print("[DEBUG] [{}] Table '{}' is empty".format(
                        self.job_post_id, attr_k))
            else:
                if hasattr(attr_v, '__tablename__'):
                    tables_metadata.setdefault(attr_v.__tablename__, attr_k)
        return tables_metadata

    def get_json_data(self):
        json_job_data = {}
        tables_metadata = self._get_tables_metadata()
        for table_name, attr_name in tables_metadata.items():
            attr = self.__getattribute__(attr_name)
            if isinstance(attr, list):
                table_instances = attr
                if table_instances:
                    for table_instance in table_instances:
                        # Save the table
                        column_names = table_instance.__table__.columns.keys()
                        table_dict \
                            = dict([(col, table_instance.__getattribute__(col))
                                    for col in column_names])
                        json_job_data.setdefault(table_name, [])
                        json_job_data[table_name].append(table_dict)
                else:
                    # TODO: use print_log/logging, check other places within file
                    print("[DEBUG] [{}] Table '{}' is empty".format(
                        self.job_post_id, table_name))
            else:
                table_instance = attr
                # Save the table
                column_names = table_instance.__table__.columns.keys()
                table_dict = dict([(col, table_instance.__getattribute__(col))
                                   for col in column_names])
                json_job_data.setdefault(table_name, table_dict)
        return json_job_data

    def _catch_value_override_error(func):
        @functools.wraps(func)
        def wrapper_catch_value_override_error(self, *args, **kwargs):
            try:
                func(self, *args, **kwargs)
            except ValueOverrideError as e:
                print('[{}] {}'.format(e.__class__.__name__, e))
        return wrapper_catch_value_override_error

    @_catch_value_override_error
    def set_company(self, **kwargs):
        self.company.set_column(**kwargs)

    @_catch_value_override_error
    def set_job_post(self, **kwargs):
        self.job_post.set_column(**kwargs)

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

    @staticmethod
    @_catch_value_override_error
    def _setup_record(record_object, records_list, **kwargs):
        record_object.set_column(**kwargs)
        records_list.append(record_object)
