import functools
# Own modules
from tables import Base
from tables import (Company, JobPost, ExperienceLevel, Industry, JobBenefit,
                    JobSalary, JobLocation, Role, Skill)
from tables import ValueOverrideError


class JobData:

    def __init__(self, job_post_id):
        self._job_post_id = job_post_id
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

    def _get_table_dict(self, table):
        # Sanity check on `table` type
        assert table.__class__.__class__.__name__ == Base.__class__.__name__
        table_dict = dict([(k, table.__getattribute__(k))
                           for k in table.__class__.__dict__.keys()
                           if not k.startswith('_')])
        return table_dict

    def get_json_data(self):
        json_job_data = {}
        for k, v in self.__dict__.items():
            if k.startswith('_'):
                continue
            # Get the table
            if not isinstance(v, list):
                table = v
                # Save the table
                table_dict = self._get_table_dict(table)
                json_job_data.setdefault(table.__tablename__, table_dict)
            else:
                tables = v
                if tables:
                    for table in tables:
                        # Save the table
                        table_dict = self._get_table_dict(table)
                        json_job_data.setdefault(table.__tablename__, [])
                        json_job_data[table.__tablename__].append(table_dict)
                else:
                    print("[DEBUG] [{}] Table '{}' is empty".format(self._job_post_id, k))
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
        exp_level.job_post_id = self._job_post_id
        self._setup_record(exp_level, self.experience_levels, **kwargs)

    def set_industry(self, **kwargs):
        industry = Industry()
        industry.job_post_id = self._job_post_id
        self._setup_record(industry, self.industries, **kwargs)

    def set_job_benefit(self, **kwargs):
        job_benefit = JobBenefit()
        job_benefit.job_post_id = self._job_post_id
        self._setup_record(job_benefit, self.job_benefits, **kwargs)

    def set_job_location(self, **kwargs):
        job_location = JobLocation()
        job_location.job_post_id = self._job_post_id
        self._setup_record(job_location, self.job_locations, **kwargs)

    def set_job_salary(self, **kwargs):
        salary = JobSalary()
        salary.job_post_id = self._job_post_id
        self._setup_record(salary, self.job_salaries, **kwargs)

    def set_role(self, **kwargs):
        role = Role()
        role.job_post_id = self._job_post_id
        self._setup_record(role, self.roles, **kwargs)

    def set_skill(self, **kwargs):
        skill = Skill()
        skill.job_post_id = self._job_post_id
        self._setup_record(skill, self.skills, **kwargs)

    @staticmethod
    @_catch_value_override_error
    def _setup_record(record_object, records_list, **kwargs):
        record_object.set_column(**kwargs)
        records_list.append(record_object)
