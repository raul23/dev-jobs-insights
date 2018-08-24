import os
import sys
from tables import (JobPost, Company, ExperienceLevel, Role, Industry, Skill,
                    JobBenefit, JobSalary, JobLocation)


class JobPostData:

    def __init__(self, job_id):
        self.job_id = job_id
        self.company = Company()
        self.job_post = JobPost()
        self.experience_levels = []
        self.roles = []
        self.industries = []
        self.skills = []
        self.job_benefits = []
        self.job_salaries = []
        self.job_locations = []

    def set_experience_level(self, **kwargs):
        exp_level = ExperienceLevel()
        self._setup_record(exp_level, self.experience_levels, kwargs)

    def set_industry(self, **kwargs):
        industry = Industry()
        self._setup_record(industry, self.industries, kwargs)

    def set_job_benefit(self, **kwargs):
        job_benefit = JobBenefit()
        self._setup_record(job_benefit, self.job_benefits, kwargs)

    def set_job_location(self, **kwargs):
        job_location = JobLocation()
        self._setup_record(job_location, self.job_locations, kwargs)

    def set_job_salary(self, **kwargs):
        salary = JobSalary()
        self._setup_record(salary, self.job_salaries, kwargs)

    def set_role(self, **kwargs):
        role = Role()
        self._setup_record(role, self.roles, kwargs)

    def set_skills(self, **kwargs):
        skill = Skill()
        self._setup_record(skill, self.skills, kwargs)

    def set_company(self, **kwargs):
        self.company.set_column(kwargs)

    def set_job_post(self, **kwargs):
        self.job_post.set_column(kwargs)

    def _setup_record(self, record, records_list, **kwargs):
        record.set_column(kwargs)
        records_list.append(record)
