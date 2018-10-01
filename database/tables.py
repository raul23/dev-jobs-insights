# TODO: change filename to 'job_data_tables.py'
# TODO: is it alright to use `None` as value for empty columns or should we use
# the empty string '' instead? Does using `None` uses less storage space than
# using the empty string ''? e.g. in the `JobLocation` table, the column `region`
# (aka US state) might be missing in the job posts, and you need to be careful
# manipulating strings because if you do len(None) you will an error, `None`
# should be changed to its string equivalent `str(None)` before manipulating
# string columns, see `_shrink_labels()` in `analyzer.py`
from sqlalchemy import Boolean, CHAR, Column, Date, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


class ValueOverrideError(Exception):
    """Raised when a non-empty column is being set by another value"""


class AbstractTable:
    def __init__(self):
        pass

    # TODO: this doesn't work for tables that are associated with a list in
    # `JobPostData` because the table object is created every time some
    # attributes are to be set
    def set_column(self, **kwargs):
        for key, value in kwargs.items():
            # Check if the attribute already has a value set
            current_value = self.__getattribute__(key)
            if current_value:
                # Attribute already has a value set
                raise ValueOverrideError(
                    "The attribute '{}' in {} already has a value='{}'.".format(
                        key, self.__tablename__, current_value))
            else:
                # Set the attribute with the provided value
                self.__setattr__(key, value)


# IMPORTANT: `Company` must first be added to the db, then the `JobPost` can be
# added. `JobPost` needs the `company_id` when being added to the db.
# The relationship from `Company` to `JobPost` is one to many
class Company(Base, AbstractTable):
    __tablename__ = 'companies'
    id = Column(Integer, primary_key=True)
    name = Column(String(250))
    url = Column(String(250))
    description = Column(Text)  # company description
    company_type = Column(String(250))
    company_min_size = Column(Integer)
    company_max_size = Column(Integer)
    high_response_rate = Column(Boolean, default=False)
    #################################
    # Relationships to other tables #
    #################################
    job_posts = relationship('JobPost')


# The relationship from `JobPost` to {table_name} is one to many
# where table_name = {`ExperienceLevel`, `Industry`, `JobBenefit`,
#                     `JobLocation`, `JobSalary`, `Role`, `Skill`}
class JobPost(Base, AbstractTable):
    __tablename__ = 'job_posts'
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id'))
    title = Column(String(250))
    url = Column(String(250), nullable=False)
    employment_type = Column(String(250))
    job_post_description = Column(Text)
    # `job_post_terminated` = True if the company is not accepting job
    # applications anymore
    job_post_terminated = Column(Boolean, default=False)
    # `job_post_removed` = True if the job post was removed
    job_post_removed = Column(Boolean, default=False)
    # `equity` = True if there is equity involved
    equity = Column(Boolean, default=False)
    remote = Column(String(250))
    relocation = Column(String(250))
    visa = Column(String(250))
    cached_webpage_filepath = Column(Text)
    # Date format YYYY-MM-DD
    date_posted = Column(Date)
    # Date format YYYY-MM-DD
    valid_through = Column(Date)
    # Datetime format YYYY-MM-DD HH:MM:SS-HH:MM
    webpage_accessed = Column(DateTime)
    #################################
    # Relationships to other tables #
    #################################
    experience_levels = relationship('ExperienceLevel')
    industries = relationship('Industry')
    job_benefits = relationship('JobBenefit')
    job_locations = relationship('JobLocation')
    job_salaries = relationship('JobSalary')
    roles = relationship('Role')
    skills = relationship('Skill')


# TODO: ExperienceLevel, Industry, JobBenefit, Role, and Skill have the same
# two columns: `job_post_id` and `name`. Create a separate table with these
# two columns, and use it as a base table for the other tables.
# ref.: https://bit.ly/2xd3bM3
class ExperienceLevel(Base, AbstractTable):
    __tablename__ = 'experience_levels'
    id = Column(Integer, primary_key=True)
    job_post_id = Column(Integer, ForeignKey('job_posts.id'))
    name = Column(String(250))  # name of experience level


class Industry(Base, AbstractTable):
    __tablename__ = 'industries'
    id = Column(Integer, primary_key=True)
    job_post_id = Column(Integer, ForeignKey('job_posts.id'))
    name = Column(String(250))  # name of industry


class JobBenefit(Base, AbstractTable):
    __tablename__ = 'job_benefits'
    id = Column(Integer, primary_key=True)
    job_post_id = Column(Integer, ForeignKey('job_posts.id'))
    name = Column(String(250))  # name of job benefit


# TODO: JobLocation and JobSalary have the same two columns: `id` and
# `job_post_id`. See remark above for ExperienceLevel.
class JobLocation(Base, AbstractTable):
    __tablename__ = 'job_locations'
    id = Column(Integer, primary_key=True)
    job_post_id = Column(Integer, ForeignKey('job_posts.id'))
    city = Column(String(250))
    region = Column(String(250))
    country = Column(String(250), nullable=False)


class JobSalary(Base, AbstractTable):
    __tablename__ = 'job_salaries'
    id = Column(Integer, primary_key=True)
    job_post_id = Column(Integer, ForeignKey('job_posts.id'))
    min_salary = Column(Integer)
    max_salary = Column(Integer)
    currency = Column(CHAR)
    conversion_time = Column(DateTime)

    # TODO: remove it if isn't used somewhere
    def __str__(self):
        return "{}-{} {}".format(self.min_salary, self.max_salary,
                                 self.currency, )


class Role(Base, AbstractTable):
    __tablename__ = 'roles'
    id = Column(Integer, primary_key=True)
    job_post_id = Column(Integer, ForeignKey('job_posts.id'))
    name = Column(String(250))  # name of role


class Skill(Base, AbstractTable):
    __tablename__ = 'skills'
    id = Column(Integer, primary_key=True)
    job_post_id = Column(Integer, ForeignKey('job_posts.id'))
    name = Column(String(250))  # name of skill
