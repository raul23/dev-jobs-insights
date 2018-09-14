from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class ValueOverrideError(Exception):
    """Raised when """


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
                raise ValueOverrideError("The attribute '{}' in {} already has "
                                         "a value='{}'.".format(
                                            key,
                                            self.__tablename__,
                                            current_value))
            else:
                # Set the attribute with the provided value
                self.__setattr__(key, value)


# IMPORTANT: `Company` must first be added to the db, then the `JobPost` can be
# added. `JobPost` needs the `company_id` when being added to the db.
class Company(Base, AbstractTable):
    __tablename__ = 'companies'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(250), nullable=False)
    url = Column(String(250))
    description = Column(Text)  # company description
    company_type = Column(String(250))
    company_min_size = Column(Integer)
    company_max_size = Column(Integer)
    high_response_rate = Column(Boolean, default=False)


class JobPost(Base, AbstractTable):
    __tablename__ = 'job_posts'
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id'))
    title = Column(String(250), nullable=False)
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
    cached_webpage_filepath = Column(String(250))
    # `webpage_accessed` makes use of the date format YYYY-MM-DD HH:MM:SS-HH:MM
    webpage_accessed = Column(DateTime, nullable=False)
    # `data_posted` makes use of the date format YYYY-MM-DD
    date_posted = Column(Date)
    # `valid_through` makes use of the date format YYYY-MM-DD
    valid_through = Column(Date)


class ExperienceLevel(Base, AbstractTable):
    __tablename__ = 'experience_levels'
    job_post_id = Column(Integer, ForeignKey('job_posts.id'), primary_key=True)
    name = Column(String(250), primary_key=True)   # name of experience level


class Industry(Base, AbstractTable):
    __tablename__ = 'industries'
    job_post_id = Column(Integer, ForeignKey('job_posts.id'), primary_key=True)
    name = Column(String(250), primary_key=True)  # name of industry


class JobBenefit(Base, AbstractTable):
    __tablename__ = 'job_benefits'
    job_post_id = Column(Integer, ForeignKey('job_posts.id'), primary_key=True)
    name = Column(String(250), primary_key=True)  # name of job benefit


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
    min_salary = Column(Integer, nullable=False)
    max_salary = Column(Integer, nullable=False)
    currency = Column(Integer, nullable=False)
    conversion_time = Column(DateTime)

    def __str__(self):
        return "{}-{} {}".format(self.min_salary, self.max_salary,
                                 self.currency, )


class Role(Base, AbstractTable):
    __tablename__ = 'roles'
    job_post_id = Column(Integer, ForeignKey('job_posts.id'), primary_key=True)
    name = Column(String(250), primary_key=True)  # name of role


class Skill(Base, AbstractTable):
    __tablename__ = 'skills'
    job_post_id = Column(Integer, ForeignKey('job_posts.id'), primary_key=True)
    name = Column(String(250), primary_key=True)  # name of skill
