from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class ValueOverrideError(Exception):
    """Raised when """


class AbstractTable:
    def __init__(self):
        pass

    def set_column(self, **kwargs):
        for key, value in kwargs.items():
            # Check if the attribute already has a value set
            current_value = self.__getattribute__(key)
            if current_value:
                raise ValueOverrideError("The attribute '{}' in {} already has "
                                         "a value='{}'.".format(
                                            key,
                                            self.__tablename__,
                                            current_value))
            else:
                # Set the attribute with the provided value
                self.__setattr__(key, value)


class Company(Base, AbstractTable):
    __tablename__ = 'companies'
    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)
    url = Column(String(250))
    description = Column(Text)
    company_type = Column(String(250))
    size = Column(Integer)
    high_response_rate = Column(Boolean)


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
    job_post_terminated = Column(Boolean)
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
    job_id = Column(Integer, ForeignKey('job_posts.id'), primary_key=True)
    name = Column(String(250), primary_key=True)


class Role(Base, AbstractTable):
    __tablename__ = 'roles'
    job_id = Column(Integer, ForeignKey('job_posts.id'), primary_key=True)
    name = Column(String(250), primary_key=True)


class Industry(Base, AbstractTable):
    __tablename__ = 'industries'
    job_id = Column(Integer, ForeignKey('job_posts.id'), primary_key=True)
    name = Column(String(250), primary_key=True)


class Skill(Base, AbstractTable):
    __tablename__ = 'skills'
    job_id = Column(Integer, ForeignKey('job_posts.id'), primary_key=True)
    name = Column(String(250), primary_key=True)


class JobBenefit(Base, AbstractTable):
    __tablename__ = 'job_benefits'
    job_id = Column(Integer, ForeignKey('job_posts.id'), primary_key=True)
    name = Column(String(250), primary_key=True)


class JobSalary(Base, AbstractTable):
    __tablename__ = 'job_salaries'
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('job_posts.id'))
    min_salary = Column(Integer, nullable=False)
    max_salary = Column(Integer, nullable=False)
    currency = Column(Integer, nullable=False)
    conversion_time = Column(DateTime)

    def __str__(self):
        return "{}-{} {}".format(self.min_salary, self.max_salary,
                                 self.currency, )


class JobLocation(Base, AbstractTable):
    __tablename__ = 'job_locations'
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('job_posts.id'))
    city = Column(String(250))
    region = Column(String(250))
    country = Column(String(250), nullable=False)
