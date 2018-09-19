import functools
# Own modules
from tables import (Company, JobPost, ExperienceLevel, Industry, JobBenefit,
                    JobSalary, JobLocation, Role, Skill)
from tables import ValueOverrideError


class JobData:
    def __init__(self, job_post_id):
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

    # TODO: this method is not necessary, see TODO in `get_sqlalchemy_job_data`
    # Also, it can be done within `get_json_data`
    def _get_attributes_sequence(self):
        # TODO: first tables in the sequence are hard-coded. Do it automatically
        # by:
        #   * add __seq_position__ to each table class initialized to 0 (the
        #       closer to 0, the the most important is the table in being called
        #       when inserting rows in the db)
        #   * iterate over each attribute of the table class with
        #       TableClass.attr_name.property if TableClass.attr_name.property
        #       refers to a RelationshipProperty, then the related table
        #       (referred in the RelationshipProperty) should be one position
        #       higher than the the table class. You can get the related table's
        #       name with TableClass.attr_name.table.fullname,
        #
        #       e.g. Company.job_posts.property.table.fullname would give
        #       'job_posts'. So in this example, 'job_posts' should be one
        #       position higher than 'companies'. As a consequence, a company
        #       should be added to 'companies', then a job_post should be added
        #       to 'job_posts'.
        # TODO: find a simpler solution to avoid hard-coding the sequence with
        # the most important tables
        attributes_sequence = ['company', 'job_post']
        for attr_k, attr_v in self.__dict__.items():
            if attr_k in attributes_sequence:
                continue
            if isinstance(attr_v, list):
                if attr_v:
                    item = attr_v[0]
                    if hasattr(item, '__tablename__'):
                        attributes_sequence.append(attr_k)
                else:
                    # TODO: use print_log/logging, check other places within file
                    print("[DEBUG] [{}] Table '{}' is empty".format(
                        self.job_post_id, attr_k))
            else:
                if hasattr(attr_v, '__tablename__'):
                    attributes_sequence.append(attr_k)
        return attributes_sequence

    def get_json_data(self):
        json_job_data = {}
        attrs_seq = self._get_attributes_sequence()
        for attr_name in attrs_seq:
            attr = self.__getattribute__(attr_name)
            if isinstance(attr, list):
                table_instances = attr
                if table_instances:
                    for table_instance in table_instances:
                        # Save the table
                        table_name = table_instance.__tablename__
                        column_names = table_instance.__table__.columns.keys()
                        table_dict \
                            = dict([(col, table_instance.__getattribute__(col))
                                    for col in column_names])
                        json_job_data.setdefault(table_name, [])
                        json_job_data[table_name].append(table_dict)
                else:
                    # TODO: use print_log/logging, check other places within file
                    # TODO: check if this print is already being done in
                    # `_get_attributes_sequence`. If yes, then this `else` block
                    # might not be necessary
                    print("[DEBUG] [{}] Table '{}' is empty".format(
                        self.job_post_id, table_name))
            else:
                table_instance = attr
                table_name = table_instance.__tablename__
                # Save the table
                # TODO: should we also add columns with None as a value?
                column_names = table_instance.__table__.columns.keys()
                table_dict = dict([(col, table_instance.__getattribute__(col))
                                   for col in column_names])
                json_job_data.setdefault(table_name, table_dict)
        return json_job_data

    # TODO: this method is not necessary anymore because we only need to add
    # the `company` in the db and the rest of the table instances are also added
    # via their relationships
    """
    def get_sqlalchemy_job_data(self):
        sqlalchemy_job_data = []
        attrs_seq = self._get_attributes_sequence()
        for attr_name in attrs_seq:
            attr = self.__getattribute__(attr_name)
            if isinstance(attr, list):
                sqlalchemy_job_data.extend(self.__getattribute__(attr_name))
            else:
                sqlalchemy_job_data.append(self.__getattribute__(attr_name))
        return sqlalchemy_job_data
    """

    def _check_kwargs_all_none(func):
        @functools.wraps(func)
        def wrapper_check_kwargs_all_none(self, *args, **kwargs):
            set_values = set(kwargs.values())
            if len(set_values) == 1 and set_values.pop() is None:
                print("[DEBUG] [{}] The table '{}' is all empty".format(
                    self.job_post_id, args[0].__tablename__))
            else:
                func(self, *args, **kwargs)

        return wrapper_check_kwargs_all_none

    def _catch_value_override_error(func):
        @functools.wraps(func)
        def wrapper_catch_value_override_error(self, *args, **kwargs):
            try:
                func(self, *args, **kwargs)
            except ValueOverrideError as e:
                print('[{}] {}'.format(e.__class__.__name__, e))
            # TODO:
            # add a flag (can be named `updated`) that indicates whether
            # one of the JobData's attributes was successfully updated. Hence,
            # this flag could be used in `get_json_data()` so you don't have to
            # do lots of computations to get the JSON job data if the latter was
            # already computed and `updated` is False. Every time one of
            # JobData's attributes are updated, the flag `updated` is set to True.
            # Then when `get_json_data` is called, the JSON job data is
            # re-generated, and the flag is set to False. This flag might also
            # by used in other places such as `get_sqlalchemy_job_data()`
        return wrapper_catch_value_override_error

    @_catch_value_override_error
    # IMPORTANT: this method can be called more than once with the same `company`
    def set_company(self, **kwargs):
        self.company.set_column(**kwargs)

    @_catch_value_override_error
    # IMPORTANT: this method can be called more than once with the same `job_post`
    def set_job_post(self, **kwargs):
        self.job_post.set_column(**kwargs)
        # Setup the `company`'s relationship with `job_post` but only if the
        # said relationship is empty (list)
        if not self.company.job_posts:
            self.company.job_posts.append(self.job_post)

    # TODO: these methods should be removed, and instead we should instantiate
    # table classes (from `tables.py`), assign directly the columns' values to
    # the table's constructor
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

    @_check_kwargs_all_none
    @_catch_value_override_error
    # TODO: change variable names
    #       use `table_instance` instead of `record_object`
    #       use `table_instances` instead of `records_list`
    #       use `_setup_table_instance` instead of `_setup_record`
    # IMPORTANT: this method along with other methods starting with `set_` are
    # called only once per table instance (aka record)
    def _setup_record(self, record_object, records_list, **kwargs):
        record_object.set_column(**kwargs)
        records_list.append(record_object)
        # IMPORTANT: the table instance's tablename is used to get the
        # job_post's relationship list where the table instance will be added
        job_post_relationship = self.job_post.__getattribute__(
            record_object.__tablename__)
        job_post_relationship.append(record_object)
