"""
Data analysis of StackOverflow developer job posts
"""
import os
import sys

from analyzers.industry_analyzer import IndustryAnalyzer
from analyzers.location_analyzer import LocationAnalyzer
from analyzers.salary_analyzer import SalaryAnalyzer
from analyzers.tag_analyzer import TagAnalyzer
from utility import genutil as util


class JobDataAnalyzer:
    def __init__(self, settings_path):

        # TODO: add DEFAULT config values
        # TODO: test all the paths with check_file_exists() and exit if there is an error
        # TODO: set in config.ini the size of the saved graphs
        # TODO: check if we must use np.{int,float}{32,64}
        # TODO: add line numbers to when calling exit_script(), also fix the inconsistency
        # TODO: replace all assert with print_exception() instead
        # TODO: in the message error, line number don't match the actual error because we are
        # not catching the actual source of the error but the catch is placed
        # farther from the source of the error
        # TODO: the outliers should be removed once and for all as early as possible
        # by finding the corresponding job ids and removing them from the different arrays
        # TODO: rearrange the name of the variables
        # TODO: specify that they are all unique, e.g. no two locations/tags/countries/us states
        # TODO: locations_info is a dict and the rest are np.array

        self.config_ini = util.read_config(settings_path)
        if self.config_ini is None:
            exit_script("ERROR: {} could not be read".format(settings_path))
        self.types_of_analysis = self.get_analyses()
        db_path = self.config_ini["paths"]["db_path"]
        db_path = os.path.expanduser(db_path)
        self.conn = util.create_connection(db_path)
        if self.conn is None:
            exit_script("ERROR: Connection to db couldn't be established")

    def get_analyses(self):
        return [k for k,v in self.config_ini["analysis_types"].items() if v]

    def run_analysis(self):
        with self.conn:
            for analysis_type in self.types_of_analysis:
                try:
                    analyze_method = self.__getattribute__(analysis_type)
                    analyze_method()
                except (AttributeError, FileNotFoundError) as err:
                    err_name = type(err).__name__
                    util.print_exception(err_name)
                    print("ERROR: {} will be skipped because of an {}".format(analysis_type, err_name))

    def analyze_tags(self):
        """
        Analysis of tags (i.e. technologies such as java, python) which consist in
        ... TODO complete description

        :return:
        """
        ta = TagAnalyzer(self.conn, self.config_ini)
        ta.run_analysis()

    def analyze_locations(self):
        """
        Analysis of locations which consists in ... TODO: complete description

        :return:
        """
        la = LocationAnalyzer(self.conn, self.config_ini)
        la.run_analysis()

    def analyze_salary(self):
        sa = SalaryAnalyzer(self.conn, self.config_ini)
        sa.run_analysis()

    def analyze_industries(self):
        """
        Analysis of tags (i.e. technologies such as java, python) which consist in
        ... TODO complete description

        :return:
        """
        ia = IndustryAnalyzer(self.conn, self.config_ini)
        ia.run_analysis()

    def generate_report(self):
        # TODO: complete method
        raise NotImplementedError


def exit_script(msg, code=1):
    print(msg)
    print("Exiting...")
    sys.exit(code)
