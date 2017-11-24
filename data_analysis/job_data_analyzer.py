"""
Data analysis of Stackoverflow developer job posts
"""
import os
import sys
import time
import ipdb

import geopy
from geopy.geocoders import Nominatim
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import numpy as np

from analyzers.salary_analyzer import SalaryAnalyzer
from utility import util, graph_util as g_util


class JobDataAnalyzer:
    def __init__(self, settings_filename):

        # TODO: add DEFAULT config values
        # TODO: test all the paths with check_file_exists() and exit if there is an error
        # TODO: set in config.ini the size of the saved graphs
        # TODO: check if we must use np.{int,float}{32,64}
        # TODO: add line numbers to when calling exit_script(), also fix the inconsistency
        # TODO: replace all assert with print_exception() instead
        # in the message error (line number don't match the actual error because we are
        # not catching the actual the source of the error but the catch is placed
        # farther from the source of the errror
        # TODO: the outliers should be removed once and for all as early as possible
        # by finding the corresponding job ids and removing them from the different arrays
        # TODO: rearrange the name of the variables
        # TODO: specify that they are all unique, e.g. no two locations/tags/countries/us states
        # TODO: locations_info is a dict and the rest are np.array

        self.config_ini = util.read_config(settings_filename)
        if self.config_ini is None:
            exit_script("ERROR: {} could not be read".format(settings_filename))
        self.types_of_analysis = self.get_analyses()
        db_path = self.config_ini["paths"]["db_path"]
        db_path = os.path.expanduser(db_path)
        self.conn = util.create_connection(db_path)
        if self.conn is None:
            exit_script("ERROR: Connection to db couldn't be established")
        self.shape_path = os.path.expanduser(self.config_ini["paths"]["shape_path"])
        # TODO: cached_locations_path should be called cached_map_locations_path because
        # it relates to map locations' coordinates
        self.cached_locations_path = self.config_ini["paths"]["cached_locations_path"]
        self.cached_locations = util.load_pickle(self.cached_locations_path)
        if self.cached_locations is None:
            self.cached_locations = {}
        self.wait_time = self.config_ini["geocoding"]["wait_time"]
        self.marker_scale = self.config_ini["basemap"]["marker_scale"]
        self.min_salary_threshold = self.config_ini["outliers"]["min_salary"]
        self.max_salary_threshold = self.config_ini["outliers"]["max_salary"]
        # These are all the data that will be saved while performing the various analyses
        # Tags stats to compute
        self.tags_stats = {"sorted_tags_count": None}
        # Locations stats to compute
        self.locations_stats = [
            "locations_info",
            "sorted_countries_count",
            "sorted_us_states_count"
        ]
        self.locations_stats = dict(zip(self.locations_stats, [None]*len(self.locations_stats)))
        # Industries stats to compute
        self.industries_stats = {"sorted_industries_count": None}

    def get_analyses(self):
        return [k for k,v in self.config_ini["analysis_types"].items() if v]

    def run_analysis(self):
        with self.conn:
            for analysis_type in self.types_of_analysis:
                try:
                    analyze_method = self.__getattribute__(analysis_type)
                    analyze_method()
                except AttributeError:
                    util.print_exception("AttributeError")
                    print("ERROR: {} will be skipped because of an AttributeError".format(analysis_type))

    def analyze_tags(self):
        """
        Analysis of tags (i.e. technologies such as java, python) which consist in
        ... TODO complete description

        :return:
        """
        # Get counts of tags, i.e. for each tag we want to know its number of
        # occurrences in job posts
        results = self.count_tag_occurrences()
        # NOTE: these are all the tags (even those that don't have a salary
        # associated with) and they are sorted in order of decreasing
        # number of occurrences (i.e. most popular tag at first)
        self.sorted_tags_count = np.array(results)

        # Generate bar chart of tags vs number of job posts
        top_k = self.config_ini["bar_chart_tags"]["top_k"]
        config = {"x": self.sorted_tags_count[:top_k, 0],
                  "y": self.sorted_tags_count[:top_k, 1].astype(np.int32),
                  "xlabel": self.config_ini["bar_chart_tags"]["xlabel"],
                  "ylabel": self.config_ini["bar_chart_tags"]["ylabel"],
                  "title": self.config_ini["bar_chart_tags"]["title"],
                  "grid_which": self.config_ini["bar_chart_tags"]["grid_which"]}
        # TODO: place number (of job posts) on top of each bar
        g_util.generate_bar_chart(config)

    def analyze_locations(self):
        """
        Analysis of locations which consists in ... TODO: complete description

        :return:
        """
        # Get counts of job posts for each location, i.e. for each location we
        # want to know its number of occurrences in job posts
        results = self.count_location_occurrences()
        # Process the results
        self.process_locations(results)
        # TODO: add in config option to set the image dimensions

        # Generate map with markers added on US states that have job posts
        # associated with
        self.generate_map_us_states()
        # Generate map with markers added on countries that have job posts
        # associated with
        self.generate_map_world_countries()
        # Generate map with markers added on european countries that have job
        # posts associated with
        self.generate_map_europe_countries()

        # NOTE: bar charts are for categorical data
        # Generate bar chart of countries vs number of job posts
        top_k = self.config_ini["bar_chart_countries"]["top_k"]
        country_names = self.format_country_names(self.sorted_countries_count[:top_k, 0])
        config = {"x": country_names,
                  "y": self.sorted_countries_count[:top_k, 1].astype(np.int32),
                  "xlabel": self.config_ini["bar_chart_countries"]["xlabel"],
                  "ylabel": self.config_ini["bar_chart_countries"]["ylabel"],
                  "title": self.config_ini["bar_chart_countries"]["title"],
                  "grid_which": self.config_ini["bar_chart_countries"]["grid_which"]}
        # TODO: place number (of job posts) on top of each bar
        g_util.generate_bar_chart(config)
        # Generate bar chart of US states vs number of job posts
        config = {"x": self.sorted_us_states_count[:, 0],
                  "y": self.sorted_us_states_count[:, 1].astype(np.int32),
                  "xlabel": self.config_ini["bar_chart_us_states"]["xlabel"],
                  "ylabel": self.config_ini["bar_chart_us_states"]["ylabel"],
                  "title": self.config_ini["bar_chart_us_states"]["title"],
                  "grid_which": self.config_ini["bar_chart_us_states"]["grid_which"]}
        g_util.generate_bar_chart(config)

        # Generate pie chart of countries vs number of job posts
        config = {"labels": self.sorted_countries_count[:, 0],
                  "values": self.sorted_countries_count[:, 1].astype(np.int32),
                  "title": self.config_ini["pie_chart_countries"]["title"]}
        # TODO: add 'other countries' for countries with few job posts
        # Pie chart is too crowded for countries with less than 0.9% of job posts
        g_util.generate_pie_chart(config)
        # Generate pie chart of countries vs number of job posts
        config = {"labels": self.sorted_us_states_count[:, 0],
                  "values": self.sorted_us_states_count[:, 1].astype(np.int32),
                  "title": self.config_ini["pie_chart_us_states"]["title"]}
        g_util.generate_pie_chart(config)

    def analyze_salary(self):
        ipdb.set_trace()
        sa = SalaryAnalyzer(self.conn, self.salary_topics, self.config_ini)
        data = sa.run_analysis()

    def analyze_industries(self):
        """
        Analysis of tags (i.e. technologies such as java, python) which consist in
        ... TODO complete description

        :return:
        """
        # Get number of job posts for each industry
        # TODO: specify that the results are already sorted in decreasing order of industry's count, i.e.
        # from the most popular industry to the least one
        results = self.count_industry_occurrences()
        # TODO: Process the results by summing the similar industries (e.g. Software Development with
        # Software Development / Engineering or eCommerce with E-Commerce)
        # TODO: use Software Development instead of the longer Software Development / Engineering
        self.sorted_industries_count = np.array(results)

        # Generate bar chart: industries vs number of job posts
        top_k = self.config_ini["bar_chart_industries"]["top_k"]
        config = {"x": self.sorted_industries_count[:top_k, 0],
                  "y": self.sorted_industries_count[:top_k, 1].astype(np.int32),
                  "xlabel": self.config_ini["bar_chart_industries"]["xlabel"],
                  "ylabel": self.config_ini["bar_chart_industries"]["ylabel"],
                  "title": self.config_ini["bar_chart_industries"]["title"],
                  "grid_which": self.config_ini["bar_chart_industries"]["grid_which"]}
        # TODO: place number (of job posts) on top of each bar
        g_util.generate_bar_chart(config)

    def format_country_names(self, country_names, max_n_char=20):
        for i, name in enumerate(country_names):
            if len(name) > max_n_char:
                alpha2 = self.countries[name]["alpha2"]
                country_names[i] = alpha2
        return country_names

    def count_tag_occurrences(self):
        """
        Returns tags sorted in decreasing order of their occurrences in job posts.
        A list of tuples is returned where a tuple is of the form (tag_name, count).

        :return: list of tuples of the form (tag_name, count)
        """
        sql = '''SELECT name, COUNT(name) as CountOf FROM entries_tags GROUP BY name ORDER BY CountOf DESC'''
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def count_location_occurrences(self):
        """
        Returns locations sorted in decreasing order of their occurrences in job posts.
        A list of tuples is returned where a tuple is of the form (location, count).

        :return: list of tuples of the form (location, count)
        """
        sql = '''SELECT location, COUNT(*) as CountOf FROM job_posts GROUP BY location ORDER BY CountOf DESC'''
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def count_industry_occurrences(self):
        """
        Returns industries sorted in decreasing order of their occurrences in job posts.
        A list of tuples is returned where a tuple is of the form (industry, count).

        :return: list of tuples of the form (industry, count)
        """
        sql = '''SELECT value, COUNT(*) as CountOf from job_overview WHERE name='Industry' GROUP BY value ORDER BY CountOf DESC'''
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def process_locations(self, locations):
        # Temp dicts
        locations_info = {}
        countries_to_count = {}
        us_states_to_count = {}
        # TODO: factorization of for loop with generate_map()
        for (i, (location, count)) in enumerate(locations):
            print("[{}/{}]".format((i + 1), len(locations)))
            # Check if valid location
            if not is_valid_location(location):
                # NOTE: We ignore the case where `location` is empty (None)
                # or refers to "No office location"
                # TODO: add logging
                continue
            # Sanitize input: this should be done at the source, i.e. in the
            # script that is loading data into the database
            elif ";" in location:
                new_locations = location.split(";")
                for new_loc in new_locations:
                    locations.append((new_loc.strip(), 1))
                continue
            else:
                # Get country or US state from `location`
                last_part_loc = get_last_part_loc(location)
                # Sanity check
                assert last_part_loc is not None, "last_part_loc is None"
                # Is the location referring to a country or a US state?
                if self.is_a_us_state(last_part_loc):
                    # `location` refers to a US state
                    # Save last part of `location` and its count (i.e. number of
                    # occurrences in job posts)
                    us_states_to_count.setdefault(last_part_loc, 0)
                    us_states_to_count[last_part_loc] += count
                    # Also since it is a US state, save 'United States' and its
                    # count (i.e. number of occurrences in job posts)
                    # NOTE: in the job posts, the location for a US state is
                    # given without the country at the end, e.g. Fort Meade, MD
                    countries_to_count.setdefault("United States", 0)
                    countries_to_count["United States"] += count
                    # Add ', United States' at the end of `location` since the
                    # location for US states in job posts don't specify the country
                    # and we might need this extra info when using the geocoding
                    # service to retrieve map coordinates to distinguish places
                    # from Canada and USA that might have the similar name for
                    # the location
                    # Example: 'Westlake Village, CA' might get linked to
                    # 'Westlake Village, Hamlet of Clairmont, Grande Prairie, Alberta'
                    # In this case, it should be linked to a region in California,
                    # not in Canada
                    formatted_location = "{}, United States".format(location)
                    locations_info.setdefault(formatted_location, {"country": "United States",
                                                         "count": 0})
                    locations_info[formatted_location]["count"] += count
                else:
                    # `location` refers to a country
                    # Check for countries written in other languages, and keep
                    # only the english translation
                    # NOTE: sometimes, a country is not given in English e.g.
                    # Deutschland and Germany
                    # Save the location and its count (i.e. number of occurrences
                    # in job posts)
                    transl_country = self.get_english_country_transl(last_part_loc)
                    assert transl_country in self.countries, "The country '{}' is not found".format(transl_country)
                    countries_to_count.setdefault(transl_country, 0)
                    countries_to_count[transl_country] += count
                    locations_info.setdefault(location, {"country": transl_country,
                                                         "count": 0})
                    locations_info[location]["count"] += count
        # NOTE: `locations_info` is already sorted based on the location's count
        # because it is almost a copy of `locations` which is already sorted
        # (based on the location's count) from the returned database request
        self.locations_info = locations_info
        # Sort the countries and US-states dicts based on the number of
        # occurrences, i.e. the dict's values. And convert the sorted dicts
        # into a numpy array
        # TODO: check if these are useful arrays
        self.sorted_countries_count = sorted(countries_to_count.items(), key=lambda x: x[1], reverse=True)
        self.sorted_countries_count = np.array(self.sorted_countries_count)
        self.sorted_us_states_count = sorted(us_states_to_count.items(), key=lambda x: x[1], reverse=True)
        self.sorted_us_states_count = np.array(self.sorted_us_states_count)

    def filter_locations(self, include_continents="All", exclude_countries=None):
        # TODO: Sanity check on `include_continents` and `exclude_countries`
        filtered_locations = []
        for loc, country_info in self.locations_info.items():
            country = country_info["country"]
            count = country_info["count"]
            if (include_continents == "All" or self.get_continent(country) in include_continents) \
                    and (exclude_countries is None or country not in exclude_countries):
                filtered_locations.append((loc, count))
        return filtered_locations

    def generate_map_us_states(self):
        # TODO: find out the complete name of the map projection used
        # We are using the Lambert ... map projection and cropping the map to
        # display only the USA territory
        map = Basemap(llcrnrlon=-119, llcrnrlat=22, urcrnrlon=-64, urcrnrlat=49,
                      projection='lcc', lat_1=32, lat_2=45, lon_0=-95)
        map.readshapefile(self.shape_path, name="states", drawbounds=True)
        locations = self.filter_locations(include_continents=["North America"],
                                          exclude_countries=["Canada", "Mexico"])
        self.generate_map(map, locations,
                          markersize=lambda count: int(np.sqrt(count)) * self.marker_scale,
                          top_k=3)

    def generate_map_world_countries(self):
        # a Miller Cylindrical projection
        # TODO: should be set in the config, just like MARKER_SCALE which is used
        # in generate_map_us_states()
        marker_scale = 1.5
        map = Basemap(projection="mill",
                      llcrnrlon=-180., llcrnrlat=-60,
                      urcrnrlon=180., urcrnrlat=80.)
        # Draw coast lines, countries, and fill the continents
        map.drawcoastlines()
        map.drawcountries()
        map.drawstates()
        map.fillcontinents()
        map.drawmapboundary()
        locations = self.filter_locations(include_continents="All")
        self.generate_map(map, locations, markersize=lambda count: marker_scale)

    def generate_map_europe_countries(self):
        # TODO: complete method
        pass

    def generate_map(self, map, locations, markersize, top_k=None):
        # TODO: decouple map generation code from data processing code, same for the
        # other map generation methods which they all should be in a module in the utility package
        # like the graph generation code (found in graph_util.py)
        new_cached_locations = False
        top_k_locations = []
        if top_k is not None:
            top_k_locations = get_top_k_locations(locations, k=top_k)
        for (i, (location, count)) in enumerate(locations):
            print("[{}/{}]".format((i+1), len(locations)))
            # Check if valid location
            if not is_valid_location(location):
                # NOTE: We ignore the cases where `location` is empty (None)
                # or refers to "No office location" or is not in the right continent
                # TODO: add logging
                continue
            # Check if we already computed the location's longitude and latitude
            # with the geocoding service
            elif location in self.cached_locations:
                loc = self.cached_locations[location]
            else:
                # TODO: else clause to be checked
                ipdb.set_trace()
                # Get the location's longitude and latitude
                # We are using the module `geopy` to get the longitude and latitude of
                # locations which will then be transformed into map coordinates so we can
                # draw markers on a map with `basemap`
                geolocator = Nominatim()
                loc = None
                try:
                    loc = geolocator.geocode(location)
                except geopy.exc.GeocoderTimedOut:
                    ipdb.set_trace()
                    util.dump_pickle(self.cached_locations, self.cached_locations_path)
                    # TODO: do something when there is a connection error with the geocoding service
                # Check if the geocoder service was able to provide the map coordinates
                if loc is None:
                    ipdb.set_trace()
                    # Take the last part (i.e. country) since the whole location
                    # string is not recognized by the geocoding service
                    last_part_loc = get_last_part_loc(location)
                    # Sanity check
                    assert last_part_loc is not None, "last_part_loc is None"
                    time.sleep(self.wait_time)
                    loc = geolocator.geocode(last_part_loc)
                    assert loc is not None, "The geocoding service could not for the second time" \
                                            "provide the map coordinates for the location '{}'".format(last_part_loc)
                time.sleep(self.wait_time)
                new_cached_locations = True
                assert loc is not None, "loc is None"
                self.cached_locations[location] = loc
            # Transform the location's longitude and latitude to the projection
            # map coordinates
            x, y = map(loc.longitude, loc.latitude)
            # Plot the map coordinates on the map; the size of the marker is
            # proportional to the number of occurrences of the location in job posts
            map.plot(x, y, marker="o", color="Red", markersize=markersize(count))
            # Annotate topk locations, i.e.the topk locations with the most job posts
            if location in top_k_locations:
                plt.text(x, y, location, fontsize=5, fontweight="bold",
                         ha="left", va="bottom", color="k")
        # Dump `cached_locations` as a pickle file if new locations' map
        # coordinates computed
        if new_cached_locations:
            util.dump_pickle(self.cached_locations, self.cached_locations_path)
        plt.show()

    def generate_report(self):
        # TODO: complete method
        pass

    def get_continent(self, country):
        assert country is not None, "country is None in get_continent()"
        if country in self.countries:
            return self.countries[country]["continent"]
        else:
            # TODO: test else clause
            ipdb.set_trace()
            return None


def get_top_k_locations(locations, k):
    assert type(locations) == list, "get_top_k_locations(): locations must be a list of tuples"
    locations = np.array(locations)
    count = locations[:, 1].astype(np.int32)
    sorted_indices = np.argsort(count)[::-1]
    return locations[sorted_indices][:k]


def exit_script(msg, code=1):
    print(msg)
    print("Exiting...")
    sys.exit(code)


if __name__ == '__main__':
    data_analyzer = JobDataAnalyzer()
    data_analyzer.run_analysis()
    ipdb.set_trace()
