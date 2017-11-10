"""
Data analysis of Stackoverflow developer jobs postings
"""
import os
import pickle
import sqlite3
import time
import ipdb

import geopy
from geopy.geocoders import Nominatim
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from mpl_toolkits.basemap import Basemap
import numpy as np
import plotly
from plotly.graph_objs import Scatter, Figure, Layout


# TODO: the following variables should be set in a config file
DB_FILENAME = os.path.expanduser("~/databases/jobs_insights.sqlite")
SHAPE_FILENAME = os.path.expanduser("~/data/basemap/st99_d00")
# Number of seconds to wait between two requests to the geocoding service
SLEEP_TIME = 1


# TODO: utility function
def create_connection(db_file, autocommit=False):
    """
    Creates a database connection to the SQLite database specified by the db_file

    :param db_file: database file
    :param autocommit: TODO
    :return: Connection object or None
    """
    try:
        if autocommit:
            conn = sqlite3.connect(db_file, isolation_level=None)
        else:
            conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(e)

    return None


def select_all_tags(conn):
    """
    Returns all tags

    :param conn:
    :return:
    """
    sql = '''SELECT * FROM tags'''
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def select_all_min_salaries(conn):
    """
    Returns all minimum salaries

    :param conn:
    :return:
    """
    sql = """SELECT job_id, value FROM job_salary WHERE name LIKE 'min%'"""
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def select_all_max_salaries(conn):
    """
    Returns all maximum salaries

    :param conn:
    :return:
    """
    sql = """SELECT job_id, value FROM job_salary WHERE name LIKE 'max%'"""
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def select_entries_tags(conn, job_ids):
    # TODO: change function name
    # Sanity check
    assert type(job_ids) is tuple
    sql = '''SELECT * FROM entries_tags WHERE id IN ({})'''
    placeholders = list(len(job_ids)*"?")
    placeholders = str(placeholders)
    placeholders = placeholders.replace("[", "")
    placeholders = placeholders.replace("]", "")
    placeholders = placeholders.replace("'", "")
    sql = sql.format(placeholders)
    cur = conn.cursor()
    cur.execute(sql, job_ids)
    return cur.fetchall()


def select_industries(conn, job_ids):
    # Sanity check
    assert type(job_ids) is tuple
    # TODO: factorization, almost same code (sql changes) as select_entries_tags()
    sql = '''SELECT job_id, value FROM job_overview WHERE job_id IN ({}) AND name="Industry"'''
    placeholders = list(len(job_ids)*"?")
    placeholders = str(placeholders)
    placeholders = placeholders.replace("[", "")
    placeholders = placeholders.replace("]", "")
    placeholders = placeholders.replace("'", "")
    sql = sql.format(placeholders)
    cur = conn.cursor()
    cur.execute(sql, job_ids)
    return cur.fetchall()


def select_roles(conn, job_ids):
    # Sanity check
    assert type(job_ids) is tuple
    # TODO: factorization, almost same code (sql changes) as select_entries_tags()
    sql = '''SELECT job_id, value FROM job_overview WHERE job_id IN ({}) AND name="Role"'''
    placeholders = list(len(job_ids)*"?")
    placeholders = str(placeholders)
    placeholders = placeholders.replace("[", "")
    placeholders = placeholders.replace("]", "")
    placeholders = placeholders.replace("'", "")
    sql = sql.format(placeholders)
    cur = conn.cursor()
    cur.execute(sql, job_ids)
    return cur.fetchall()


def select_locations(conn, job_ids):
    # Sanity check
    assert type(job_ids) is tuple
    # TODO: factorization, almost same code (sql changes) as select_entries_tags()
    sql = '''SELECT id, location FROM job_posts WHERE id IN ({})'''
    placeholders = list(len(job_ids)*"?")
    placeholders = str(placeholders)
    placeholders = placeholders.replace("[", "")
    placeholders = placeholders.replace("]", "")
    placeholders = placeholders.replace("'", "")
    sql = sql.format(placeholders)
    cur = conn.cursor()
    cur.execute(sql, job_ids)
    return cur.fetchall()


# TODO: not used anymore
def count_tag(conn, tag):
    """
    For a given tag, count the number of its occurrences in the `entries_tags` table

    :param conn:
    :return:
    """
    # Sanity check
    assert type(tag) is tuple
    sql = '''SELECT COUNT(name) FROM entries_tags WHERE name=?'''
    cur = conn.cursor()
    cur.execute(sql, tag)
    return cur.fetchone()


def count_tag_occurrences(conn):
    """
    For a given tag, count the number of its occurrences in the `entries_tags` table

    :param conn: sqlite3.Connection object
    :return: list of tuples of the form (tag_name, count)
    """
    sql = '''SELECT name, COUNT(name) as CountOf FROM entries_tags GROUP BY name ORDER BY CountOf DESC'''
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def count_location_occurrences(conn):
    """
    For a given location, count the number of its occurrences in the `job_posts` table

    :param conn: sqlite3.Connection object
    :return: list of tuples of the form (location, count)
    """
    sql = '''SELECT location, COUNT(*) as CountOf FROM job_posts GROUP BY location ORDER BY CountOf DESC'''
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def count_industry_occurrences(conn):
    """
    For a given industry, count the number of its occurrences in the `job_overview` table

    :param conn:
    :return:
    """
    sql = '''SELECT value, COUNT(*) as CountOf FROM job_overview WHERE name='Industry' GROUP BY value ORDER BY CountOf DESC'''
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def process_locations():
    pass


def analyze_tags(conn):
    pass


def analyze_locations(conn):
    pass


def analyze_industries(conn):
    pass


def analyze_roles(conn):
    pass


# TODO: add in Utility
def open_pickle(path):
    """
    Opens a pickle file and returns its contents or None if file not found.

    :param path: path to the pickle file
    :return: content of the pickle file or None if error
    """
    try:
        with open(path, "rb") as f:
            data = pickle.load(f)
    except FileNotFoundError as e:
        print(e)
        return None
    return data


# TODO: add in Utility
def dump_pickle(path, data):
    """
    Dumps a pickle file on disk and returns 0 if everything went right or None
    if file not found.

    :param path: path to the pickle file where data will be written
    :param data: data to be saved on disk
    :return: 0 if success or None if error
    """
    try:
        with open(path, "wb") as f:
            data = pickle.dump(path, f)
    except FileNotFoundError as e:
        print(e)
        return None
    return 0


def is_a_us_state(location):
    """
    Returns True if the location refers to a US state and False otherwise.
    NOTE: the US state must be in the form of two letters, i.e. it follows the
    ISO-? format TODO: add the correct ISO number
    NOTE: it is an extremely simple parsing method where we assume that the
    locations in Stackoverflow job posts provide two letters for US states only
    (except for UK) but it is good enough for our needs; thus it is not robust
    if we use with other job sites for instance
    TODO: make it more robust by retrieving a list of US states in the ISO-?
    format and comparing `location` against the list

    :param location: string of the location to check
    :return bool: True if it is a US state or False otherwise
    """
    # Sanity check
    assert location.find(",") == -1, "The location ({}) given to is_a_us_state() " \
                                     "contains a comma"
    # NOTE: the location can refer to a country (e.g. Seongnam-si, South Korea)
    # or to a US state (e.g. Portland, OR). Usually, if the last part of the
    # location string consist of two letter in capital, it refers to a US
    # state; however we must take into account 'UK'
    if len(location) == 2 and location != "UK":
        return True
    else:
        return False


def is_location_present(location):
    """
    Returns True if `location` refers to a location or False otherwise.
    A valid location is one that doesn't refer to `None` or "No office location"
    which are the two options in Stackoverflow job posts for cases where there is
    no location given for a job post.

    TODO: Does No office location only refers to the case where the job post is
    for a remote job?

    :param location: string of the location to check
    :return bool: True if it is a valid location or False otherwise
    """
    if location in [None, "No office location"]:
        return False
    else:
        return True


# Translation of a location to english
# NOTE: some locations are not given in English and we work with its english
# counterpart only
location_english_translation = {"Deutschland": "Germany",
                                "Spanien": "Spain",
                                "Österreich": "Austria",
                                "Schweiz": "Switzerland"}


if __name__ == '__main__':
    # TODO: don't forget to delete big variables if you don't use them anymore
    conn = create_connection(DB_FILENAME)
    with conn:
        # 1. Tags analysis (i.e. technologies such as java, python)
        # Get counts of tags, i.e. for each tag we want to know its number of
        # occurrences in job postings
        results = count_tag_occurrences(conn)
        # Convert the result (from the SQL SELECT request) as a dict
        results = dict(results)
        # Sort tags in order of decreasing number of occurrences (i.e. most
        # popular at first)
        # NOTE: these are all the tags (even those that don't a salary associated
        # with), i.e. do not confuse `all_tags_sorted` with `tags_with_salary`
        # TODO: is it efficient to sort the dict or a numpy array? Check also
        # other places where a dict is sorted instead of a numpy array
        all_tags_sorted_tags = sorted(results.items(), key=lambda x: x[1], reverse=True)
        all_tags_sorted_tags = np.array(all_tags_sorted_tags)

        # 2. Locations analysis
        # Get counts of job posts for each location, i.e. for each tag we want
        # to know its number of occurrences in job postings
        results = count_location_occurrences(conn)
        # Process the results
        countries_to_count = {}
        us_states_to_count = {}
        for location, count in results:
            # TODO: factorization, same code as in location-salary case
            # Check if 'No location' or empty for the location
            if is_location_present(location):
                # Get country or US state from the location string
                # NOTE: in most cases, location is of the form 'Berlin, Germany'
                # where country is given at the end after the comma
                last_part = location.split(",")[-1].strip()
                location = last_part
                # Is the location referring to a country or a US state?
                ipdb.set_trace()
                if is_a_us_state(last_part):
                    # The location string refers to a US state
                    # Save the location and its count (i.e. number of occurrences
                    # in job posts)
                    us_states_to_count.setdefault(location, 0)
                    us_states_to_count[location] += count
                    # Also since it is a US state, save the 'USA' and its count
                    # (i.e. number of occurrences in job posts)
                    # NOTE: the location string for a US state is given
                    # without the country at the end, e.g. Fort Meade, MD
                    location = "USA"
                    countries_to_count.setdefault(location, 0)
                    countries_to_count[location] += count
                else:
                    # The location string refers to a country
                    # Check for countries written in other languages, and keep
                    # only the english translation only
                    # NOTE: sometimes, a country is given in English or another
                    # language, e.g. Deutschland and Germany
                    if location in location_english_translation:
                        # Get the english translation of the given location
                        location = location_english_translation[location]
                    # Save the location and its count (i.e. number of occurences
                    # in job posts)
                    countries_to_count.setdefault(location, 0)
                    countries_to_count[location] += count
            else:
                # NOTE: We ignore the case where the location string is empty (None)
                # or refers to "No office location"
                # TODO: replace pass with logging
                pass

        # Sort the countries and USA-states dict based on the number of
        # occurrences, i.e. the dict's values. And convert the sorted dicts
        # into a numpy array
        sorted_countries_count = sorted(countries_to_count.items(), key=lambda x: x[1], reverse=True)
        sorted_countries_count = np.array(sorted_countries_count)
        sorted_us_states_count = sorted(us_states_to_count.items(), key=lambda x: x[1], reverse=True)
        sorted_us_states_count = np.array(sorted_us_states_count)
        # Delete the two dicts that we will not use anymore afterward
        del countries_to_count
        del us_states_to_count
        ipdb.set_trace()

        # MAP: Add locations on a map of the World
        # Load the cached locations' longitude and latitude if they were already
        # computed in a previous session with the geocoding service
        cached_locations = open_pickle("cached_locations.pkl")
        if cached_locations is None:
            # No cached location computations found
            cached_locations = {}
        # We are using the module `geopy` to get the longitude and latitude of
        # locations which will then be transformed into map coordinates so we can
        # draw markers on a map with `basemap`
        geolocator = Nominatim()

        # TODO: Annotate the top 5 locations (display the location names) for
        # example in the Europe and USA cases (not the Worldwide case because not
        # enough space to annotate)

        # Case 1: US states
        # TODO: also do map for Europe
        # TODO: only draw markers on US territory, not in Canada
        # `scale` should be set in a config file
        scale = 5
        # TODO: find out the complete name of the map projection used
        # We are using the Lambert ... map projection and cropping the map to
        # display the USA territory
        map = Basemap(llcrnrlon=-119, llcrnrlat=22, urcrnrlon=-64, urcrnrlat=49,
                      projection='lcc', lat_1=32, lat_2=45, lon_0=-95)
        map.readshapefile(SHAPE_FILENAME, name="states", drawbounds=True)

        # Used to display progress on the terminal
        n_result = 1
        for location, count in results:
            print("{}/{}".format(n_result, len(results)))
            n_result += 1
            # Check if we aleady computed the
            if location in cached_locations:
                loc = cached_locations[location]
            elif is_location_present(location):
                # Get country or US state from the location string
                # NOTE: in most cases, location is of the form 'Berlin, Germany'
                # where country is given at the end after the comma
                last_part = location.split(",")[-1].strip()
                # Is the location referring to a country or a US state?
                if is_a_us_state(last_part):
                    # The location string refers to a US state
                    # Add 'USA' at the end of `location`, so the US state doesn't
                    # get confused with other regions, such as 'Westlake Village, CA'
                    # which might get linked to 'Westlake Village, Hamlet of Clairmont, Grande Prairie, Alberta'
                    # It should be linked to a region in California, not in Canada
                    location += ", USA"
                # Get the location's longitude and latitude coordinates
                try:
                    loc = geolocator.geocode(location)
                except geopy.exc.GeocoderTimedOut:
                    if dump_pickle("cached_locations.pkl", cached_locations) is None:
                        # TODO: replace pass with logging
                        pass
                    # TODO: do something when there is a connection error with the geocoding service
                    ipdb.set_trace()
                # Check if error with the geocoding service
                if loc is None:
                    # Could not retrieve the location's longitude and latitude coordinates
                    # For example, 'Khwaeng Phra Khanong Nuea, Thailand' returns
                    # nothing. Thus in this case we call the geocoder again but
                    # with the country only (e.g. 'Thailand')
                    # TODO: factorization, we are re-doing what we just did, should call a function that does all that
                    if ";" in location:
                        # The city 'Teunz, Germany; Kastl, Germany' causes problems
                        # because it is two cities; we must fix it at the source
                        # TODO: remove this hack, it should be done at the source
                        cities = location.split(";")
                        for c in cities:
                            c = c.strip()
                            if c in cached_locations:
                                loc = cached_locations[c]
                            else:
                                loc = geolocator.geocode(c)
                                cached_locations[c] = loc
                                time.sleep(SLEEP_TIME)
                            x, y = map(loc.longitude, loc.latitude)
                            map.plot(x, y, marker='o', color='Red', markersize=int(np.sqrt(count)) * scale)
                        continue
                    else:
                        # Take the last part (country) since the first part is not recognized
                        if last_part in cached_locations:
                            loc = cached_locations[last_part]
                        else:
                            time.sleep(SLEEP_TIME)
                            loc = geolocator.geocode(last_part)
                time.sleep(SLEEP_TIME)
                # TODO: we should not add city with USA at the end, since we have to add USA at the end everytime 
                # we are dealing with a US state like in the country case 2 below
                cached_locations[location] = loc
            else:
                # NOTE: We ignore the case where the location string is empty (None)
                # or refers to "No office location"
                # TODO: replace pass with logging
                pass
            x, y = map(loc.longitude, loc.latitude)
            map.plot(x, y, marker='o', color='Red', markersize=int(np.sqrt(count)) * scale)
        plt.show()

        # Case 2: Countries
        # the map, a Miller Cylindrical projection
        # TODO: uncomment
        """
        scale = 1.2
        map = Basemap(projection='mill',
                    llcrnrlon=-180., llcrnrlat=-60,
                    urcrnrlon=180., urcrnrlat=80.)

        # draw coast lines and fill the continents
        map.drawcoastlines()
        map.drawcountries()
        map.drawstates()
        map.fillcontinents()
        map.drawmapboundary()

        n_result = 1
        # TODO: factorization code already used for case 1: US states
        for (city, count) in results:
            print("{}/{}".format(n_result, len(results)))
            n_result += 1
            if city in [None, "No office location"]:
                continue
            elif city in cached_locations:
                loc = cached_locations[city]
            else:
                # Is it a US state?
                last_part = city.split(",")[-1].strip()
                if len(last_part) == 2 and last_part != "UK":
                    # It is a US state
                    # Add USA at the end, so the US state doesn't get confused with other regions, such as
                    # Westlake Village, CA' which might get linked to 'Westlake Village, Hamlet of Clairmont, Grande Prairie, Alberta'
                    # It should be linked to a region in California, not Canada
                    city += ", USA"
                if city in cached_locations:
                    loc = cached_locations[city]
                else:
                    if ";" in city:
                        cities = city.split(";")
                        for c in cities:
                            c = c.strip()
                            if c in cached_locations:
                                loc = cached_locations[c]
                            else:
                                ipdb.set_trace()
                            x, y = map(loc.longitude, loc.latitude)
                            map.plot(x, y, marker='o', color='Blue', markersize=1.5)
                        continue
                    else:
                        # Take the last part (country) since the first part is not recognized
                        if last_part in cached_locations:
                            loc = cached_locations[last_part]
                        else:
                            ipdb.set_trace()
            x, y = map(loc.longitude, loc.latitude)
            map.plot(x, y, marker='o', color='Blue', markersize=1.5)
        plt.show()
        ipdb.set_trace()
        """

        # NOTE: bar charts are for categorical data
        # Bar chart: countries vs number of job posts
        # TODO: uncomment to plot bar chart
        """
        ax = plt.gca()
        index = np.arange(len(sorted_countries_count))
        plt.bar(index, sorted_countries_count[:, 1].astype(np.int64))
        plt.xticks(index, sorted_countries_count[:, 0])
        ax.set_xlabel('Countries')
        ax.set_ylabel('Number of jobs')
        ax.set_title("Countries popularity")
        labels = ax.get_xticklabels()
        plt.setp(labels, rotation=270.)
        plt.tight_layout()
        plt.show()
        """

        # Pie chart: countries vs number of jobs
        # TODO: add other countries for countries with few job posts
        # TODO: add % for each country
        # TODO: uncomment to plot pie chart
        """
        ax = plt.gca()
        values = sorted_countries_count[:, 1].astype(np.int64)
        labels = sorted_countries_count[:, 0]
        plt.pie(values, labels=labels, autopct='%1.1f%%')
        ax.set_title("Countries popularity by number of job posts")
        plt.axis('equal')
        plt.show()
        """

        # Bar chart: us states vs number of job posts
        # TODO: uncomment
        """
        ax = plt.gca()
        index = np.arange(len(sorted_us_states_count))
        plt.bar(index, sorted_us_states_count[:, 1].astype(np.int64))
        plt.xticks(index, sorted_us_states_count[:, 0])
        ax.set_xlabel('US states')
        ax.set_ylabel('Number of jobs')
        ax.set_title("US states popularity")
        labels = ax.get_xticklabels()
        plt.setp(labels, rotation=270.)
        plt.tight_layout()
        # TODO: uncomment
        #plt.show()
        """


        # Pie chart: US states vs number of jobs
        # TODO: add other US states for US states with few job posts
        # TODO: add % for each US state
        # TODO: uncomment
        """
        ax = plt.gca()
        values = sorted_us_states_count[:, 1].astype(np.int64)
        labels = sorted_us_states_count[:, 0]
        plt.pie(values, labels=labels, autopct='%1.1f%%')
        ax.set_title("US states popularity by number of job posts")
        plt.axis('equal')
        # TODO: uncomment
        #plt.show()
        """


        # 3.2 tags analysis
        # Bar chart: tags vs number of job posts
        # TODO: maybe take the first top 20 tags because there are so many tags they will not all fit
        # TODO: uncomment
        """
        # TODO: we only have to call it once
        ax = plt.gca()
        # TODO: the top X should be a param
        index = np.arange(len(sorted_tags[:20, 0]))
        plt.bar(index, sorted_tags[:20, 1].astype(np.int64))
        plt.xticks(index, sorted_tags[:20, 0])
        ax.set_xlabel('Skills (tags)')
        ax.set_ylabel('Number of jobs')
        ax.set_title('Top 20 most popular skills')
        labels = ax.get_xticklabels()
        plt.setp(labels, rotation=270.)
        ax.yaxis.set_major_locator(ticker.MultipleLocator(20))
        ax.yaxis.set_minor_locator(ticker.MultipleLocator(10))
        plt.grid(True, which="major")
        plt.tight_layout()
        # TODO: uncomment
        #plt.show()
        """


        # 2. Analysis of salary
        # Average, Max, Min salary, STD (mode, median)
        # Return list of minimum salary
        min_salaries = select_all_min_salaries(conn)
        min_salaries = np.array(min_salaries)
        job_ids_1 = min_salaries[:, 0]
        min_salaries = min_salaries[:, 1].astype(np.float64)
        # Return list of maximum salary
        max_salaries = select_all_max_salaries(conn)
        max_salaries = np.array(max_salaries)
        job_ids_2 = max_salaries[:, 0]
        max_salaries = max_salaries[:, 1].astype(np.float64)
        # Sanity check on `job_ids_*`
        assert np.array_equal(job_ids_1, job_ids_2), "The two returned job_ids don't match"
        # TODO: change name to job_ids_with_salary
        job_ids = job_ids_1
        del job_ids_2

        # Reshape min-max salaries arrays
        min_salaries = min_salaries.reshape((len(min_salaries), 1))
        max_salaries = max_salaries.reshape((len(max_salaries), 1))

        # Compute salary mid-range for each min-max interval
        salary_ranges = np.hstack((min_salaries, max_salaries))
        # TODO: check precision for `salary_mid_ranges`
        salary_mid_ranges = salary_ranges.mean(axis=1)
        job_id_to_salary_ranges = dict(zip(job_ids, salary_mid_ranges))
        # Compute salary mean across list of mid-range salaries
        global_mean_salary = salary_mid_ranges.mean()
        # Precision to two decimals
        global_mean_salary = float(format(global_mean_salary, '.2f'))
        # Compute std across list of mid-range salaries
        global_std_salary = salary_mid_ranges.std()
        # Precision to two decimals
        global_std_salary = float(format(global_std_salary, '.2f'))
        # TODO: two methods to get max and min salaries along with their corresponding job_id's
        # TODO: first method follows
        # Get min and max salaries across list of mid-range salaries
        sorted_indices = np.argsort(salary_mid_ranges)
        salary_mid_ranges_sorted = salary_mid_ranges[sorted_indices]
        global_min_salary = salary_mid_ranges_sorted[0]
        global_max_salary = salary_mid_ranges_sorted[-1]
        # Get job_id's associated with these global min and max salaries
        min_index = sorted_indices[0]
        max_index = sorted_indices[-1]
        min_job_id = job_ids[min_index]  # TODO: e.g. 157792
        max_job_id = job_ids[max_index]  # TODO: e.g. 155189

        # TODO: get more information about these (min-max) jobs from their job_id's

        # TODO: we use the first method because we want to be able to quickly determine the second, third, ...
        # higher salaries in case the highest one ends up being an outlier
        # TODO: Second method follows ...
        """
        global_min_salary = salary_mid_ranges.min()
        global_max_salary = salary_mid_ranges.max()
        # Get job_id's associated with these global min and max salaries
        min_index = np.argmin(salary_mid_ranges)
        max_index = np.argmax(salary_mid_ranges)
        min_job_id = job_ids[min_index] # 157792
        max_job_id = job_ids[max_index]  # 155189
        """

        # Histogram: salary range and frequency, spot outliers (e.g. extremely low salary)
        # NOTE: histogram for continuous data
        # TODO: histogram for salary range, globally and in the USA only
        # Compute number of bins
        # TODO: get the second highest salary since the highest one is an outlier
        # TODO: do we use np.int32 or np.int64?
        # TODO: uncomment
        """
        n_bins = np.ceil((salary_mid_ranges_sorted[-2] - global_min_salary)/10000.).astype(np.int64)
        ax = plt.gca()
        # TODO: we remove the outlier corresponding to the highest value
        ax.hist(salary_mid_ranges_sorted[:-1], bins=n_bins, color='r')
        ax.set_xlabel('Mid-range salaries')
        ax.set_ylabel('Number of jobs')
        ax.set_title('Histogram: Mid-range salaries')
        ax.xaxis.set_major_locator(ticker.MultipleLocator(10000))
        #ax.xaxis.set_major_locator(ticker.MultipleLocator(10000))
        ax.yaxis.set_major_locator(ticker.MultipleLocator(5))
        ax.yaxis.set_minor_locator(ticker.MultipleLocator(1))
        plt.xlim(0, salary_mid_ranges_sorted[-2])
        labels = ax.get_xticklabels()
        plt.setp(labels, rotation=270.)
        plt.grid(True, which='major')
        plt.tight_layout()
        # TODO: add function to save image instead of showing it
        #plt.show()
        """

        # Salary by country: location (job_posts), job post might not have location; lots
        #                    of similar locations (e.g. Barcelona, Spanien and Barcelona, Spain or
        #                    Montreal, QC, Canada and Montréal, QC, Canada)
        # Select countries associated to job_id's with salaries
        # TODO: factorization same code as for 'Salary by tags' and other parts
        results = select_locations(conn, tuple(job_ids))
        # Process locations to extract countries or US states
        countries_salaries = {}
        us_states_salaries = {}
        for job_id, location in results:
            # Check if No location given
            if location in [None, "No office location"]:
                continue
            else:
                # Get country or US state
                last_part = location.split(",")[-1].strip()
                # Is it a country or a US state?
                if len(last_part) > 2 or last_part == "UK":
                    # It is a country
                    # TODO: the following line is also in the else and should be place before the current if
                    location = last_part

                    # Check for similar countries written in other languages
                    if location == "Deutschland":
                        location = "Germany"
                    elif location == "Spanien":
                        location = "Spain"
                    elif location == "Österreich":
                        location = "Austria"
                    elif location == "Schweiz":
                        location = "Switzerland"
                    else:
                        # TODO: Add an assert to test that you are not getting a never seen location
                        # maybe retrieve a list
                        pass

                    countries_salaries.setdefault(location, [0, 0, 0])
                    mid_range_salary = job_id_to_salary_ranges[job_id]
                    countries_salaries[location][2] += 1  # update count
                    cum_sum = countries_salaries[location][1]
                    # # TODO: add precision for average salary
                    countries_salaries[location][0] = (cum_sum + mid_range_salary) / countries_salaries[location][2]  # update average
                    countries_salaries[location][1] += mid_range_salary  # update cumulative sum
                else:
                    # It is a US state
                    # TODO: factorization, same code as for the if case (the countries case)
                    location = last_part
                    us_states_salaries.setdefault(location, [0, 0, 0])
                    mid_range_salary = job_id_to_salary_ranges[job_id]
                    us_states_salaries[location][2] += 1  # update count
                    cum_sum = us_states_salaries[location][1]
                    us_states_salaries[location][0] = (cum_sum + mid_range_salary) / us_states_salaries[location][2]  # update average
                    us_states_salaries[location][1] += mid_range_salary  # update cumulative sum

                    # Add the US state to the countries dict also
                    # TODO: factorization, same code as for the US states (see previously)
                    location = "USA"
                    countries_salaries.setdefault(location, [0, 0, 0])
                    mid_range_salary = job_id_to_salary_ranges[job_id]
                    countries_salaries[location][2] += 1  # update count
                    cum_sum = countries_salaries[location][1]
                    countries_salaries[location][0] = (cum_sum + mid_range_salary) / countries_salaries[location][2]  # update average
                    countries_salaries[location][1] += mid_range_salary  # update cumulative sum

        temp_array_1 = np.array([(k, v[0], v[2]) for k, v in countries_salaries.items()])
        temp_array_2 = np.array([(k, v[0], v[2]) for k, v in us_states_salaries.items()])

        # Salary by US states
        # TODO: get salary by US states

        # Salary by job_overview: "Company size", "Company type", "Experience level",
        #                         "Industry", "Job type", "Role"
        # TODO: get salary by company size (however, the value in the db must be further processed because
        # we don't readily have access to numerical values but strings, e.g. 1k-5k people; we must do the same
        # processing we applied to salary since we must also change 'k' to 1000
        # TODO: get salary by Company type (e.g. Private, VC Funded)
        # TODO: get salary by experience level (e.g. Mid-Level, Senior)
        # TODO: get salary by job type (i.e. Contract, Internship, Permanent)

        # Salary by job_perks: "relocation", "remote", "salary", "visa"
        # TODO: get salary by job_perks

        # Get salary by industry (e.g. Animation, Cloud Computing, Finance)
        # Select industries associated to job_id's with salaries
        # TODO: factorization same code as for 'Salary by tags' and other parts
        # TODO: specify that we are selecting industries that have a salary with it
        results = select_industries(conn, tuple(job_ids))
        results = np.array(results)
        industries_salaries = {}
        for job_id, industry in results:
            industries_salaries.setdefault(industry, [0, 0, 0])
            mid_range_salary = job_id_to_salary_ranges[job_id]
            industries_salaries[industry][2] += 1  # update count
            cum_sum = industries_salaries[industry][1]
            industries_salaries[industry][0] = (cum_sum + mid_range_salary) / industries_salaries[industry][2]  # update average
            industries_salaries[industry][1] += mid_range_salary  # update cumulative sum
        temp_array = np.array([(k, v[0], v[2]) for k, v in industries_salaries.items()])
        industries_with_salary = temp_array[:, 0]
        salary_of_industries = temp_array[:, 1].astype(np.float64)
        counts_of_industries = temp_array[:, 2].astype(np.int64)
        del temp_array
        sorted_indices = np.argsort(salary_of_industries)[::-1]
        industries_with_salary = industries_with_salary[sorted_indices]
        salary_of_industries = salary_of_industries[sorted_indices]
        counts_of_industries = counts_of_industries[sorted_indices]
        # Reshape ... TODO: finish comment
        industries_with_salary = industries_with_salary.reshape((len(industries_with_salary), 1))
        salary_of_industries = salary_of_industries.reshape((len(salary_of_industries), 1))
        counts_of_industries = counts_of_industries.reshape((len(counts_of_industries), 1))
        industries_salaries = np.hstack((industries_with_salary, salary_of_industries, counts_of_industries))

        # Get salary by role (e.g. Backend Developer, Mobile Developer)
        # Select roles associated to job_id's with salaries
        # TODO: factorization same code as for 'Salary by tags'
        results = select_roles(conn, tuple(job_ids))
        results = np.array(results)
        roles_salaries = {}
        for job_id, role in results:
            roles_salaries.setdefault(role, [0, 0, 0])
            mid_range_salary = job_id_to_salary_ranges[job_id]
            roles_salaries[role][2] += 1  # update count
            cum_sum = roles_salaries[role][1]
            roles_salaries[role][0] = (cum_sum + mid_range_salary) / roles_salaries[role][2]  # update average
            roles_salaries[role][1] += mid_range_salary  # update cumulative sum
        temp_array = np.array([(k, v[0], v[2]) for k, v in roles_salaries.items()])
        roles_with_salary = temp_array[:, 0]
        salary_of_roles = temp_array[:, 1].astype(np.float64)
        counts_of_roles = temp_array[:, 2].astype(np.int64)
        del temp_array
        sorted_indices = np.argsort(salary_of_roles)[::-1]
        roles_with_salary = roles_with_salary[sorted_indices]
        salary_of_roles = salary_of_roles[sorted_indices]
        counts_of_roles = counts_of_roles[sorted_indices]
        # Reshape ... TODO: finish comment
        roles_with_salary = roles_with_salary.reshape((len(roles_with_salary), 1))
        salary_of_roles = salary_of_roles.reshape((len(salary_of_roles), 1))
        counts_of_roles = counts_of_roles.reshape((len(counts_of_roles), 1))
        roles_salaries = np.hstack((roles_with_salary, salary_of_roles, counts_of_roles))

        # Salary by tags: e.g. android, java, python
        # Select tags associated to job_id's with salaries
        # TODO: factorization
        results = select_entries_tags(conn, tuple(job_ids))
        results = np.array(results)
        # TODO: not using the next two variables
        #entries_job_ids = results[:, 0]
        #entries_tags = results[:, 1]
        tags_salaries = {}
        for job_id, tag in results:
            tags_salaries.setdefault(tag, [0, 0, 0])
            mid_range_salary = job_id_to_salary_ranges[job_id]
            tags_salaries[tag][2] += 1  # update count
            cum_sum = tags_salaries[tag][1]
            tags_salaries[tag][0] = (cum_sum + mid_range_salary) / tags_salaries[tag][2]  # update average
            tags_salaries[tag][1] += mid_range_salary  # update cumulative sum
        temp_array = np.array([(k, v[0], v[2]) for k,v in tags_salaries.items()])
        tags_with_salary = temp_array[:, 0]
        salary_of_tags = temp_array[:, 1].astype(np.float64)
        counts_of_tags = temp_array[:, 2].astype(np.int64)
        del temp_array
        sorted_indices = np.argsort(salary_of_tags)[::-1]
        tags_with_salary = tags_with_salary[sorted_indices]
        salary_of_tags = salary_of_tags[sorted_indices]
        counts_of_tags = counts_of_tags[sorted_indices]
        # Reshape ... TODO: finish comment
        tags_with_salary = tags_with_salary.reshape((len(tags_with_salary), 1))
        salary_of_tags = salary_of_tags.reshape((len(salary_of_tags), 1))
        counts_of_tags = counts_of_tags.reshape((len(counts_of_tags), 1))
        # TODO: `tags_salaries` not used
        #tags_salaries = np.hstack((tags_with_salary, salary_of_tags, counts_of_tags))

        # Scatter plot: on the x-axis we have the number of job posts for a given
        # tag and on the y-axis we have the average mid-range salary for the given
        # tag
        # TODO: specify that this is only for tags that have a salary, there are alot more tags that don't have a salary
        # TODO: uncomment
        # TODO: removed the outlier, salary_of_tags[2:]
        """
        plotly.offline.plot({
            "data": [Scatter(x=list(counts_of_tags[2:].flatten()),
                             y=list(salary_of_tags[2:].flatten()),
                             mode='markers',
                             text=list(tags_with_salary[2:].flatten()))],
            "layout": Layout(title="Average mid-range salary of programming skills", hovermode='closest',
                             yaxis=dict(tickformat="$0.0f"))
        })
        """

        # 3.X Industries analysis
        # Bar chart: industries vs number of job posts
        # NOTE: these are all the industries even those that don't have a salary associated with
        # Get number of job posts for each industry
        # TODO: specify that the results are already sorted in decreasing order of industry's count, i.e.
        # from the most popular industry to the least one
        results = count_industry_occurrences(conn)
        # TODO: Process the results by summing the similar industries (e.g. Software Development with
        # Software Development / Engineering or eCommerce with E-Commerce)
        # TODO: use Software Development instead of the longer Software Development / Engineering
        results = np.array(results)
        # TODO: maybe take the first top 20 tags because there are so many industries they will not all fit
        # TODO: we only have to call it once
        ipdb.set_trace()
        ax = plt.gca()
        # TODO: the top X should be a param
        index = np.arange(len(results[:20, 0]))
        plt.bar(index, results[:20, 1].astype(np.int64))
        plt.xticks(index, results[:20, 0])
        ax.set_xlabel('Industries')
        ax.set_ylabel('Number of jobs')
        ax.set_title('Top 20 most popular industries')
        labels = ax.get_xticklabels()
        plt.setp(labels, rotation=270.)
        ax.yaxis.set_major_locator(ticker.MultipleLocator(20))
        ax.yaxis.set_minor_locator(ticker.MultipleLocator(10))
        plt.grid(True, which="major")
        plt.tight_layout()
        # TODO: uncomment
        #plt.show()

        # Scatter plot: on the x-axis we have the number of job posts for a given
        # industry and on the y-axis we have the average mid-range salary for the given
        # industry
        # TODO: specify that this is only for tags that have a salary, there are alot more tags that don't have a salary
        # TODO: uncomment
        # TODO: removed the outlier, salary_of_industries[1:]
        # TODO: do also a scatter plot for job role vs number of job posts
        """
        plotly.offline.plot({
            "data": [Scatter(x=list(counts_of_industries[1:].flatten()),
                             y=list(salary_of_industries[1:].flatten()),
                             mode='markers',
                             text=list(industries_with_salary[1:].flatten()))],
            "layout": Layout(title="Average mid-range salary of industries", hovermode='closest',
                             yaxis=dict(tickformat="$0.0f"))
        })
        """
        ipdb.set_trace()


        # 3. Frequency analysis
        # TODO: bring all code here

        # 4. Analysis of industries and tags
        # For each industries, get all tags that are related to the given industry

        # 5. Add locations on a map of the World
        # TODO; bring all code here
        # Case 1: US states


        # Case 2: Countries
