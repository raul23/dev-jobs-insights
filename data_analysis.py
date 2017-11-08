import os
import sqlite3
import ipdb

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np


DB_FILENAME = os.path.expanduser("~/databases/jobs_insights.sqlite")


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


if __name__ == '__main__':
    ipdb.set_trace()
    conn = create_connection(DB_FILENAME)
    with conn:
        # Analysis of Stackoverflow dev jobs postings

        # 1. Analysis of tags (technologies)
        # Get all tags
        tags = select_all_tags(conn)
        # For each tag, count how many they are
        tags_times = {}
        # TODO: optimization, too many requests to the db to get the count of each tag
        # can you do it all in one request?
        for tag in tags:
            # Since `tags` is a list of tuple
            tag = tag[0]
            n_times = count_tag(conn, (tag,))
            tags_times[tag] = n_times[0]

        # Sort tags in order of decreasing occurrences (i.e. most popular at first)
        # NOTE: these are all the tags (even those that don't a salary associated
        # with), i.e. do not confuse `sorted_tags` with `tags_with_salary`
        # TODO: rename `sorted_tags` to `all_tags_sorted` to be able to differentiate it with `tags_with_salary`
        # which refer only to tags that have a salary associated with
        sorted_tags = sorted(tags_times.items(), key=lambda x: x[1], reverse=True)
        sorted_tags = np.array(sorted_tags)

        # 3.1 location analysis
        # NOTE: bar chart for categorical data
        # Bar chart: locations (by countries and by US states) vs number of job posts
        ipdb.set_trace()
        ax = plt.gca()

        """
        index = np.arange(5)
        values1 = [5, 7, 3, 4, 6]
        plt.bar(index, values1)
        plt.xticks(index, ['B', 'D', 'A', 'D', 'E'])
        plt.show()
        ipdb.set_trace()
        """

        index = np.arange(len(sorted_tags[:20, 0]))
        plt.bar(index, sorted_tags[:20, 1].astype(np.int64))
        plt.xticks(index, sorted_tags[:20, 0])
        # TODO: we only have to call it once
        ax.set_xlabel('Skills (tags)')
        ax.set_ylabel('Number of jobs')
        ax.set_title('Top 20 skills')
        labels = ax.get_xticklabels()
        plt.setp(labels, rotation=270.)
        ax.yaxis.set_major_locator(ticker.MultipleLocator(20))
        ax.yaxis.set_minor_locator(ticker.MultipleLocator(10))
        plt.grid(True, which="major")
        plt.tight_layout()
        plt.show()
        ipdb.set_trace()

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
        tags_salaries = np.hstack((tags_with_salary, salary_of_tags, counts_of_tags))

        # 3. Frequency analysis

        # 3.2 tags analysis
        # Bar chart: tags vs number of job posts
        # TODO: maybe take the first top 20 tags because there are so many tags they will not all fit

        # 3.3.


        # 4. Analysis of industries and tags
        # For each industries, get all tags that are related to the given industry