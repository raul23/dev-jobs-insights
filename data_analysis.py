import os
import sqlite3
import ipdb

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
        for tag in tags:
            # Since `tags` is a list of tuple
            tag = tag[0]
            n_times = count_tag(conn, (tag,))
            tags_times[tag] = n_times

        # Sort tags in order of decreasing occurrences (i.e. most popular at first)
        sorted_tags = sorted(tags_times.items(), key=lambda x: x[1], reverse=True)

        ipdb.set_trace()

        # 2. Analysis of salary
        # Average, Max, Min salary, STD (mode, median)
        # Histogram: salary range and frequency, spot outliers (e.g. extremely low salary)
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
        job_ids = job_ids_1
        del job_ids_2

        # Reshape min-max salaries arrays
        min_salaries = min_salaries.reshape((len(min_salaries), 1))
        max_salaries = max_salaries.reshape((len(max_salaries), 1))

        ipdb.set_trace()

        # Compute salary mid-range for each min-max interval
        salary_ranges = np.hstack((min_salaries, max_salaries))
        salary_mid_ranges = salary_ranges.mean(axis=1)
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

        # TODO: we use the first method because we want to be able to quickly determin the second, third, ...
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



        # Salary by country: location (job_posts), job post might not have location; lots
        #                    of similar locations (e.g. Barcelona, Spanien and Barcelona, Spain or
        #                    Montreal, QC, Canada and Montréal, QC, Canada)

        # Salary by US states
        # Salary by job_overview: "Company size", "Company type", "Experience level",
        #                         "Industry", "Job type", "Role"
        # Salary by job_perks: "relocation", "remote", "salary", "visa"
        # Salary by tags: e.g. android, java, python


        # 3. Analysis of locations
        # Bar chart: locations (by countries and by US states) vs number of job posts
