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
    sql = """SELECT value FROM job_salary WHERE name LIKE 'min%'"""
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def select_all_max_salaries(conn):
    """
    Returns all maximum salaries

    :param conn:
    :return:
    """
    sql = """SELECT value FROM job_salary WHERE name LIKE 'max%'"""
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
        # Return list of maxmimum salary
        max_salaries = select_all_max_salaries(conn)
        # Return list of minimum salary
        min_salaries = select_all_min_salaries(conn)
        # Compute mid-range for each min-max interval
        mid_ranges = np.hstack((min_salaries, max_salaries))

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
