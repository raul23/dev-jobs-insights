import os
import pickle
import sqlite3

import ipdb


DB_FILENAME = os.path.expanduser("~/databases/jobs_insights.sqlite")


# TODO: utility function
def create_connection(db_file, autocommit=False):
    """
    Creates a database connection to the SQLite database specified by the db_file

    :param db_file: database file
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


def validate_value(value):
    if "k" in value:



def append_items(prefix_item, input_items, output_items):
    for name, values in input_items.items():
        if type(values) is not list:
            values = [values]
        for val in values:
            for v in val.split(","):
                if name in ["company_size", "salary"]:

                output_items.append((prefix_item, name, v))


if __name__ == '__main__':
    ipdb.set_trace()
    conn = create_connection(DB_FILENAME)
    with conn:
        f = open("entries_data.pkl", "rb")
        data = pickle.load(f)
        f.close()

        job_posts = []
        job_perks = []
        job_overview = []

        for k, v in data.items():
            id = k
            author = v['author']
            link = v['link']
            location = v['location']
            job_posts.append((id, author, link, location))
            perks = v['perks']
            overview_items = v['overview_items']
            append_items(prefix_item=id, input_items=perks, output_items=job_perks)
            append_items(prefix_item=id, input_items=overview_items, output_items=job_overview)

        ipdb.set_trace()
        cur = conn.cursor()
        cur.executemany("INSERT INTO job_posts VALUES(?, ?, ?, ?)", job_posts)
        cur.executemany("INSERT INTO job_perks (job_id, name, value) VALUES(?, ?, ?)", job_perks)
        cur.executemany("INSERT INTO job_overview (job_id, name, value) VALUES(?, ?, ?)", job_overview)
        conn.commit()
