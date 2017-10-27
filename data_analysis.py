import sqlite3
import ipdb


DB_FILENAME = "feeds.sqlite"


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
    sql = '''SELECT * FROM tags'''
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchone()


if __name__ == '__main__':
    ipdb.set_trace()
    conn = create_connection(DB_FILENAME)
    with conn:
        # Analysis of Stackoverflow dev jobs postings
        # 1. Analysis of tags (technologies)
        # For each tag, count how many they are
        # Select the number of unique tags
        n_tags = select_all_tags(conn)

        # 2. Analysis of