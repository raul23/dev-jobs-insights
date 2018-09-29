import argparse
import os
import sqlite3
import time


DB_FILENAME = os.path.expanduser("~/databases/rss_feeds.sqlite")
SCHEMA_FILENAME = "rss_feeds_schema.sql"


if __name__ == '__main__':
    # Setup argument parser
    parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser(description="Create SQLite database '{}'".format(DB_FILENAME))
    parser.add_argument("-o", action="store_true", dest="overwrite",
                        default=False,
                        help="Overwrite the db file '{}'".format(DB_FILENAME))
    # Process command-line arguments
    results = parser.parse_args()

    db_is_new = not os.path.exists(DB_FILENAME)

    if results.overwrite and not db_is_new:
        print("WARNING: {} will be overwritten".format(DB_FILENAME))
        # Exit program before delay expires or the database is overwritten
        time.sleep(5)
        os.remove(DB_FILENAME)

    if db_is_new or results.overwrite:
        print("Creating database ...")
        with sqlite3.connect(DB_FILENAME) as conn:
            try:
                with open(SCHEMA_FILENAME, 'rt') as f:
                    schema = f.read()
                    conn.executescript(schema)
            except IOError as e:
                raise IOError(e)
            else:
                print("Database created!")
    else:
        print("Database '{}' already exists!".format(DB_FILENAME))
