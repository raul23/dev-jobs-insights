import argparse
import os
import sqlite3
import sys
import time
import ipdb


DB_FILENAME = os.path.expanduser("~/databases/jobs_insights.sqlite")
SCHEMA_FILENAME = "jobs_insights_schema.sql"


if __name__ == '__main__':
    # Setup argument parser
    parser = argparse.ArgumentParser()

    parser = argparse.ArgumentParser(description="Create SQLite database %s" %DB_FILENAME)
    parser.add_argument("-o", action="store_true", dest="overwrite",
                        default=False,
                        help="Overwrite the db file %s" %DB_FILENAME)

    # Process command-line arguments
    results = parser.parse_args()

    db_is_new = not os.path.exists(DB_FILENAME)

    if results.overwrite and not db_is_new:
        print("WARNING: %s will be overwritten" % DB_FILENAME)
        time.sleep(5)
        os.remove(DB_FILENAME)

    with sqlite3.connect(DB_FILENAME) as conn:
        if db_is_new or results.overwrite:
            print("Creating db")
            try:
                with open(SCHEMA_FILENAME, 'rt') as f:
                    schema = f.read()
                    conn.executescript(schema)
            except IOError as e:
                print("I/O error({0}): {1}".format(e.errno, e.strerror))
            except:  # Handle other exceptions such as attribute errors
                print("Unexpected error:", sys.exc_info()[0])
        else:
            print("Database exists")
