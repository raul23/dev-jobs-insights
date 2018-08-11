import os
import sqlite3
# Third-party code
import feedparser
# Own code
from feeds_classes import Entry, Feed


DB_FILENAME = os.path.expanduser("~/databases/dev_jobs_insights.sqlite")


class RSSReader:
    def __init__(self, autocommit=False):
        self.autocommit = autocommit
        # Create db connection
        self.conn = None
        # Current feed URL being parsed
        self.feed_url = None

    def submit_feed(self, feed_url):
        self.feed_url = feed_url
        self.conn = create_connection(DB_FILENAME)
        with self.conn:
            # Parse RSS feed
            feed_parser_dict = feedparser.parse(feed_url)
            # TODO: case where there is a parse exception, e.g. SAXParseException('not well-formed (invalid token)',)
            # feed and entries will both be empty in that case. Check by removing the file extension of the feed file
            # Process feed
            # TODO: another case you should check is when there is sqlite3.ProgrammingError, e.g. Incorrect number
            # of bindings supplied. The current statement uses 2, and there are 1 supplied. Check by removing
            # a field from a table
            self.process_feed(feed_parser_dict.feed)
            # Process entries
            self.process_entries(feed_parser_dict.entries)

    def process_feed(self, feed_dict):
        # Parse the feed dict
        feed = Feed(self.feed_url, feed_dict)
        # Check if the current feed is already in the db
        row = self.select_feed((feed.name,))
        if row is None:  # feed not found in the db
            # Insert current feed in db
            self.insert_feed((feed.name, feed.title, feed.updated))
        else:
            print("INFO: the feed '{}' is already in the database.".format(feed.name))
            # TODO: do we make use of the feed's date?
            # Check if there have been jobs updates
            # TODO: sanity check if updated not found
            '''
            if feed.updated > row[-1]:  # Jobs updates
                # Update the dates
                self.update_feed((feed.updated, feed.name))
            '''

    def process_entries(self, entries_list):
        # `entries_list` is a list of dict
        for entry_dict in entries_list:
            # Process the entry
            entry = self.process_entry(entry_dict)
            if entry:
                # Process tags associated with the entry
                self.process_tags(entry)

    def process_entry(self, entry_dict):
        # Parse the entry
        entry = Entry(self.feed_url, entry_dict)
        # Check if entry has an id
        if entry.id:
            # Check if the current entry is already in the db
            row = self.select_entry((entry.id,))
            if row is None:  # New entry
                # Sanity check if the `feed_name` (entries foreign key) is already
                # in the Feeds table
                # NOTE: the `feed_name` is a very important attribute for the entries table
                # because it is a foreign key that links both feeds and entries tables
                row = self.select_feed((entry.feed_name,))
                if row:  # feed found
                    print("INFO: the entry '{}' will be inserted in the database.".format(entry.title))
                    # Insert current entry in db
                    self.insert_entry((entry.id,
                                       entry.feed_name,
                                       entry.title,
                                       entry.author,
                                       entry.link,
                                       entry.summary,
                                       entry.published))
                    return entry
                else:
                    print("ERROR: the feed '{}' is not found in the db. Entry '{}' "
                          "can not be processed any further.".format(entry.feed_name, entry.title))
            else:
                print("INFO: the entry '{}' is already in the database.".format(entry.title))
        else:
            print("ERROR: the entry {} doesn't have an id. It belongs to the feed '{}' "
                  "and the entry will not be processed any further.".format(entry.title, entry.feed_name))
        return None

    def process_tags(self, entry):
        for tag in entry.tags:
            # Check if tag is already in db
            row = self.select_tag((entry.id, tag,))
            if row is None:  # New tag
                # Insert tag in db
                self.insert_tag((entry.id, tag,))
            else:
                print("INFO: the tag '{}' is already in the database.".format(tag))
            # TODO: next to be removed since we don't use the association table entries_tags, it had been
            # incorporated into the tags table
            """
            # Check if there is already an entry-tag in the entries_tags table
            row = self.select_entry_tag((entry.id, tag,))
            if row is None:  # New entry_tag
                # Insert into entries_tags association table
                self.insert_entry_tag((entry.id, tag,))
            else:
                print("INFO: the tag '{}' is already in the database.".format(tag))
            """

    def select_feed(self, feed):
        sql = '''SELECT * FROM feeds WHERE name=?'''
        self.sanity_check_sql(feed, sql)
        cur = self.conn.cursor()
        cur.execute(sql, feed)
        return cur.fetchone()

    def select_entry(self, entry):
        sql = '''SELECT * FROM entries WHERE job_id=?'''
        self.sanity_check_sql(entry, sql)
        cur = self.conn.cursor()
        cur.execute(sql, entry)
        return cur.fetchone()

    def select_tag(self, tag):
        sql = '''SELECT * FROM tags WHERE job_id=? AND name=?'''
        self.sanity_check_sql(tag, sql)
        cur = self.conn.cursor()
        cur.execute(sql, tag)
        return cur.fetchone()

    # TODO: to be removed since we don't use the association table entries_tags, it had been
    # incorporated into the tags table
    def select_entry_tag(self, entry_tag):
        sql = '''SELECT * FROM entries_tags WHERE job_id=? AND name=?'''
        self.sanity_check_sql(entry_tag, sql)
        cur = self.conn.cursor()
        cur.execute(sql, entry_tag)
        return cur.fetchone()

    def insert_feed(self, feed):
        sql = '''INSERT INTO feeds (name, title, updated) VALUES (?,?,?)'''
        self.sanity_check_sql(feed, sql)
        cur = self.conn.cursor()
        cur.execute(sql, feed)
        self.commit()
        return cur.lastrowid

    def insert_entry(self, entry):
        sql = '''INSERT INTO entries VALUES (?,?,?,?,?,?,?)'''
        self.sanity_check_sql(entry, sql)
        cur = self.conn.cursor()
        cur.execute(sql, entry)
        self.commit()
        return cur.lastrowid

    def insert_tag(self, tag):
        sql = '''INSERT INTO tags VALUES (?,?)'''
        self.sanity_check_sql(tag, sql)
        cur = self.conn.cursor()
        cur.execute(sql, tag)
        self.commit()
        return cur.lastrowid

    # TODO: to be removed since we don't use the association table entries_tags, it had been
    # incorporated into the tags table
    def insert_entry_tag(self, entry_tag):
        sql = '''INSERT INTO entries_tags VALUES (?,?)'''
        self.sanity_check_sql(entry_tag, sql)
        cur = self.conn.cursor()
        cur.execute(sql, entry_tag)
        self.commit()
        return cur.lastrowid

    def update_feed(self, feed):
        sql = '''UPDATE feeds SET updated=? WHERE name=?'''
        self.sanity_check_sql(feed, sql)
        cur = self.conn.cursor()
        cur.execute(sql, feed)
        self.commit()

    @staticmethod
    def sanity_check_sql(val, sql):
        assert type(val) is tuple
        assert len(val) == sql.count('?')

    def commit(self):
        """
        Wrapper to sqlite3.conn.commit()

        :return: None
        """
        if not self.autocommit:
            self.conn.commit()


# TODO: add to the utility package
def create_connection(db_file, autocommit=False):
    """
    Creates a database connection to the SQLite database specified by `db_file`

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


if __name__ == '__main__':
    # List of RSS Feeds
    # For more recent RSS feeds: https://stackoverflow.com/jobs/feed
    # Downloaded RSS feeds
    rss_feeds = ["/Users/nova/data/jobs_insights/2018-08-09 - developer jobs - Stack Overflow.xhtml"]

    rss_reader = RSSReader()

    for feed_url in rss_feeds:
        rss_reader.submit_feed(feed_url)
