import os
import sqlite3

import feedparser
import ipdb

from feeds_classes import Entry, Feed


DB_FILENAME = os.path.expanduser("~/databases/jobs_insights.sqlite")


class RSSReader:

    def __init__(self, autocommit=False):
        self.autocommit = False
        # Create db connection
        self.conn = None
        # Current feed URL being parsed
        self.current_feed_url = None

    def submit_feed(self, feed_url):
        self.current_feed_url = feed_url
        self.conn = create_connection(DB_FILENAME)
        with self.conn:
            # Parse RSS feed
            feed_parser_dict = feedparser.parse(feed_url)
            # Process feed
            self.process_feed(feed_parser_dict.feed)
            # Process entries
            self.process_entries(feed_parser_dict.entries)

    def process_feed(self, feed_dict):
        # Parse the feed dict and build a Feed instance from it
        feed = Feed(self.current_feed_url, feed_dict)
        # Check if the current feed is already in the db
        row = self.select_feed((feed.name,))
        if row is None:  # feed not found
            # Insert current feed in db
            self.insert_feed((feed.name, feed.title, feed.updated))
        else:
            # TODO: do we make use of the feed's date?
            pass
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
            # Check if there are new entries
            # Process entry
            entry = self.process_entry(entry_dict)
            if entry:
                # Process tag associated with entry
                self.process_tag(entry)


    def process_entry(self, entry_dict):
        # Parse each entries and build an Entry instance
        # from each entry
        entry = Entry(self.current_feed_url, entry_dict)
        # Check if entry has an id
        if entry.id:
            # Check if the current entry is already in the db
            row = self.select_entry((entry.id,))
            if row is None:  # New entry
                # Sanity check if the `feed_name` (entries' foreign key) is already
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
                          "can not be further processed.".format(entry.feed_name, entry.title))
            else:
                print("INFO: the entry '{}' is already in the database.".format(entry.title))
        else:
            print("ERROR: the entry {} doesn't have an id. It belongs to the feed '{}' "
                  "and the entry will not be further processed.".format(entry.title, entry.feed_name))
        return None

    def process_tag(self, entry):
        for tag in entry.tags:
            # Check if tag is already in db
            row = self.select_tag((tag,))
            if row is None:  # New tag
                # Insert tag in db
                self.insert_tag((tag,))
            # Check if there is already an entry-tag in the entries_tags table
            row = self.select_entry_tag((entry.id, tag,))
            if row is None:  # New entry_tag
                # Insert into entries_tags association table
                self.insert_entry_tag((entry.id, tag,))
            else:
                print("INFO: the tag '{}' is already in the database.".format(tag))

    def select_feed(self, feed):
        # Sanity check
        assert type(feed) is tuple
        sql = '''SELECT * FROM feeds WHERE name=?'''
        cur = self.conn.cursor()
        cur.execute(sql, feed)
        return cur.fetchone()

    def select_entry(self, entry):
        # Sanity check
        assert type(entry) is tuple
        sql = '''SELECT * FROM entries WHERE id=?'''
        cur = self.conn.cursor()
        cur.execute(sql, entry)
        return cur.fetchone()

    def select_tag(self, tag):
        # Sanity check
        assert type(tag) is tuple
        sql = '''SELECT * FROM tags WHERE name=?'''
        cur = self.conn.cursor()
        cur.execute(sql, tag)
        return cur.fetchone()

    def select_entry_tag(self, entry_tag):
        # Sanity check
        assert type(entry_tag) is tuple
        sql = '''SELECT * FROM entries_tags WHERE id=? AND name=?'''
        cur = self.conn.cursor()
        cur.execute(sql, entry_tag)
        return cur.fetchone()

    def insert_feed(self, feed):
        # Sanity check
        assert type(feed) is tuple
        sql = '''INSERT INTO feeds VALUES (?,?,?)'''
        cur = self.conn.cursor()
        cur.execute(sql, feed)
        self.commit()
        return cur.lastrowid

    def insert_entry(self, entry):
        # Sanity check
        assert type(entry) is tuple
        sql = '''INSERT INTO entries VALUES (?,?,?,?,?,?,?)'''
        cur = self.conn.cursor()
        cur.execute(sql, entry)
        self.commit()
        return cur.lastrowid

    def insert_tag(self, tag):
        # Sanity check
        assert type(tag) is tuple
        sql = '''INSERT INTO tags VALUES (?)'''
        cur = self.conn.cursor()
        cur.execute(sql, tag)
        self.commit()
        return cur.lastrowid

    def insert_entry_tag(self, entry_tag):
        # Sanity check
        assert type(entry_tag) is tuple
        sql = '''INSERT INTO entries_tags VALUES (?,?)'''
        cur = self.conn.cursor()
        cur.execute(sql, entry_tag)
        self.commit()
        return cur.lastrowid

    def update_feed(self, feed):
        # Sanity check
        assert type(feed) is tuple
        sql = '''UPDATE feeds SET updated=? WHERE name=?'''
        cur = self.conn.cursor()
        cur.execute(sql, feed)
        self.commit()

    def commit(self):
        """
        Wrapper to sqlite3.conn.commit()

        :return: None
        """
        if not self.autocommit:
            self.conn.commit()


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


if __name__ == '__main__':
    # List of RSS Feeds
    #rss_feeds = ["https://stackoverflow.com/jobs/feed"]
    rss_feeds = ["/Users/nope/Downloads/websites_downloaded/developer_jobs_stackoverflow/version2_xhtml_only/2017-10-26 - developer jobs - Stack Overflow.xhtml"]

    rss_reader = RSSReader()

    for feed_url in rss_feeds:
        rss_reader.submit_feed(feed_url)
