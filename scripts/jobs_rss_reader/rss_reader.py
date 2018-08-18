import os
import sys

import feedparser

from entry import Entry
from feed import Feed
# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
from utility import genutil as g_util


DB_FILEPATH = os.path.expanduser("~/databases/dev_jobs_insights.sqlite")


class RSSReader:
    def __init__(self, autocommit=False):
        self.autocommit = autocommit
        # Create db connection
        self.conn = None
        # Current feed URL being parsed
        self.feed_url = None

    def read(self, feed_url):
        self.feed_url = feed_url
        self.conn = g_util.connect_db(DB_FILEPATH)
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
        if self.select_feed((feed.name,)) is None:
            # Insert current feed in db
            self.insert_feed((feed.name, feed.title, feed.updated))
        else:
            print("[INFO] The feed '{}' is already in the database.".format(feed.name))

    def process_entries(self, entries_list):
        # `entries_list` is a list of dict (of entries)
        for entry_dict in entries_list:
            # Process the entry
            entry = self.process_entry(entry_dict)
            if entry:
                # Process tags associated with the entry
                self.process_tags(entry)

    def process_entry(self, entry_dict):
        # Parse the given entry
        entry = Entry(self.feed_url, entry_dict)
        # Check if entry has an id
        if entry.id:
            # Check if the current entry is already in the db
            if self.select_entry((entry.id,)) is None:  # New entry
                # Sanity check if the `feed_name` (entries foreign key) is already
                # in the Feeds table
                # NOTE: the `feed_name` is a very important attribute for the entries table
                # because it is a foreign key that links both the feeds and entries tables
                if self.select_feed((entry.feed_name,)):  # feed found
                    print("[INFO] The entry '{}' will be inserted in the database.".format(entry.title))
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
                    print("[ERROR] The feed '{}' is not found in the db. Entry '{}' "
                          "can not be processed any further.".format(entry.feed_name, entry.title))
            else:
                print("[INFO] The entry '{}' is already in the database.".format(entry.title))
        else:
            print("[ERROR] The entry {} doesn't have an id. It belongs to the feed '{}' "
                  "and the entry will not be processed any further.".format(entry.title, entry.feed_name))
        return None

    def process_tags(self, entry):
        for tag in entry.tags:
            # Check if tag is already in db
            if self.select_tag((entry.id, tag,)) is None:
                # Insert tag in db
                self.insert_tag((entry.id, tag,))
            else:
                print("[INFO] The tag '{}' is already in the database.".format(tag))

    def insert_entry(self, entry):
        sql = '''INSERT INTO entries VALUES (?,?,?,?,?,?,?)'''
        self.sanity_check_sql(entry, sql)
        cur = self.conn.cursor()
        cur.execute(sql, entry)
        self.commit()
        return cur.lastrowid

    def insert_feed(self, feed):
        sql = '''INSERT INTO feeds (name, title, updated) VALUES (?,?,?)'''
        self.sanity_check_sql(feed, sql)
        cur = self.conn.cursor()
        cur.execute(sql, feed)
        self.commit()
        return cur.lastrowid

    def insert_tag(self, tag):
        sql = '''INSERT INTO tags VALUES (?,?)'''
        self.sanity_check_sql(tag, sql)
        cur = self.conn.cursor()
        cur.execute(sql, tag)
        self.commit()
        return cur.lastrowid

    def select_entry(self, entry):
        sql = '''SELECT * FROM entries WHERE job_id=?'''
        self.sanity_check_sql(entry, sql)
        cur = self.conn.cursor()
        cur.execute(sql, entry)
        return cur.fetchone()

    def select_feed(self, feed):
        sql = '''SELECT * FROM feeds WHERE name=?'''
        self.sanity_check_sql(feed, sql)
        cur = self.conn.cursor()
        cur.execute(sql, feed)
        return cur.fetchone()

    def select_tag(self, tag):
        sql = '''SELECT * FROM tags WHERE job_id=? AND name=?'''
        self.sanity_check_sql(tag, sql)
        cur = self.conn.cursor()
        cur.execute(sql, tag)
        return cur.fetchone()

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


if __name__ == '__main__':
    # List of RSS Feeds:
    # - For more recent RSS feeds: https://stackoverflow.com/jobs/feed
    # - Downloaded RSS feeds:
    rss_feeds = ["/Users/nova/data/dev_jobs_insights/cache/rss_feeds/2018-08-09 - developer jobs - Stack Overflow.xhtml"]

    rss_reader = RSSReader()

    for feed_url in rss_feeds:
        rss_reader.read(feed_url)
