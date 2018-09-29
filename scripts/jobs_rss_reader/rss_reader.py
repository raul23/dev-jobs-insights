import argparse
import os
import sys
# Third-party modules
import ipdb
import feedparser
# Own modules
from entry import Entry
from exc import *
from feed import Feed
from utility.genutil import connect_db, read_yaml_config
from utility.script_boilerplate import ScriptBoilerplate


class RSSReader:
    def __init__(self, db_filepath, logging_cfg, logger, autocommit=False):
        self.db_filepath = db_filepath
        # Create db connection
        self.conn = connect_db(self.db_filepath)
        # Current feed's URL being parsed
        self.feed_url = None
        self.logging_cfg = logging_cfg
        self.logger = logger
        self.autocommit = autocommit

    def read(self, feed_url):
        self.feed_url = feed_url
        with self.conn:
            # ==============
            # Parse RSS feed
            # ==============
            feed_parser_dict = feedparser.parse(feed_url)
            # =============
            # Process feed
            # =============
            self.process_feed(feed_parser_dict.feed)
            # ===============
            # Process entries
            # ===============
            self.process_entries(feed_parser_dict.entries)

    def process_feed(self, feed_dict):
        # Parse the feed dict
        feed = Feed(self.feed_url, feed_dict, self.logging_cfg)
        # Check if the current feed is already in the db
        if self.select_feed((feed.name,)) is None:
            # Insert current feed in db
            self.insert_feed((feed.name, feed.title, feed.updated))
        else:
            self.logger.info(
                "The feed '{}' is already in the database.".format(feed.name))

    def process_entries(self, entries_list):
        # `entries_list` is a list of dict (of entries)
        for i, entry_dict in enumerate(entries_list, start=1):
            try:
                # Process the entry
                self.logger.info("Processing entry #{}".format(i))
                entry = self.process_entry(entry_dict)
                # Process tags associated with the entry
                self.process_tags(entry)
            except (KeyError, DuplicateEntryError) as e:
                self.logger.exception(e)
                self.logger.warning("The entry #{} will be skipped".format(i))
            except FeedNotFoundError as e:
                raise FeedNotFoundError(e)
            else:
                self.logger.debug("Entry #{} processed!".format(i))

    def process_entry(self, entry_dict):
        # Parse the given entry
        entry = Entry(self.feed_url, entry_dict, self.logging_cfg)
        # Check if the current entry is already in the db
        if self.select_entry((entry.id,)) is None:  # New entry
            # Sanity check if the `feed_name` (entries foreign key) is already
            # in the Feeds table
            # NOTE: the `feed_name` is a very important attribute for the
            # entries table because it is a foreign key that links both the
            # feeds and entries tables
            if self.select_feed((entry.feed_name,)):  # feed found
                self.logger.info("The entry '{}' will be inserted in the "
                                 "database.".format(entry.title))
                # Insert current entry in db
                self.insert_entry((entry.id,
                                   entry.feed_name,
                                   entry.title,
                                   entry.author,
                                   entry.url,
                                   entry.location,
                                   entry.summary,
                                   entry.published))
                return entry
            else:
                raise FeedNotFoundError("The feed '{}' is not found in the "
                                        "database.".format(entry.feed_name))
        else:
            raise DuplicateEntryError("The entry with id='{}' is already in the "
                                      "database.".format(entry.id))

    def process_tags(self, entry):
        for tag in entry.tags:
            # Check if tag is already in db
            if self.select_tag((entry.id, tag,)) is None:
                # Insert tag in db
                self.insert_tag((entry.id, tag,))
            else:
                self.logger.warning("The tag '{}' is already in the database. "
                                    "The tag will be skipped.".format(tag))
                continue

    def insert_entry(self, entry):
        # TODO: automate adding number of '?' in the SQL expression
        sql = '''INSERT INTO entries VALUES (?,?,?,?,?,?,?,?)'''
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
        sql = '''SELECT * FROM entries WHERE job_post_id=?'''
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
        sql = '''SELECT * FROM tags WHERE job_post_id=? AND name=?'''
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
        assert type(val) is tuple, \
            "The values for the SQL expression are not of `tuple` type"
        assert len(val) == sql.count('?'), \
            "Wrong number of values ({}) in the SQL expression '{}'".format(
                len(val), sql)

    def commit(self):
        """
        Wrapper to sqlite3.conn.commit()

        :return: None
        """
        if not self.autocommit:
            self.conn.commit()


if __name__ == '__main__':
    sb = ScriptBoilerplate(
        module_name=__name__,
        module_file=__file__,
        cwd=os.getcwd(),
        parser_desc="Run data analysis of Stackoverflow job posts.",
        parser_formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    sb.parse_args()
    logger = sb.get_logger()
    try:
        logger.info("Loading the config file '{}'".format(sb.args.main_cfg))
        main_cfg = read_yaml_config(sb.args.main_cfg)
    except OSError as e:
        logger.exception(e)
        logger.error("The config file '{}' couldn't be loaded".format(
            sb.args.main_cfg))
        logger.warning("The program will exit")
        sys.exit(1)
    else:
        logger.info("Config file loaded!")
    rss_feeds = main_cfg['rss_feeds']
    status_code = 0
    try:
        logger.info("Starting the RSS reader")
        rss_reader = RSSReader(
            db_filepath=os.path.expanduser(main_cfg['db_filepath']),
            logging_cfg=sb.logging_cfg_dict,
            logger=logger)
        for feed_url in rss_feeds:
            try:
                logger.info("Reading the feed '{}'".format(feed_url))
                rss_reader.read(feed_url)
            except FeedNotFoundError as e:
                logger.critical(e)
                logger.warning("The feed '{}' will be skipped".format(feed_url))
            else:
                logger.info("End of processing feed '{}'")
    except (AssertionError, KeyboardInterrupt) as e:
        logger.critical(e)
        status_code = 1
    else:
        logger.info("End of RSS reader")
    finally:
        logger.info("Program will exit")
        sys.exit(status_code)

    # TODO: check case where there is a parse exception
    # e.g. SAXParseException('not well-formed (invalid token)',)
    # feed and entries will both be empty in that case. Check by removing
    # the file extension of the feed file
    # TODO: another case you should check is when there is a
    # sqlite3.ProgrammingError
    # e.g. Incorrect number of bindings supplied. The current statement
    # uses 2, and there are 1 supplied.
    # Check by removing a field from a table
