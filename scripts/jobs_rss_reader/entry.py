import os
import sys

from bs4 import BeautifulSoup

# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
from utility import genutil as g_util


class Entry:
    def __init__(self, url, entry_dict):
        self.feed_name = url  # Foreign key
        self.id = None  # Primary key
        self.title = None
        self.author = None
        self.link = None
        self.summary = None
        self.published = None  # Date with format YYYY-MM-DD HH:MM:SS-HH:MM
        self.tags = []
        # TODO: there is an `authors` field also; it is a list in case there are
        # more than one author. Check if it is the case in the entries
        # There is also an `author_detail` field but it seems that it contains
        # the same info as `author`.
        # There is also an `updated` field; check if it is different than
        # `published` and if it is the case it might be useful

        # Extract relevant entry fields
        if entry_dict.get('id'):
            self.id = entry_dict.id
            self.title = entry_dict.get('title')
            self.author = entry_dict.get('author')
            self.link = entry_dict.get('link')
            raw_summary = entry_dict('summary')
            if raw_summary is not None:
                self.summary = parse_summary(raw_summary)
            # Extract published_parsed which is of time.struct_time type
            # NOTE: the published_parsed date is given in UTC
            published_parsed = entry_dict('published_parsed')
            if published_parsed is not None:
                # Convert UTC to local time
                self.published = g_util.get_local_time(published_parsed)
            # Extract tags
            tags = entry_dict('tags')
            if tags is not None:
                for tag in entry_dict.tags:
                    self.tags.append(tag.term)
        else:
            print("[ERROR] Entry from {} doesn't have an id" % self.feed_name)


def parse_summary(raw_summary):
    doc = BeautifulSoup(raw_summary, "html.parser")
    # NOTE: when you get text from HTML elements, call strip() to remove any
    # trailing whitespaces (e.g. new lines)
    return doc.get_text().strip()
