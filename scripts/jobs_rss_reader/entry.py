import os
# Third-party modules
from bs4 import BeautifulSoup
# Own modules
from utility.genutil import get_local_time
from utility.logging_boilerplate import LoggingBoilerplate


class Entry:
    def __init__(self, url, entry_dict, logging_cfg):
        sb = LoggingBoilerplate(__name__,
                                __file__,
                                os.getcwd(),
                                logging_cfg)
        self.logger = sb.get_logger()
        self.feed_name = url  # Foreign key
        self.id = None  # Primary key
        self.title = None
        self.author = None
        self.url = None
        self.location = None
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
            self.url = entry_dict.get('link')
            self.location = entry_dict.get('location')
            raw_summary = entry_dict.get('summary')
            if raw_summary is not None:
                self.summary = parse_summary(raw_summary)
            else:
                self.logger.warning("No summary found in the entry "
                                    "'{}'".format(self.id))
            # Extract `published_parsed` which is of `time.struct_time` type
            # NOTE: the `published_parsed` date is given in UTC
            published_parsed = entry_dict.get('published_parsed')
            if published_parsed is not None:
                # Convert UTC to local time
                self.published = get_local_time(published_parsed)
            else:
                self.logger.warning("No `published_parsed` in the entry "
                                    "'{}'".format(self.id))
            # Extract tags
            tags = entry_dict.get('tags')
            if tags is not None:
                for tag in entry_dict.tags:
                    self.tags.append(tag.term)
            else:
                self.logger.warning("No tags in the entry '{}'".format(self.id))
        else:
            raise KeyError(
                "Entry from '{}' doesn't have an id".format(self.feed_name))


def parse_summary(raw_summary):
    # NOTE: when you get text from HTML elements, call strip() to remove any
    # trailing whitespaces (e.g. new lines)
    return BeautifulSoup(raw_summary, "html.parser").get_text().strip()
