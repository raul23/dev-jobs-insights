import os
import sys

# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
from utility import genutil as g_util


class Feed:
    def __init__(self, url, feed_dict):
        """
        Feed constructor
        :param url:
        :param feed_dict:
        """
        self.name = url  # Primary key of the entries table
        self.title = None
        self.updated = None  # Date with format YYYY-MM-DD HH:MM:SS-HH:MM

        # Extract feed title
        if feed_dict.get('title'):
            self.title = feed_dict.title
        # Fallback: get the title from title_detail
        elif feed_dict.get("title_detail") and \
                feed_dict.title_detail.get('value'):
            self.title = feed_dict.title_detail.value
        else:
            print("[WARNING] No title could be extracted from the RSS feed {}".format(url))

        # Extract updated_parsed which is of time.struct_time type
        if feed_dict.get('updated_parsed'):
            # NOTE: the updated_parsed date is given in UTC
            # Convert UTC to local time
            self.updated = g_util.get_local_time(feed_dict.updated_parsed)
        else:
            print("[WARNING] no updated date could be extracted from the "
                  "RSS feed {}".format(url))
            self.updated = g_util.get_local_time()
