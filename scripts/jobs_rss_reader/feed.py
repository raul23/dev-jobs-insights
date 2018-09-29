import os
# Own modules
from utility.genutil import get_local_time
from utility.logging_boilerplate import LoggingBoilerplate


class Feed:
    def __init__(self, url, feed_dict, logging_cfg):
        """
        Feed constructor
        :param url:
        :param feed_dict:
        """
        sb = LoggingBoilerplate(__name__,
                                __file__,
                                os.getcwd(),
                                logging_cfg)
        self.logger = sb.get_logger()
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
            self.logger.warning("No title could be extracted from the RSS feed "
                                "'{}'".format(url))
        # Extract updated_parsed which is of time.struct_time type
        if feed_dict.get('updated_parsed'):
            # NOTE: the updated_parsed date is given in UTC
            # Convert UTC to local time
            self.updated = get_local_time(feed_dict.updated_parsed)
        else:
            self.logger.warning("No updated date could be extracted from the RSS "
                                "feed '{}'".format(url))
            self.updated = get_local_time()
