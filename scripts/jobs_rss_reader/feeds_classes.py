from datetime import datetime
# Third-party code
from bs4 import BeautifulSoup
import pytz
import tzlocal


class Feed:
    def __init__(self, url, feed_dict):
        """
        Feed constructor

        :param url:
        :param feed_dict:
        """
        self.name = url  # Primary key
        self.title = None
        self.updated = None  # Date with format YYYY-MM-DD HH:MM:SS-HH:MM

        # Extract feed title
        if hasattr(feed_dict, "title"):
            self.title = feed_dict.title
        elif hasattr(feed_dict, "title_detail") and \
                hasattr(feed_dict.title_detail, "value"):  # Fallback
            self.title = feed_dict.title_detail.value
        else:
            print("[WARNING] no title could be extracted from the RSS feed {}".format(url))

        # Extract `updated_parsed` which is of `time.struct_time` type
        if hasattr(feed_dict, "updated_parsed"):
            # NOTE: the `updated_parsed` date is given in UTC
            # Convert UTC to local time
            self.updated = get_local_time(feed_dict.updated_parsed)
        else:
            print("[WARNING] no updated date could be extracted from the "
                  "RSS feed {}".format(url))
            self.updated = get_local_time()


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
        # There is also an `updated` field; check if it is different thant
        # `published` and if it is the case it might be useful

        # Extract relevant entry fields
        if hasattr(entry_dict, "id"):
            self.id = entry_dict.id
            self.title = extract_value(entry_dict, "title")
            self.author = extract_value(entry_dict, "author")
            self.link = extract_value(entry_dict, "link")
            raw_summary = extract_value(entry_dict, "summary")
            if raw_summary is not None:
                self.summary = parse_summary(raw_summary)
            # Extract `published_parsed` which is of `time.struct_time` type
            # NOTE: the `published_parsed` date is given in UTC
            published_parsed = extract_value(entry_dict, "published_parsed")
            if published_parsed is not None:
                # Convert UTC to local time
                self.published = get_local_time(published_parsed)
            # Extract tags
            tags = extract_value(entry_dict, "tags")
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


def extract_value(entry_dict, key):
    """

    :param entry_dict:
    :param key:
    :return:
    """
    ret_val = None
    if hasattr(entry_dict, key):
        ret_val = entry_dict[key]
    else:
        print("[WARNING] Entry {} doesn't have the key {}".format(entry_dict.id, key))
    return ret_val


# TODO: add in utility package
def get_local_time(utc_time=None):
    """
    If a UTC time is given, it is converted to the local time zone. If
    `utc_time` is None, then the local time zone is simply returned.
    The local time zone is returned as a string with format
    YYYY-MM-DD HH:MM:SS-HH:MM

    :param utc_time: object of type `time.struct_time`
    :return local_time: string representing the local time
    """
    # Get the local timezone name
    tz = pytz.timezone(tzlocal.get_localzone().zone)
    local_time = None

    if utc_time:
        # Convert time.struct_time into datetime
        utc_time = datetime(*utc_time[:6])
        # Convert naive object (time zone unaware) into aware object
        utc_time = utc_time.replace(tzinfo=pytz.UTC)
        # Convert the UTC time into the local time zone
        local_time = utc_time.astimezone(tz)
    else:
        # Get the time in the system's time zone
        local_time = datetime.now(tz)
        # Remove microseconds
        local_time = local_time.replace(microsecond=0)
    # Use date format: YYYY-MM-DD HH:MM:SS-HH:MM
    # ISO format is YYYY-MM-DDTHH:MM:SS-HH:MM
    local_time = local_time.isoformat().replace("T", " ")
    return local_time
