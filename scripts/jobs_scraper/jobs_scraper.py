import argparse
from datetime import date, datetime
import json
import math
import os
import pathlib
import re
import sqlite3
import sys
import time
# Third-party modules
from bs4 import BeautifulSoup
from forex_python.converter import get_currency_name, get_rate, \
    RatesNotAvailableError
from pycountry_convert import country_name_to_country_alpha2
import requests
import ipdb
# Own modules
from job_data import DuplicateRecordError, JobData, NoOfficeLocationFoundError
import exc
from scraping_session import ScrapingSession
from utility.genutil import connect_db, dump_pickle, get_local_datetime, \
    load_json, read_file, read_yaml_config, write_file
from utility.logging_boilerplate import LoggingBoilerplate
from utility.script_boilerplate import ScriptBoilerplate


# ref.: https://stackoverflow.com/a/50120316
class recursionlimit:
    def __init__(self, limit):
        self.limit = limit
        self.old_limit = sys.getrecursionlimit()

    def __enter__(self):
        sys.setrecursionlimit(self.limit)

    def __exit__(self, type_, value, tb):
        sys.setrecursionlimit(self.old_limit)


class JobsScraper:
    def __init__(self, main_cfg, logging_cfg, logger):
        self.main_cfg = main_cfg
        self.logging_cfg = logging_cfg
        # Setup all loggers
        self.logger = logger
        # Setup `job_data`'s logger
        log_info = JobData.get_logging_info()
        sb = LoggingBoilerplate(log_info[0],
                                log_info[1],
                                log_info[2],
                                logging_cfg)
        self.job_data_logger = sb.get_logger()
        # =====================================================================
        # Sessions initialization
        # =====================================================================
        self.session = None
        self.all_sessions = []
        # self.json_job_data = {}
        # =====================================================================
        # Save all data paths from the main config
        # =====================================================================
        self.db_filepath = os.path.expanduser(main_cfg['db_filepath'])
        self.us_states_filepath = os.path.expanduser(
            main_cfg['data_paths']['us_states'])
        self.cached_webpages_dirpath = os.path.expanduser(
            main_cfg['data_paths']['cached_webpages'])
        self.currencies_filepath = os.path.expanduser(
            main_cfg['data_paths']['currencies'])
        self.scraped_job_data_dirpath = os.path.expanduser(
            main_cfg['saving_cfg']['scraped_job_data_dirpath'])
        # =====================================================================
        self.conn = None
        # Establish a session to be used for the GET requests
        self.req_session = requests.Session()
        self.headers = self.main_cfg['headers']
        self.last_request_time = -sys.float_info.max
        # =====================================================================
        # Load JSON data
        # =====================================================================
        # `currencies_data` is a list of dicts. Each item in `currencies_data`
        # is a dict with the keys ['cc', 'symbol', 'name'] where 'cc' is short
        # for currency code
        self.currencies_data = load_json(self.currencies_filepath)
        # Load the dict of US states where the keys are the USPS 2-letter codes
        # for the U.S. state and the values are the names
        # e.g. 'AZ': 'Arizona'
        self.us_states = load_json(self.us_states_filepath)
        # =====================================================================
        # Cache the rates that were already used for converting one currency to
        # another. Hence, we won't have to send HTTP requests to get these rates
        # if they are already cached
        # `cached_rates` has for keys the name of the rates and the values are
        # a tuple of the associated rates and the time (UTC) the rate was
        # retrieved. The name of the rate (key) is built like this:
        # {base_cur}_{dest_cur}
        # e.g. {'EUR_USD': (1.1391, 1535166701.7913752),
        #       'EUR_CAD': (1.4976,1535167046.6978931)}
        self.cached_rates = {}

    def start_scraping(self):
        # =====================================================================
        # Data retrieval from SQLite database
        # =====================================================================
        # Database connection
        try:
            self.logger.debug("Connecting to db '{}'".format(self.db_filepath))
            self.conn = connect_db(self.db_filepath)
        except sqlite3.Error as e:
            raise sqlite3.Error(e)
        else:
            self.logger.debug("Connection established!")
        # Get all the entries' URLs (+job_post_id, author)
        with self.conn:
            try:
                self.logger.debug(
                    "Retrieving entries from db '{}'".format( self.db_filepath))
                rows = self.select_entries()
            except sqlite3.OperationalError as e:
                raise sqlite3.OperationalError(e)
            else:
                if not rows:
                    raise exc.EmptyQueryResultSetError(
                        "The returned query result set is empty. Web scraping "
                        "will end!")
                else:
                    self.logger.debug("{} rows retrieved".format(len(rows)))
        # =====================================================================
        # Processing data from SQLite database
        # =====================================================================
        # For each entry's URL, scrape more job data from the job post's webpage
        n_skipped = 0
        skipped = False
        self.logger.info("Total URLs to process: {}".format(len(rows)))
        # TODO: add a progress bar
        for count, (job_post_id, title, author, url, location, published) in \
                enumerate(rows, start=1):
            # TODO: debug code
            if False and job_post_id != 203827:
                continue
            try:
                # TODO: add timing for each important processing parts
                # Initialize the current scraping session
                self.session = ScrapingSession(
                    job_post_id,
                    url=url,
                    data=JobData(job_post_id, self.job_data_logger))
                self.logger.info("#{} Processing '{}'".format(count, url))
                self.logger.info("Scraping session initialized for job_post_id "
                                 "{}".format(job_post_id))
                # Update the job post's URL
                self.session.data.set_job_post(url=url)
                # =============================================================
                # Update with RSS feed's data
                # =============================================================
                # Update the job data with the already extracted data from the
                # RSS feed
                self.logger.info("Processing the RSS feed data")
                self.process_rss_feed_data(author=author,
                                           title=title,
                                           published=published,
                                           location=location)
                # =============================================================
                # Load cached webpage
                # =============================================================
                # Load the cached webpage or retrieve it online
                self.logger.info("Loading cached webpage")
                html = self.load_cached_webpage()
                self.session.bs_obj = BeautifulSoup(html, 'lxml')
                # =============================================================
                # Job removal check
                # =============================================================
                # Before extracting any job data, check if the job post had
                # been removed. IMPORTANT: we check this situation if there is
                # no title in the job post.
                pattern = "header.job-details--header > div.grid--cell > " \
                          "h1.fs-headline1 > a"
                # TODO: make a separate method that processes the title tag to
                # determine if the job post was removed. At the same time set
                # the job post's title if it is to be found. The method could
                # return a boolean that tells if the job post was removed or not.
                # Hence, you can know if the job post should be skipped.
                try:
                    self.logger.debug("Checking if job post is removed")
                    _ = self.get_text_in_tag(pattern=pattern)
                except exc.EmptyTextError as e:
                    # TODO: create more specific error such as EmptyTitleError
                    # IMPORTANT: the title tag is found but the text is empty,
                    # a very unusual case! To be further investigated if it
                    # happens.
                    self.logger.exception(e)
                    self.logger.critical("The title tag should not contain an "
                                         "empty text. Unusual case!")
                    skipped = True
                    continue
                except exc.TagNotFoundError as e:
                    # TODO: create more specific error such as
                    # TitleTagNotFoundError
                    # No title tag found, thus it means that the job post was
                    # removed
                    self.logger.exception(e)
                    self.logger.error("No title found in the job post")
                    self.logger.warning("Job post removed!")
                    self.session.data.set_job_post(job_post_removed=True)
                    skipped = True
                    continue
                else:
                    self.logger.debug("Job post NOT removed!")
                # =============================================================
                # Process job notice
                # =============================================================
                # Check if the job is accepting applications by extracting the
                # message:
                # "This job is no longer accepting applications."
                # This notice is located in
                # body > div.container > div#content > aside.s-notice
                # NOTE: Usually when this notice is present in a job post, the
                # JSON linked data is not found anymore within the html of the job
                # post
                # TODO: once the specific errors are created, the errors catching
                # should be done within `process_notice()` and the errors should
                # be caught further below
                try:
                    self.logger.debug("Processing job notice")
                    self.process_notice()
                except exc.EmptyTextError as e:
                    # TODO: create more specific error such as EmptyNoticeError
                    self.logger.exception(e)
                    self.logger.critical("The notice tag should not contain an "
                                         "empty text. Unusual case!")
                    skipped = True
                    continue
                # =============================================================
                # Process JSON linked data
                # =============================================================
                # Process linked data from <script type="application/ld+json">
                self.logger.info("Processing JSON linked data")
                self.process_linked_data()
                # =============================================================
                # Process <header>
                # =============================================================
                # Process job data (e.g. salary, remote, location) from the <header>
                self.logger.info("Processing the <header>")
                self.process_header()
                # =============================================================
                # Process overview items
                # =============================================================
                # Process job data from the Overview section
                self.logger.info("Processing the overview items")
                self.process_overview_items()
                # TODO: process sections within .company-items.
                # These sections are "Life at [company_name]" and
                # "About [company_name]"
                # e.g. https://bit.ly/2xiFthz (example01.html)
                # TODO: add data from .developer-culture-items
                # TODO: follow the link pointed by the button
                # "Learn more about [company_name]" to extract more data about
                # the company. The link points to the company profile hosted
                # under stackoverflow.com
                # e.g. https://bit.ly/2xiFthz (example01.html)
                # The company profile is located @
                # https://stackoverflow.com/jobs/companies/[company_name]
                # e.g. https://stackoverflow.com/jobs/companies/discover
                # TODO: add similar jobs found within .more-jobs-items
                self.logger.info("Finished Processing {}".format(url))
                # TODO: debug code
                if False and count == 100:
                    break
            except exc.WebPageNotFoundError as e:
                self.logger.exception(e)
                self.logger.critical("The current URL '{}' will be "
                                     "skipped.".format(url))
                skipped = True
            except (AttributeError, KeyError) as e:
                # TODO: catch AttributeError every time we set a column to a record?
                # Or it should be caught in `job_data.py` as a decorator.
                # Maybe, it should be merge with the other exception getting
                # caught so the AttributeError is more specific to where it
                # comes from.
                self.logger.exception(e)
                skipped = True
            finally:
                # Save the current session
                # json_data = self.session.data.get_json_data()
                # self.json_job_data.setdefault(job_post_id, json_data)
                self.all_sessions.append((job_post_id, self.session))
                self.logger.info("Session ended")
                if skipped:
                    self.logger.warning(
                        "The current URL '{}' will be skipped".format(url))
                    n_skipped += 1
                skipped = False
        ipdb.set_trace()
        self.session = None
        # =====================================================================
        # Directory creation for saving scraped job data
        # =====================================================================
        # Folder name will begin with the date+time
        timestamped = datetime.now().strftime('%Y%m%d-%H%M%S-{fname}')
        # Create directory where the scraped job data will be saved
        scraped_job_data_dirpath = os.path.join(
            self.scraped_job_data_dirpath,
            timestamped.format(fname='scraped_job_data'))
        pathlib.Path(scraped_job_data_dirpath).mkdir(parents=True, exist_ok=True)
        self.logger.warning(
            "Directory for job data created: {}".format(scraped_job_data_dirpath))
        # IMPORTANT: saving JSON job data disabled
        """
        # Save scraped data into json file
        # ref.: https://stackoverflow.com/a/31343739 (presence of unicode
        # strings, e.g. EURO currency symbol)
        # TODO: code factorization, saving data (scraped and sessions data) in
        # similar ways
        self.logger.info("Saving JSON scraped job data ({} job posts)".format(
                          len(self.json_job_data)))
        try:
            filename = 'scraped_job_data.json'
            scraped_job_data_filepath = os.path.join(
                scraped_job_data_dirpath, filename)
            # IMPORTANT: JSON data saving disabled because datetime and date are
            # present in the table instances
            # TODO: re-enable saving of JSON data
            # replace the date and datetime objects from the table instances'
            # columns with their corresponding string representations
            # g_util.dump_json_with_codecs(scraped_job_data_filepath,
            #                              self.json_job_data)
        except (OSError, TypeError) as e:
            self.logger.error("JSON scraped job data couldn't be saved")
        else:
            self.logger.info("JSON scraped job data saved in {}".format(
                             scraped_job_data_filepath))
        """
        # =====================================================================
        # Session saving as pickle files
        # =====================================================================
        # see https://stackoverflow.com/a/2135179 for `sys.setrecursionlimit`
        split_size = self.main_cfg['saving_cfg']['split_size']
        with recursionlimit(15000):
            n_sessions = len(self.all_sessions)
            n_groups = math.ceil(n_sessions / split_size)
            start_index = 0
            end_index = min(len(self.all_sessions), start_index + split_size)
            for i in range(n_groups):
                sessions = dict(self.all_sessions[start_index:end_index])
                self.logger.info("Saving pickled sessions data, parts {}-{}, "
                                 "{} job posts".format(
                                  start_index, end_index-1, len(sessions)))
                try:
                    filename = 'all_sessions-{}-{}.pkl'.format(
                        start_index, end_index-1)
                    all_sessions_filepath = os.path.join(
                        scraped_job_data_dirpath, filename)
                    dump_pickle(all_sessions_filepath, sessions)
                except (OSError, TypeError) as e:
                    self.logger.exception(e)
                    self.logger.error("Pickled sessions data {}-{} couldn't be "
                                      "saved".format(start_index, end_index-1))
                    break
                else:
                    self.logger.info(
                        "Pickled sessions data, parts {}-{}, saved in {}".format
                        (start_index, end_index, all_sessions_filepath))
                    start_index = end_index
                    end_index = min(len(self.all_sessions),
                                    start_index + split_size)
        self.logger.info("Skipped URLs={}/{}".format(n_skipped, len(rows)))

    def process_rss_feed_data(self, author, title, location, published):
        # Company: author
        self.session.data.set_company(name=author)
        # Job post: title, published, location
        # Process title to remove the unnecessary info since this is what is being
        # done in the JSON linked data
        # e.g. 'iOS Engineer at Bending Spoons (Milano, Italy)' --> 'iOS Engineer'
        pos = title.rfind(" at ")
        title = title[0:pos]
        self.session.data.set_job_post(title=title,
                                       date_posted=self.str_to_date(published))
        if location is None:
            self.logger.warning("No office location found!")
            self.logger.info(
                "The location from the RSS feed ({}) is 'None'".format(
                    self.session.data.job_post_id))
            location = "No office location"
            self.logger.info(
                "'{}' will be used as the name of the location/country".format(
                 location))
            self.session.data.set_job_location(country=location)
        elif self.session.data.job_locations:
            # The job location was already extracted. No need to get it if it was
            # extracted from the JSON linked data for example. Hence, we can save
            # some computations
            self.logger.warning("The 'job location' was already previously "
                                "extracted")
        else:
            try:
                # Process the location
                # We want to standardize the country (e.g. Finland --> FI)
                location = self.process_location_text(location)
                self.session.data.set_job_location(**location)
            except (KeyError, exc.InvalidLocationTextError) as e:
                self.logger.exception(e)
            else:
                self.logger.debug("The location '{}' was saved.".format(location))

    # Returns: `retval`, dict
    #   {'company_min_size': int,
    #    'company_max_size': int}
    # Raises:
    #   - InvalidCompanySizeError by itself
    #   - NoCompanySizeError by itself
    def process_company_size(self, company_size):
        retval = {'company_min_size': None, 'company_max_size': None}
        # Example: '1k-5k people' --> '1000-5000'
        # Replace the letter 'k' with '000'
        company_size = company_size.replace('k', '000')
        # Extract only the numbers, i.e. 'people','+' should be ignored
        # e.g. 10k+ people --> 10000
        regex = r"\d+"
        matches = re.findall(regex, company_size)
        if matches:
            matches = [int(match) for match in matches]
            self.logger.debug("Found the min size '{}' in the company size "
                              "'{}'".format(min(matches), company_size))
            retval['company_min_size'] = min(matches)
            if len(matches) > 2:
                raise exc.InvalidCompanySizeError(
                    "Found more than two numbers '{}' in company size "
                    "'{}'".format(matches, company_size))
            elif len(matches) == 2:
                retval['company_max_size'] = max(matches)
                self.logger.debug("Found the max size '{}' in the company size "
                                  "'{}'".format(max(matches), company_size))
            else:
                pass
            return retval
        else:
            raise exc.NoCompanySizeError(
                "No size (min or max) could be retrieved from the company size "
                "{}".format(company_size))

    @staticmethod
    # Returns: str of the modified employment type
    # NOTE: 'Employment type' is also equivalent to 'job type'
    def process_employment_type(employment_type):
        # Standardize the employment type by modifying to all caps and
        # replacing hyphens with underscores
        # e.g. Full-time --> FULL_TIME
        if employment_type == 'Contract':
            employment_type = 'contractor'
        return employment_type.upper().replace('-', '_')

    def process_header(self):
        # Get more job data (e.g. salary, remote, location) from the <header>
        # The job data in the <header> are found in this order:
        # 1. Title of job post
        # 2. company name
        # 3. office location
        # 4. Other job data: Salary, Remote, Visa sponsor, Paid relocation, ...
        # NOTE: the company name and office location are found on the same line
        # separated by a vertical line. The other job data are to be all found on
        # the same line (after the company name and office location) and these
        # job data are all part of a class that starts with '-', e.g. '-salary',
        # '-remote' or '-visa'
        bs_obj = self.session.bs_obj
        url = self.session.url
        # =====================================================================
        # 1. Get title of job post
        # =====================================================================
        # First check if the title was not already extracted before
        title = self.session.data.job_post.title
        if title:
            # Title already extracted
            self.logger.warning("The job post's title already has a "
                                "value='{}'".format(title))
            self.logger.warning("The currently extracted job post's title '{}' is "
                                "ignored".format(title))
        else:
            # Extract job post's title
            try:
                self.logger.debug("Extracting the job post's title")
                pattern = "header.job-details--header > div.grid--cell > " \
                          "h1.fs-headline1 > a"
                title = self.get_text_in_tag(pattern)
                self.session.data.set_job_post(title=title)
                # TODO: once the specific errors are created, some
            except exc.TagNotFoundError as e:
                # TODO: create more specific error such as TitleTagNotFoundError
                # No title tag found, thus it means that the job post was removed
                # IMPORTANT: this case should not happen because it should have
                # already been detected previously when checking if the job post
                # was removed.
                self.logger.exception(e)
                self.logger.critical(
                    "No title found in the job post. This case should have "
                    "already been previously detected.")
            except exc.EmptyTextError as e:
                # TODO: create more specific error such as EmptyTitleError
                # IMPORTANT: the title tag is found but the text is empty, a very
                # unusual case! To be further investigated if it happens.
                # IMPORTANT: this case should not happen because it should have
                # already been detected previously when checking if the job post
                # was removed.
                self.logger.exception(e)
                self.logger.critical(
                    "The title tag should not contain an empty text. Unusual "
                    "case!")
            else:
                self.logger.debug("Job post's title '{}' saved!".format(title))
        # =====================================================================
        # 2. Get company name
        # =====================================================================
        # First check if the company name was not already extracted before
        company_name = self.session.data.company.name
        if company_name:
            # Company name already extracted
            self.logger.warning(
                "The company name already has a value='{}'".format(company_name))
            self.logger.warning(
                "The currently extracted company name '{}' is ignored".format(
                 company_name))
        else:
            # Extract company name
            try:
                self.logger.debug("Extracting the company name")
                pattern = "header.job-details--header > div.grid--cell > " \
                          "div.fc-black-700 > a"
                company_name = self.get_text_in_tag(pattern)
                self.session.data.set_company(name=company_name)
            except exc.TagNotFoundError as e:
                # TODO: create more specific error such as
                # CompanyNameTagNotFoundError
                # No tag containing the company name was found
                # IMPORTANT: this case should not happen because it should have
                # already been detected previously when checking if the job post
                # was removed.
                self.logger.exception(e)
                self.logger.critical(
                    "No company name found in the job post. This case should have "
                    "already been previously detected.")
            except exc.EmptyTextError as e:
                # TODO: create more specific error such as EmptyCompanyNameError
                # IMPORTANT: the title tag is found but the text is empty, a very
                # unusual case! To be further investigated if it happens.
                # IMPORTANT: this case should not happen because it should have
                # already been detected previously.
                self.logger.exception(e)
                self.logger.critical(
                    "The tag for the company name should not contain an empty "
                    "text. Unusual case!")
            else:
                self.logger.debug("Company name '{}' saved!".format(company_name))
        # =====================================================================
        # 3. Get office location
        # =====================================================================
        # Get the office location which is located on the same line as the
        # company name
        """
        if self.session.data.job_locations:
            # The job location was already extracted. No need to get it if it was
            # extracted from the JSON linked data for example. Hence, we can save
            # some computations
            job_locations = [(l.city, l.region, l.country) for l in
                             self.session.data.job_locations]
            self.logger.warning(
                "The 'job location' was already previously extracted: "
                "{}".format(job_locations))
        """
        try:
            pattern = "header.job-details--header > div.grid--cell > " \
                      "div.fc-black-700 > span.fc-black-500"
            text = self.get_text_in_tag(pattern)
            # Process the location text
            # We want to standardize the country (e.g. Finland --> FI)
            location = self.process_location_text(text)
            self.session.data.set_job_location(**location)
            self.logger.debug("The location {} was saved.".format(location))
        except exc.TagNotFoundError as e:
            # from `get_text_in_tag()`
            self.logger.debug(e)
        except exc.EmptyTextError as e:
            # from `get_text_in_tag()`
            self.logger.exception(e)
        except (KeyError, exc.InvalidLocationTextError) as e:
            # from `process_location_text()`
            self.logger.exception(e)
        except DuplicateRecordError as e:
            # from `set_job_location()`
            self.logger.warning(e)
        except NoOfficeLocationFoundError as e:
            # from `set_job_location()`
            self.logger.critical(e)
        # =====================================================================
        # 4. Get the other job data
        # =====================================================================
        # Get the other job data on the next line after the company name
        # and location
        # Examples of other job data: Salary, Remote, Visa sponsor,
        # Paid relocation
        pattern = "header.job-details--header > div.grid--cell > div.mt12"
        div_tag = bs_obj.select_one(pattern)
        if div_tag:  # div.mt12
            # Each <div> child is associated to a job item (e.g. salary,
            # remote) and is a <span> tag with a class that starts with '-'
            # Example:
            # header.job-details--header > div.grid--cell > div.mt12 > span.-salary.pr16
            children = div_tag.findChildren()
            for child in children:
                # Each job data text is found within <span> with a class that
                # starts with '-', e.g. <span class='-salary pr16'>
                # NOTE: we need the child element's class that starts with '-'
                # because we will then know how to name the extracted job
                # data item
                child_class = [tag_class for tag_class in child.attrs['class']
                               if tag_class.startswith('-')]
                if child_class:  # <span> class that starts with '-'
                    # Get the <span> class name without the '-' at the
                    # beginning, this will correspond to the type of job data
                    # (e.g. salary, remote, relocation, visa)
                    key_name = child_class[0][1:]
                    text = child.text
                    if text:  # value = text
                        self.logger.info(
                            "The item '{}' is found".format(key_name))
                        # Removing any whitespace from the `text`
                        text = text.strip()
                        # Apply specific processing if the extracted text refers
                        # to a salary
                        if key_name == 'salary':
                            # Case: `text` is salary related
                            if self.session.data.job_salaries:
                                # Case: job salaries already extracted
                                # No need to get salaries if they were extracted
                                # in the JSON linked data. Hence, we can save
                                # some computations
                                self.logger.warning(
                                    "The job salaries for '{}' were already "
                                    "extracted previously".format(company_name))
                                continue
                            else:
                                # Case: No job salaries extracted yet
                                try:
                                    # Extract min-max salaries from `text`
                                    # NOTE: equity can be also extracted
                                    job_salaries \
                                        = self.process_salary_text(text)
                                except (exc.CurrencyRateError,
                                        exc.NoCurrencyCodeError,
                                        exc.NoCurrencySymbolError,
                                        exc.InvalidCountryError) as e:
                                    self.logger.exception(e)
                                else:
                                    # Save all the salary-related info in the
                                    # `job_salaries` table
                                    for job_salary in job_salaries:
                                        self.logger.debug(
                                            "Updating dict with {}".format(
                                             job_salary))
                                        if job_salary.get('equity'):
                                            self.session.data.set_job_post(
                                                **job_salary)
                                        else:
                                            self.session.data.set_job_salary(
                                                **job_salary)
                        else:
                            # Case: other job data that is not salary. They are
                            # remote, relocation,and visa
                            self.logger.debug(
                                "Updating dict with {{{}:{}}})".format(
                                 key_name, text))
                            # Save the other job data in the `job_posts` table
                            self.session.data.set_job_post(**{key_name: text})
                    else:
                        self.logger.error(
                            "No text found for the job data with key "
                            "'{}'.".format(key_name))
                else:
                    self.logger.error(
                        "The <span>'s class doesn't start with '-'.")
        else:
            self.logger.info(
                "Couldn't extract the other job data @ '{}'. The other job data "
                "should be found in '{}'".format(url, pattern))

    def process_linked_data(self):
        # Get linked data from <script type="application/ld+json">:
        # On the webpage of a job post, important data about the job post
        # (e.g. job location or salary) can be found in
        # <script type="application/ld+json">
        # This linked data is a JSON object that stores important job info like
        # employmentType, experienceRequirements, jobLocation
        script_tag \
            = self.session.bs_obj.find(attrs={'type': 'application/ld+json'})
        url = self.session.url
        if script_tag:
            """
            The linked data found in <script type="application/ld+json"> is a 
            json object with the following keys:
            '@context', '@type', 'title', 'skills', 'description', 'datePosted',
            'validThrough', 'employmentType', 'experienceRequirements',
            'industry', 'jobBenefits', 'hiringOrganization', 'baseSalary', 
            'jobLocation'
            """
            linked_data = json.loads(script_tag.get_text())
            # Extract data for populating the `job_posts` table
            date_posted = self.str_to_date(linked_data.get('datePosted'))
            valid_through = self.str_to_date(linked_data.get('validThrough'))
            self.session.data.set_job_post(
                title=linked_data.get('title'),
                employment_type=linked_data.get('employmentType'),
                job_post_description=linked_data.get('description'),
                date_posted=date_posted,
                valid_through=valid_through)
            # Extract data for populating the `job_salaries` table
            min_salary \
                = linked_data.get('baseSalary', {}).get('value', {}).get('minValue')
            max_salary \
                = linked_data.get('baseSalary', {}).get('value', {}).get('maxValue')
            currency \
                = linked_data.get('baseSalary', {}).get('currency')
            self.session.data.set_job_salary(min_salary=min_salary,
                                             max_salary=max_salary,
                                             currency=currency)
            # Extract data for populating the `companies` table
            self.session.data.set_company(
                name=linked_data.get('hiringOrganization', {}).get('name'),
                url=linked_data.get('hiringOrganization', {}).get('sameAs'),
                description=linked_data.get(
                    'hiringOrganization', {}).get('description'))

            def process_values(values, set_method_name):
                try:
                    # Sanity check on `values`
                    if values is None:
                        self.logger.debug(
                            "`values` is  'None'; `set_method_name`='{}'".format(
                             set_method_name))
                    elif not values:
                        # `values` is an empty list
                        self.logger.debug(
                            "`values` is an empty list; "
                            "`set_method_name`='{}'".format(set_method_name))
                    else:
                        if isinstance(values, str):
                            # A long comma-separated string
                            values = self.str_to_list(values)
                        elif isinstance(values, list) and \
                                isinstance(values[0], str):
                            # A list of strings
                            values = self.strip_list(values)
                        else:
                            # e.g. list of dicts
                            pass
                        set_method \
                            = self.session.data.__getattribute__(set_method_name)
                        for value in values:
                            try:
                                if isinstance(value, dict):
                                    set_method(**value)
                                else:
                                    set_method(name=value)
                            except DuplicateRecordError as e:
                                self.logger.warning(e)
                            except NoOfficeLocationFoundError as e:
                                self.logger.debug(e)
                                location = [v for k, v in value.items()]
                                self.logger.debug("The location '{}' is skipped".format(location))
                except AttributeError as e:
                    self.logger.exception(e)

            """
            if self.session.data.job_locations:
                # The job location was already extracted. No need to get it if it
                # was extracted from the JSON linked data for example. Hence, we
                # can save some computations
                job_locations = [(l.city, l.region, l.country) for l in
                                 self.session.data.job_locations]
                self.logger.warning(
                    "The 'job location' was already previously extracted: "
                    "{}".format(job_locations))
            """
            # Extract data for populating the `job_locations` table
            process_values(values=self.get_loc_in_ld(linked_data),
                           set_method_name='set_job_location')
            # Extract data for populating the `experience_levels` table
            process_values(values=linked_data.get('experienceRequirements'),
                           set_method_name='set_experience_level')
            # Extract data for populating the `industries` table
            process_values(values=linked_data.get('industry'),
                           set_method_name='set_industry')
            # Extract data for populating the `skills` table
            process_values(values=linked_data.get('skills'),
                           set_method_name='set_skill')
            # Extract data for populating the `job_benefits` table
            process_values(values=linked_data.get('jobBenefits'),
                           set_method_name='set_job_benefit')
            # Convert the minimum and maximum salaries to
            # `dest_currency` (e.g. USD)
            try:
                results = self.convert_min_and_max_salaries(min_salary,
                                                            max_salary,
                                                            currency)
            except exc.CurrencyRateError as e:
                self.logger.exception(e)
            except (exc.NoneBaseCurrencyError, exc.NoneSalaryError,
                    exc.SameCurrencyError) as e:
                self.logger.debug(e)
            else:
                self.session.data.set_job_salary(**results)
                self.logger.debug("Salaries saved: {}".format(results))
            finally:
                self.logger.debug("Finished processing the linked data from "
                                  "'{}'".format(url))
        else:
            # Reasons for not finding <script type='application/ld+json'>:
            # maybe the page is not found anymore (e.g. job post removed) or
            # the company is not longer accepting applications
            self.logger.info("The page @ '{}' doesn't contain any "
                             "`<script type='application/ld+json'>`".format(url))

    def process_location_text(self, text):
        updated_values = {}
        # The text where you find the location looks like this:
        # '\n|\r\nNo office location                    '
        # strip() removes the first newline and the right whitespaces.
        # Then split('\n')[-1] extracts the location string. And the replace()
        # will remove any spaces after the commas.
        # e.g. 'Toronto, ON, Canada' --> 'Toronto,ON,Canada'
        text = text.strip().split('|')[-1].strip().replace(', ', ',')
        # Based on the number of commas, we can know if the text:
        # - refers only to a country --> No comma, e.g. Canada
        # - refers to a city and Country --> One commas, e.g. 'Bellevue, WA'
        # - refers a city, region (state, province), country --> Two commas,
        #       e.g. Toronto, ON, Canada
        # - can't be decoded --> Zero and 3+ commas
        if text.count(',') == 0:
            self.logger.warning("No commas found in location text '{}'. We will "
                                "assume that the location text '{}' refers to a "
                                "country.".format(text, text))
            # Save country, no more information can be extracted
            updated_values['country'] = text
        elif text.count(',') == 1:
            # One comma in location text
            # Example 1: 'Bellevue, WA'
            # Example 2: 'Helsinki, Finland'
            self.logger.debug(
                "Found 1 comma in the location text '{}'".format(text))
            # Save city and country
            updated_values = dict(zip(['city', 'country'], text.split(',')))
            # Do further processing on the country since it might refer in fact
            # to a U.S. state, e.g. 'Bellevue, WA'. For U.S. jobs, the job
            # posts don't specify the country as it is the case for job posts
            # for other countries.
            if self.is_a_us_state(updated_values['country']):
                self.logger.debug("The location text '{}' refers to a place in the "
                                  "US".format(text))
                # Fix the location information: the country refers actually to
                # a U.S. state, and save 'US' as the country
                updated_values['region'] = updated_values['country']
                updated_values['country'] = 'US'
                # NOTE: No need to standardize the country name (like we do in
                # the other cases) because it is already standard
                return updated_values
        elif text.count(',') == 2:
            # Two commas in location text
            # e.g. Toronto, ON, Canada
            self.logger.debug(
                "Found 2 commas in the location text '{}'".format(text))
            updated_values = dict(zip(['city', 'region', 'country'],
                                      text.split(',')))
        else:
            # Incorrect number of commas in location text. Thus we can't extract
            # the location from the text. I haven't encounter this case yet, but
            # we never know.
            raise exc.InvalidLocationTextError(
                "Invalid location text '{}'. Incorrect number of "
                "commas.".format(text))
        # Standardize the country, e.g. Finland -> FI
        try:
            std_country = self.standardize_country(updated_values['country'])
        except KeyError as e:
            raise KeyError(e)
        else:
            updated_values['country'] = std_country
            return updated_values

    def process_notice(self):
        pattern = "body > div.container > div#content > aside.s-notice"
        try:
            text = self.get_text_in_tag(pattern=pattern)
            self.session.data.set_job_post(job_post_terminated=True)
        except exc.EmptyTextError as e:
            raise exc.EmptyTextError(e)
        except exc.TagNotFoundError as e:
            # TODO: create more specific error such as
            # NoticeTagNotFoundError
            self.logger.debug("Job notice not found")
            self.logger.debug("Job still accepting job applications!")
        else:
            self.logger.debug(
                "Job post not accepting applications anymore")
            self.logger.debug("Job notice found @ '{}': '{}'".format(
                self.session.url, text))

    def process_overview_items(self):
        # Get job data from the Overview section. There are three places within
        # the Overview section that will be extracted for more job data:
        # 1. in the "High response rate" sub-section of Overview
        # 2. in the "About this job" sub-section of Overview
        # 3. in the "Technologies" sub-section of Overview
        # NOTE: these sub-sections are located within <div id=""overview-items>
        # TODO: add also the job description (it is also available from the linked
        # data), e.g. https://bit.ly/2xiFthz (example01.html)
        bs_obj = self.session.bs_obj
        url = self.session.url
        # =====================================================================
        # 1. High response rate section
        # =====================================================================
        # The high response rate might not be present (it isn't often we get
        # to see this notice on job posts)
        # Prefix to add at the beginning of the log messages
        # TODO: add the prefix to the log messages when parsing the high
        # response rate section
        # TODO: is the high response rate section part of the overview items?
        # If it isn't, then it should not be processed here with the overview items
        try:
            self.get_text_in_tag(".-high-response > .-text > .-title")
            self.session.data.set_company(high_response_rate=True)
        except exc.TagNotFoundError as e:
            # Raised by `get_text_in_tag()`
            # TODO: create more specific error such as
            # HighResponseRateTagNotFoundError
            self.logger.debug("The tag for `high_response_rate` wasn't found")
        except exc.EmptyTextError as e:
            # TODO: create more specific error such as EmptyHighResponseRateError
            # IMPORTANT: the tag for `high_response_rate` is found but the text
            # is empty, a very unusual case! To be further investigated if it
            # happens.
            self.logger.exception(e)
            self.logger.critical("The tag for `high_response_rate` should not "
                                 "contain an empty text. Unusual case!")
        else:
            self.logger.debug("The `high_response_rate` was set to True")

        # =====================================================================
        # 2. About this job section
        # =====================================================================
        # Get more job data (e.g. role, industry, company size) in the
        # "About this job" section. Each item is located in
        # "#overview-items > .mb32 > .job-details--about > .grid--cell6 > .mb8"
        # NOTE: these job data are presented in two columns, with three items
        # per column
        pattern = "#overview-items > .mb32 > .job-details--about > " \
                  ".grid--cell6 > .mb8"
        div_tags = bs_obj.select(pattern)
        # Standardize the key names used in the "About this" section
        # e.g. 'Job type' should be replaced with 'Employment type'
        convert_keys = {'Job type': 'Employment type'}
        # Prefix to add at the beginning of the log messages
        pre = "[About this]"
        if div_tags:
            # Each `div_tag` corresponds to a job data item
            # Example:
            # 'Job type: Full-time'
            # 'Company type: Private'
            # Extract the text from each `div_tag`
            for div_tag in div_tags:
                # Sample raw text: '\nJob type: \Full-time\n'
                # Remove the whitespace chars, and split by ':' to get the
                # key name (e.g. Job type) and its value (e.g. Full-time)
                # `temp` is a list where temp[0] is the key name (e.g. Job Type)
                # and temp[1] is its associated value (e.g. Full-time)
                temp = div_tag.text.strip().split(":")
                # Remove any whitespace chars from the key name and its value
                key_name, value = temp[0].strip(), temp[1].strip()
                # Convert the key name to use the standard key name
                key_name = convert_keys.get(key_name, key_name)
                # The key names should all be lowercase and spaces be replaced
                # with underscores e.g. Employment type ---> employment_type
                key_name = key_name.replace(" ", "_").lower()
                # TODO: do we avoid extracting experience levels and
                # industries if they were already extracted from the JSON
                # linked data?
                if (key_name == 'experience_level' and
                    self.session.data.experience_levels) or \
                        (key_name == 'industry' and self.session.data.industries):
                    # Case: experience levels and industries already
                    # extracted. No need to get them if they were
                    # extracted from the JSON linked data. Hence, we
                    # can save some computations
                    self.logger.warning("{} The '{}' was already extracted "
                                        "previously".format(pre, key_name))
                    continue
                # Perform SPECIFIC processing on the values depending on the
                # type of job data
                if key_name in ['experience_level', 'role', 'industry']:
                    # Comma-separated values should be converted to a list
                    # The key names with comma-separated values are:
                    # experience_level, role, industry
                    # e.g. Mid-Level, Senior, Lead  --> [Mid-Level, Senior, Lead]
                    self.logger.debug("{} The value '{}' will be converted to a "
                                      "list --> [{}]".format(pre, value, value))
                    list_values = self.str_to_list(value)
                    for value in list_values:
                        # Save the key ('name') and its associated value
                        kwarg = {'name': value}
                        self.logger.info("{} The item {{'name' : {}}} will be "
                                         "saved".format(pre, value))
                        try:
                            if key_name == 'experience_level':
                                self.session.data.set_experience_level(**kwarg)
                            elif key_name == 'role':
                                self.session.data.set_role(**kwarg)
                            else:
                                # Industry
                                self.session.data.set_industry(**kwarg)
                        except DuplicateRecordError as e:
                            self.logger.warning(e)
                elif key_name == 'company_size':
                    # Replace 'k' with '000' in the company size text
                    # '1k-5k people' --> '1000-5000'
                    # NOTE: it can happen that there is only a number in the
                    # company size, e.g. 10k+ people
                    try:
                        sizes = self.process_company_size(value)
                    except exc.InvalidCompanySizeError as e:
                        self.logger.exception(e)
                    except exc.NoCompanySizeError as e:
                        self.logger.exception(e)
                    else:
                        self.logger.debug("{} The company size '{}' was processed "
                                          "to '{}'".format(pre, value, sizes))
                        # Save the key and its associated value
                        self.session.data.set_company(**sizes)
                elif key_name == 'employment_type':
                    new_value = self.process_employment_type(value)
                    self.logger.debug("{} The employment type '{}' was processed "
                                      "to '{}'".format(pre, value, new_value))
                    value = new_value
                    # Save the key and its associated value
                    kwarg = {key_name: value}
                    self.logger.info("{} The item {{{} : {}}} will be "
                                     "saved".format(pre, key_name, value))
                    self.session.data.set_job_post(**kwarg)
                elif key_name == 'company_type':
                    # No further processing done on the company type
                    # Save the key and its associated value
                    kwarg = {key_name: value}
                    self.logger.info("{} The item {{{} : {}}} will be "
                                     "saved".format(pre, key_name, value))
                    self.session.data.set_company(**kwarg)
                else:
                    self.logger.error("{} Invalid job data: {{{} : {}}}".format(
                                        pre, key_name, value))
        else:
            self.logger.warning("{} Couldn't extract job data from the 'About "
                                "this job' section @ '{}'. The job data should "
                                "be found in {}".format(pre, url, pattern))

        # =====================================================================
        # 3. Technologies section
        # =====================================================================
        # Get the list of technologies, e.g. ruby, python, html5
        # NOTE: unlike the other job data in "overview_items", the technologies
        # are returned as a list
        # Prefix to add at the beginning of the log messages
        pre = "[Technologies]"
        # TODO: do we avoid extracting the skills if they were already extracted
        # from the JSON linked data?
        if self.session.data.skills:
            # Case: the skills were already extracted. No need to get them if
            # they were extracted from the JSON linked data. Hence, we can save
            # some computations
            self.logger.warning("{} The skills were already extracted "
                                "previously".format(pre))
        else:
            pattern = "#overview-items > .mb32 > div > a.job-link"
            link_tags = bs_obj.select(pattern)
            skills = []
            if link_tags:
                for link_tag in link_tags:
                    skill = link_tag.text
                    if skill:
                        self.logger.debug(
                            "{} Skill '{}' extracted".format(pre, skill))
                        skills.append(skill)
                    else:
                        self.logger.warning("{} No text found for the technology "
                                            "with href={}. URL @ '{}'".format(
                                                pre, link_tag["href"], url))
                if skills:
                    self.logger.info("{} These skills {} were successfully extracted "
                                     "from the Technologies section".format(
                                        pre, skills))
                    for skill in skills:
                        # Save the skill
                        kwarg = {'name': skill}
                        self.logger.info("{} The skill '{}' will be saved".format(
                                           pre, skill))
                        try:
                            self.session.data.set_skill(**kwarg)
                        except DuplicateRecordError as e:
                            self.logger.warning(e)
                else:
                    self.logger.warning("{} No skills extracted from the "
                                        "Technologies section")
            else:
                self.logger.info("{} Couldn't extract technologies from the "
                                 "Technologies section @ '{}'. The technologies "
                                 "should be found in '{}'".format(
                                    pre, url, pattern))

    # Returns:
    #   `updated_values`: list of min-max salaries where each item is a `dict`
    #       with keys 'currency', 'min_salary', 'max_salary', 'conversion_time'
    #       where 'conversion_time' is only associated with converted currencies
    # Raises:
    #   - InvalidCountryError from get_currency_code()
    #   - NoCurrencyCodeError from get_currency_code()
    #   - NoCurrencySymbolError from get_currency_symbol()
    #   - CurrencyRateError from convert_min_and_max_salaries()
    # e.g. `salary_range` = '€50k - 65k'
    def process_salary_range(self, salary_range):
        updated_values = []
        # Extract the currency symbol at the beginning of the salary range text
        # e.g. '€' will be extracted from €42k - 75k'
        # `results` is either:
        #       - a tuple (`currency_symbol`, `end`) or
        #       - an exception (if no currency symbol could be extracted)
        #
        # NOTE: `end` refers to the index of the first digit in the
        # `salary_range` text, e.g. end=1 if salary_range='€42k - 75k'
        try:
            results = self.get_currency_symbol(salary_range)
            # `results` is a tuple
            currency_symbol, end = results
            # Get the currency code (e.g. EUR) based on the currency symbol
            # (e.g. €)
            currency_code = self.get_currency_code(currency_symbol)
            # Remove the currency from the salary range,
            # e.g. '€42k - 75k' --> '42k - 75k'
            salary_range = salary_range[end:]
            # Replace the letter 'k' with '000', e.g. 42k --> 42000
            salary_range = salary_range.replace('k', '000')
            # Get the minimum and maximum salaries separately
            # e.g. '42000 - 75000' --> min_salary=42000, max_salary=75000
            min_salary, max_salary = self.get_min_max_salary(salary_range)
            # Save the salary-related info
            updated_values.append({'currency': currency_code,
                                   'min_salary': min_salary,
                                   'max_salary': max_salary
                                   })
            # Convert the min and max salaries to `dest_currency` (e.g. USD)
            # `results` is a `dict` of keys 'currency', 'min_salary',
            # 'max_salary', 'conversion_time'
            results = self.convert_min_and_max_salaries(min_salary,
                                                        max_salary,
                                                        currency_code)
        except exc.InvalidCountryError as e:
            # raised by `get_currency_code()`
            raise exc.InvalidCountryError(e)
        except exc.NoCurrencySymbolError as e:
            # raised by `get_currency_symbol()`
            raise exc.NoCurrencySymbolError(e)
        except exc.NoCurrencyCodeError as e:
            # raised by `get_currency_code()`
            raise exc.NoCurrencyCodeError(e)
        except exc.CurrencyRateError as e:
            # raised by `convert_min_and_max_salaries()`
            raise exc.CurrencyRateError(e)
        except (exc.NoneBaseCurrencyError, exc.NoneSalaryError,
                exc.SameCurrencyError) as e:
            # raised by `convert_min_and_max_salaries()`
            self.logger.debug(e)
        else:
            # Case: min and max salaries were successfully converted
            # Save the converted salaries
            updated_values.append(results)
        finally:
            # Cases: min and max salaries successfully converted and
            # SameCurrencyError
            # Return the saved salaries
            return updated_values

    # Returns: list of `dict`s
    #   where each `dict` can be:
    #       - {'equity': bool}
    #       - {'min_salary': int,
    #          'max_salary': int,
    #          'currency': str
    #         }
    #       - {'min_salary': int,
    #          'max_salary': int, '
    #          'currency': str,
    #          'conversion_time': float
    #         }
    # Raises:
    #   - NoCurrencyCodeError by process_salary_range()
    #   - NoCurrencySymbolError by process_salary_range()
    #   - InvalidCountryError by process_salary_range()
    #   - CurrencyRateError by process_salary_range()
    def process_salary_text(self, salary_text):
        updated_values = []
        # Check if the salary text contains 'Equity', e.g. '€42k - 75k | Equity'
        if 'Equity' in salary_text:
            self.logger.debug(
                "Equity found in the salary text {}".format(salary_text))
            # Split the salary text to get the salary range and equity
            # e.g. '€42k - 75k | Equity' will be splitted as '€42k - 75k' and
            # 'Equity'
            # IMPORTANT: the salary text can consist of 'Equity' only. In that
            # case `salary_range` must be set to None to avoid processing the
            # salary text any further.
            if '|' in salary_text:
                # Case: salary range and equity, e.g. '€42k - 75k | Equity'
                # _ refers to equity
                salary_range, _ = [v.strip() for v in salary_text.split('|')]
            else:
                # Case: only equity
                self.logger.debug(
                    "No salary found, only equity in '{}'".format(salary_text))
                salary_range = None
            # Save equity but not salary range since salary range must be
            # further processed to extract the min and max salaries which are
            # the useful information we want to save
            updated_values.append({'equity': True})
        else:
            # Case: only salary range and no equity in the salary text
            self.logger.debug(
                "Equity is not found in the salary text '{}'".format(salary_text))
            # Save the salary range
            salary_range = salary_text
        # Process the salary range to extract the min and max salaries
        if salary_range:
            try:
                # `results` is a list of min-max salaries where each item
                # is a `dict` with the keys 'currency', 'min_salary', and
                # 'max_salary'
                results = self.process_salary_range(salary_range)
            except exc.NoCurrencyCodeError as e:
                # raised by `get_currency_code()`
                raise exc.NoCurrencyCodeError(e)
            except exc.NoCurrencySymbolError as e:
                # Raised by `process_salary_range()`
                raise exc.NoCurrencySymbolError(e)
            except exc.InvalidCountryError as e:
                # Raised by `process_salary_range()`
                raise exc.InvalidCountryError(e)
            except exc.CurrencyRateError as e:
                # Raised by `process_salary_range()`
                raise exc.CurrencyRateError(e)
            else:
                self.logger.debug("The salary text {} was successfully "
                                  "processed!")
                updated_values.extend(results)
        return updated_values

    # Returns: str of text in given HTML tag
    # Raises:
    #   - EmptyTextError by itself
    #   - TagNotFoundError by itself
    def get_text_in_tag(self, pattern):
        url = self.session.url
        tag = self.session.bs_obj.select_one(pattern)
        if tag:
            # Tag found
            # Extract text within tag
            text = tag.text
            if text:
                # Text found
                # Remove any whitespaces in the text
                text = text.strip()
                self.logger.debug("The text '{}' is found.".format(text))
                return text
            else:
                # Empty text
                raise exc.EmptyTextError("The text is empty")
        else:
            # Tag not found
            raise exc.TagNotFoundError(
                "Couldn't find the tag {}. URL @ '{}'".format(pattern, url))

    # Returns a `dict`:
    #   {'min_salary': int,
    #    'max_salary': int,
    #    'currency': str,
    #    'conversion_time': datetime
    #   }
    #
    # Raises:
    #   - CurrencyRateError raised by convert_currency()
    #   - NoneBaseCurrencyError raised by itself
    #   - NoneSalaryError raised by itself
    #   - SameCurrencyError by itself
    # Convert the min and max salaries to a base currency (e.g. USD)
    def convert_min_and_max_salaries(self, min_salary, max_salary, base_currency):
        # Sanity check on the inputs
        if min_salary is None or max_salary is None:
            raise exc.NoneSalaryError("The salary is 'None'")
        if base_currency is None:
            raise exc.NoneBaseCurrencyError("The base currency is 'None'")
        # Check that the base currency is different from the destination
        # currency, e.g. USD-->USD
        if base_currency != self.main_cfg['dest_currency']:
            self.logger.debug("The min and max salaries [{}-{}] will be converted "
                              "from {} to {}".format(
                                min_salary,
                                max_salary,
                                base_currency,
                                self.main_cfg['dest_currency']))
            try:
                # Convert the min and max salaries to `dest_currency` (e.g. USD)
                min_salary_converted, timestamp = \
                    self.convert_currency(min_salary,
                                          base_currency,
                                          self.main_cfg['dest_currency'])
                max_salary_converted, _ = \
                    self.convert_currency(max_salary,
                                          base_currency,
                                          self.main_cfg['dest_currency'])
            except (RatesNotAvailableError, requests.exceptions.ConnectionError) as e:
                raise exc.CurrencyRateError(e)
            else:
                return {'min_salary': min_salary_converted,
                        'max_salary': max_salary_converted,
                        'currency': self.main_cfg['dest_currency'],
                        'conversion_time': timestamp}
        if base_currency == self.main_cfg['dest_currency']:
            raise exc.SameCurrencyError(
                "The min and max salaries [{}-{}] are already in the desired "
                "currency {}".format(min_salary,
                                     max_salary,
                                     self.main_cfg['dest_currency']))

    # Returns: tuple, integer of converted amount and datetime of conversion time
    # Raises:
    #   - NoneBaseCurrencyError by itself
    #   - RatesNotAvailableError by get_rate()
    #   - requests.exceptions.ConnectionError by get_rate()
    # Convert an amount from a base currency (e.g. EUR) to a destination currency (e.g. USD)
    # NOTE: `base_currency` and `dest_currency` are currency codes, e.g. USD, EUR, CAD
    def convert_currency(self, amount, base_currency, dest_currency):
        # Get the rate from cache if it is available
        rate_key = '{}_{}'.format(base_currency, dest_currency)
        rate_used = self.cached_rates.get(rate_key, {}).get('rate')
        conversion_time = self.cached_rates.get(rate_key, {}).get('conversion_time')
        if rate_used is None:
            # Rate not available from cache
            self.logger.debug("No cache rate found for {}-->{}".format(
                               base_currency, dest_currency))
            # Get the rate online and cache it
            try:
                rate_used = get_rate(base_currency, dest_currency)
                conversion_time = get_local_datetime()
            except RatesNotAvailableError as e:
                raise RatesNotAvailableError(e)
            except requests.exceptions.ConnectionError as e:
                raise requests.exceptions.ConnectionError(e)
            else:
                # Cache the rate
                self.logger.debug("The rate {} is cached for {}-->{}".format(
                                   rate_used, base_currency, dest_currency))
                self.cached_rates[rate_key] = {
                    'rate': rate_used,
                    'conversion_time': conversion_time}
        else:
            # Rate available from cache
            self.logger.debug("The cached rate {} is used for {}-->{}".format(
                               rate_used, base_currency, dest_currency))
        # Convert the base currency to the desired currency using the
        # retrieved rate
        converted_amount = int(round(rate_used * amount))
        # NOTE: round(a, 2) doesn't work in python 2.7:
        # >> a = 0.3333333
        # >> round(a, 2),
        # Use the following in python2.7:
        # >> float(format(a, '.2f'))
        return converted_amount, conversion_time

    # Returns: currency code, string
    # Raises:
    #   - NoCurrencyCodeError by itself
    #   - InvalidCountryError by itself
    def get_currency_code(self, currency_symbol):
        # First check if the currency symbol is not a currency code already
        if get_currency_name(currency_symbol):
            self.logger.debug(
                "The currency symbol '{}' is actually a currency code.")
            return currency_symbol
        # NOTE: there is no 1-to-1 mapping when going from currency symbol
        # to currency code
        # e.g. the currency symbol £ is used for the currency codes EGP, FKP,
        # GDP, GIP, LBP, and SHP
        # Search into the `currencies_data` list for all the currencies that have
        # the given `currency_symbol`. Each item in `currencies_data` is a dict
        # with the keys ['cc', 'symbol', 'name'].
        results = [item for item in self.currencies_data
                   if item["symbol"] == currency_symbol]
        # NOTE: C$ is used as a currency symbol for Canadian Dollar instead of $
        # However, C$ is already the official currency symbol for
        # Nicaragua Cordoba (NIO)
        # Thus we will assume that C$ is related to the Canadian Dollar.
        # NOTE: in stackoverflow job posts, '$' alone refers to US$ but '$' can
        # refer to multiple currency codes such as ARS (Argentine peso), AUD, CAD.
        # Thus, we will make an assumption that '$' alone will refer to US$ since
        # if it is in AUD or CAD, the currency symbols 'A$' and 'C$' are usually
        # used in job posts, respectively.
        if currency_symbol != "C$" and len(results) == 1:
            self.logger.debug("Found only one currency code {} associated with the "
                              "given currency symbol {}".format(
                                results[0]["cc"], currency_symbol))
            return results[0]["cc"]
        else:
            # Two possible cases
            # 1. Too many currency codes associated with the given currency symbol
            # 2. It is not a valid currency symbol
            if currency_symbol == "$":
                # United States dollar
                currency_code = "USD"
            elif currency_symbol == "A$":
                # Australian dollar
                currency_code = "AUD"
            elif currency_symbol == "C$":
                # Canadian dollar
                currency_code = "CAD"
            elif currency_symbol == "£":
                # We assume £ is always associated with the British pound
                # However, it could have been EGP, FKP, GIP, ...
                currency_code = "GBP"
            elif currency_symbol == "kr":  # Danish krone
                # Technically, 'kr' is a valid currency symbol for the Danish
                # krone 'kr' is not recognized because `forex_python` uses 'Kr'
                # as the currency symbol for the Danish krone.
                currency_code = "DKK"
            elif currency_symbol == "R":
                # There are two possibilities of currency codes with the
                # currency symbol 'R':
                # Russian Ruble (RUB) or South African rand (ZAR)
                # Check the current job post's country to determine which of
                # the two currency codes to choose from
                # TODO: test this part where the currency is Ruble (Russia)
                ipdb.set_trace()
                country = None
                if self.session.data.job_locations:
                    country = self.session.data.job_locations[0].country
                if country is None:
                    raise exc.NoCurrencyCodeError(
                        "Could not get a currency code from '{}'".format(
                            currency_symbol))
                elif country == 'ZA':
                    currency_code = 'ZAR'
                elif country == 'RU':
                    currency_code = "RUB"
                else:
                    raise exc.InvalidCountryError(
                        "The country '{}' is invalid and a currency code could "
                        "not be decided for the currency symbol 'R'".format(
                            country))
            else:
                raise exc.NoCurrencyCodeError(
                    "Could not get a currency code from '{}'".format(
                        currency_symbol))
            return currency_code

    # Get currency symbol located at the BEGINNING of the string `text`
    # e.g. '€42k - 75k'
    def get_currency_symbol(self, text):
        # Returned value is a tuple (currency_symbol, end)
        # NOTE: `end` refers to the index of the first digit in the
        # salary `text`, e.g. '€42k - 75k' --> end=1
        # Search for the symbol at the beginning of the text
        regex = r"^(\D+)"
        match = re.search(regex, text)
        if match:
            self.logger.debug(
                "Found currency {} in text {}".format(match.group(), text))
            # Some currencies have whitespaces at the end,
            # e.g. 'SGD 60k - 79k'. Thus, the strip()
            return match.group().strip(), match.end()
        else:
            raise exc.NoCurrencySymbolError(
                "No currency symbol could be retrieved from the salary text "
                "{}".format(text))

    # Get the location data in a linked data JSON object
    def get_loc_in_ld(self, linked_data):
        # TODO: some job post don't have a location in the RSS feed (i.e. no
        # office location) but do have lots of job locations in the JSON linked
        # data. e.g. job_post_id=203827
        job_locations = linked_data.get('jobLocation')
        if job_locations:
            if len(job_locations) > 1:
                self.logger.critical(
                    "There are {} job locations in the JSON linked data!".format(
                     len(job_locations)))
            processed_locations = []
            for location in job_locations:
                city = location.get('address', {}).get('addressLocality')
                region = location.get('address', {}).get('addressRegion')
                country = location.get('address', {}).get('addressCountry')
                if city == '-':
                    city = None
                if region == '-':
                    region = None
                if country == '-':
                    country = None
                location_dict =  {'city': city,
                                  'region': region,
                                  'country': country}
                processed_locations.append(location_dict)
            return processed_locations
        else:
            # TODO: no need to raise error, just return None
            # raise exc.NoJobLocationError("No job locations found in the linked data.")
            self.logger.warning("No job locations found in the linked data")
            return None

    @staticmethod
    # Extract the min and max salaries from a salary range
    # e.g. '42000 - 75000' --> min_salary=42000, max_salary=75000
    def get_min_max_salary(salary_range):
        min_salary, max_salary = salary_range.replace(" ", "").split("-")
        min_salary = int(min_salary)
        max_salary = int(max_salary)
        return min_salary, max_salary

    def get_webpage(self, url):
        current_delay = time.time() - self.last_request_time
        diff_between_delays = \
            current_delay - self.main_cfg['delay_between_requests']
        if diff_between_delays < 0:
            self.logger.debug("Waiting {} seconds before sending next HTTP "
                             "request...".format(abs(diff_between_delays)))
            time.sleep(abs(diff_between_delays))
            self.logger.debug("Time is up! HTTP request will be sent.")
        try:
            self.logger.debug("Sending HTTP request ...")
            req = self.req_session.get(url, headers=self.headers,
                                       timeout=self.main_cfg['http_get_timeout'])
            html = req.text
        except OSError as e:
            raise OSError(e)
        else:
            if req.status_code == 404:
                raise exc.PageNotFoundError(
                    "404 - PAGE NOT FOUND. The URL '{}' returned a 404 status "
                    "code.".format(url))
        self.last_request_time = time.time()
        self.logger.debug("Webpage retrieved!")
        return html

    def is_a_us_state(self, name):
        if self.us_states.get(name):
            # `name` is a U.S. state
            return True
        else:
            # `name` is not a U.S. state
            return False

    # Load the cached webpage HTML if the webpage is found locally. If it isn't
    # found locally, then we will try to retrieve it with a GET request
    def load_cached_webpage(self):
        html = ""
        # File path where the webpage (only HTML) will be saved
        cached_webpage_filepath = os.path.join(
            self.cached_webpages_dirpath,
            "{}.html".format(self.session.job_post_id))
        url = self.session.url
        # =====================================================================
        # 1st try: load the HTML page from cache
        # =====================================================================
        if self.cached_webpages_dirpath:
            try:
                self.logger.debug("Reading the HTML file '{}' from cache".format(
                    cached_webpage_filepath))
                html = read_file(cached_webpage_filepath)
            except OSError as e:
                self.logger.exception(e)
            else:
                self.logger.debug(
                    "The cached webpage HTML is loaded from '{}'".format(
                        cached_webpage_filepath))
                # Get the webpage's datetime modified as the datetime the
                # webpage was originally accessed
                self.session.data.set_job_post(
                    cached_webpage_filepath=cached_webpage_filepath,
                    webpage_accessed=datetime.fromtimestamp(
                        os.path.getmtime(cached_webpage_filepath)))
                return html
        else:
            self.logger.warning("The caching option is disabled")

        self.logger.warning("The webpage HTML @ '{}' will be retrieved".format(url))
        # =====================================================================
        # 2nd try: get the webpage HTML with an HTTP request
        # =====================================================================
        # TODO: the 2nd try should be done in a separate method
        webpage_accessed = None
        try:
            html = self.get_webpage(url)
            # Get the datetime the webpage was retrieved (though not 100%
            # accurate)
            webpage_accessed = get_local_datetime()
            if self.cached_webpages_dirpath:
                self.save_webpage_locally(url, cached_webpage_filepath, html)
        except (OSError, exc.HTTP404Error) as e:
            # from `get_webpage()`
            raise exc.WebPageNotFoundError(e)
        except exc.WebPageSavingError as e:
            # from `save_webpage_locally()`
            # IMPORTANT: even if the webpage couldn't be saved locally, the
            # the webpage will be returned as `html` to the calling method
            self.logger.exception(e)
            self.logger.warning("The webpage @ '{}' will not be saved "
                                "locally".format(url))
            cached_webpage_filepath = None
        else:
            self.logger.info("The webpage was saved in '{}'. URL is "
                             "'{}'".format(cached_webpage_filepath, url))
        self.session.data.set_job_post(
            cached_webpage_filepath=cached_webpage_filepath,
            webpage_accessed=webpage_accessed)
        return html

    def save_webpage_locally(self, url, filepath, html):
        try:
            self.logger.debug("Saving webpage to '{}'".format(filepath))
            write_file(filepath, html)
        except OSError as e:
            raise exc.WebPageSavingError("the webpage @ '{}' will not be "
                                         "saved locally.".format(url))
        else:
            self.logger.debug("Webpage saved!")

    def select_entries(self):
        """
        Returns all (job_post_id, title, author, url, location, published) from
        the `entries` table

        :return:
        """
        sql = "SELECT job_post_id, title, author, url, location, published FROM " \
              "entries"
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def standardize_country(self, country):
        # TODO: automate this part here by using a translator service
        # Converts a country name to the alpha2 code
        invalid_country_log_msg = "The country '{}' is not a valid country. " \
                                  "Instead, '{}' will be used as the " \
                                  "alpha2 code."
        # Return already the alpha2 code for those countries not recognized by
        # `pycountry_convert`
        if country == 'UK':
            # UK' is not recognized by `pycountry_convert` as a valid
            # country. 'United Kingdom' associated with the 'GB' alpha2 code are
            # used instead
            self.logger.debug(invalid_country_log_msg.format(country, 'GB'))
            return 'GB'
        elif country == 'Deutschland':
            # 'Deutschland' (German for Germany) is not recognized by
            # `pycountry_convert` as a valid country. 'Germany' associated with
            # the 'DE' alpha2 code are used instead
            self.logger.debug(invalid_country_log_msg.format(country, 'DE'))
            return 'DE'
        elif country == 'Österreich':
            # 'Österreich' (German for Austria) is not recognized by
            # `pycountry_convert` as a valid country. 'Austria' associated with
            # the 'AT' alpha2 code are used instead
            self.logger.debug(invalid_country_log_msg.format(country, 'AT'))
            return 'AT'
        elif country == 'Vereinigtes Königreich':
            # 'Vereinigtes Königreich' (German for United Kingdom) is not
            # recognized by `pycountry_convert` as a valid country.
            # 'United Kingdom' associated with the 'GB' alpha2 code are used
            # instead
            self.logger.debug(invalid_country_log_msg.format(country, 'GB'))
            return 'GB'
        elif country == 'Schweiz':
            # 'Schweiz' (German for Switzerland) is not recognized by
            # `pycountry_convert` as a valid country. 'Switzerland' associated
            # with the 'CH' alpha2 code are used instead
            self.logger.debug(invalid_country_log_msg.format(country, 'CH'))
            return 'CH'
        elif country == 'Spanien':
            # 'Spanien' (German for Spain) is not recognized by
            # `pycountry_convert` as a valid country. 'Spain' associated with the
            # 'ES' alpha2 code are used instead
            self.logger.debug(invalid_country_log_msg.format(country, 'ES'))
            return 'ES'
        try:
            alpha2 = country_name_to_country_alpha2(country)
        except KeyError as e:
            raise KeyError(e)
        else:
            self.logger.debug("The country '{}' will be updated to the standard "
                              "name '{}'.".format(country, alpha2))
            return alpha2

    @staticmethod
    def str_to_date(str_date):
        # e.g. '2018-08-01' or '2018-09-24 05:22:56-04:00'
        n_items = len(str_date.split())
        assert 1 <= n_items <= 2
        if n_items == 2:
            # case: '2018-09-24 05:22:56-04:00'
            str_date = str_date.split()[0]
        year, month, day = [int(i) for i in str_date.split('-')]
        return date(year, month, day)

    @staticmethod
    def str_to_list(str_v):
        # If string of comma-separated values
        # (e.g. 'Architecture, Developer APIs, Healthcare'),
        # return a list of values instead,
        # e.g. ['Architecture', 'Developer APIs', 'Healthcare']
        try:
            items_list = [v.strip() for v in str_v.split(',')]
        except AttributeError as e:
            raise AttributeError(e)
        else:
            return items_list

    @staticmethod
    def strip_list(items_list):
        try:
            items_list = [i.strip() for i in items_list]
        except AttributeError as e:
            raise AttributeError(e)
        else:
            return items_list


if __name__ == '__main__':
    sb = ScriptBoilerplate(
        module_name=__name__,
        module_file=__file__,
        cwd=os.getcwd(),
        parser_desc="Web scrape job posts from stackoverflow.com/jobs",
        parser_formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    sb.parse_args()
    logger = sb.get_logger()
    status_code = 1
    try:
        logger.info("Loading the config file '{}'".format(sb.args.main_cfg))
        main_cfg = read_yaml_config(sb.args.main_cfg)
        logger.info("Config file loaded!")
        # Start the scraping of job posts
        logger.info("Starting the web scraping")
        JobsScraper(main_cfg=main_cfg,
                    logging_cfg=sb.logging_cfg_dict,
                    logger=logger).start_scraping()
    except (FileNotFoundError, KeyboardInterrupt, OSError, sqlite3.Error,
            sqlite3.OperationalError, exc.EmptyQueryResultSetError) as e:
        logger.exception(e)
        logger.warning("Program will exit")
    else:
        status_code = 0
        logger.info("End of the web scraping")
    sys.exit(status_code)
