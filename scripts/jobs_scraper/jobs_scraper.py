from datetime import date
import json
import os
import re
import sqlite3
import sys
import time

from bs4 import BeautifulSoup
from forex_python.converter import get_currency_name, get_rate, RatesNotAvailableError
from pycountry_convert import country_name_to_country_alpha2
import requests
import ipdb

# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects/dev_jobs_insights/database"))
from job_post_data import JobPostData
import js_exceptions as js_e
from scraping_session import ScrapingSession
from tables import ValueOverrideError
from utility import genutil as g_util


DB_FILEPATH = os.path.expanduser("~/databases/dev_jobs_insights.sqlite")
# NOTE: if `CACHED_WEBPAGES_DIRPATH` is None, then the webpages will not be cached
# The webpages will then be retrieved from the internet.
CACHED_WEBPAGES_DIRPATH = os.path.expanduser("~/data/dev_jobs_insights/cache/webpages/stackoverflow_job_posts/")
SCRAPED_JOB_DATA_FILEPATH = os.path.expanduser("~/data/dev_jobs_insights/scraped_job_data.json")
CURRENCY_FILEPATH = os.path.expanduser("~/data/dev_jobs_insights/currencies.json")
US_STATES_FILEPATH = os.path.expanduser("~/data/dev_jobs_insights/us_states.json")
DELAY_BETWEEN_REQUESTS = 2
HTTP_GET_TIMEOUT = 5
# TODO: debug code
DEBUG = False
DEST_CURRENCY = "USD"
DEST_SYMBOL = "$"


class JobsScraper:
    def __init__(self, autocommit=False):
        self.session = None
        self.autocommit = autocommit
        # Db connection
        self.conn = None
        # Establish a session to be used for the GET requests
        self.req_session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) "
                          "AppleWebKit 537.36 (KHTML, like Gecko) Chrome",
            "Accept": "text/html,application/xhtml+xml,application/xml;"
                      "q=0.9,image/webp,*/*;q=0.8"
        }
        self.scraped_job_posts = {}
        self.last_request_time = -sys.float_info.max
        # `currency_data` is a list of dicts. Each item in `currency_data` is a
        # dict with the keys ['cc', 'symbol', 'name'] where 'cc' is short for
        # currency code
        self.currency_data = g_util.load_json(CURRENCY_FILEPATH)
        # Load the dict of US states where the keys are the USPS 2-letter codes
        # for the U.S. state and the values are the names
        # e.g. 'AZ': 'Arizona'
        self.us_states = g_util.load_json(US_STATES_FILEPATH)
        # Reverse the dict of U.S. states to search based on the full name
        # instead of the 2-letter codes
        # TODO: save the reversed US states in a JSON file and load it here,
        # like you did with `us_states`
        self.reversed_us_states \
            = self.us_states.__class__(map(reversed, self.us_states.items()))
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
        try:
            self.conn = g_util.connect_db(DB_FILEPATH)
        except sqlite3.Error as e:
            raise sqlite3.Error(e)
        with self.conn:
            # Get all the entries' URLs
            try:
                rows = self.select_entries()
            except sqlite3.OperationalError as e:
                self.print_log("CRITICAL", "Web scraping will end!")
                raise sqlite3.OperationalError(e)
            else:
                if not rows:
                    raise js_e.EmptyQueryResultSetError(
                        "The returned query result set is empty. Web scraping "
                        "will end!")
        # For each entry's URL, scrape more job data from the job post's webpage
        count = 1
        n_skipped = 0
        self.print_log("INFO", "Total URLs to process = {}".format(len(rows)))
        for job_id, author, url in rows:

            try:
                print()
                self.print_log("", "#{} Processing {}".format(count, url))
                count += 1

                # Initialize the current scraping session
                self.session = ScrapingSession(job_id, url=url, data=JobPostData())
                self.print_log("INFO", "Scraping session initialized")
                # Update the job post's URL
                self.session.data.set_job_post(url=url)

                # Load the cached webpage or retrieve it online
                try:
                    html = self.load_cached_webpage()
                except js_e.WebPageNotFoundError as e:
                    self.print_log("ERROR", exception=e)
                    self.print_log("CRITICAL", "The current URL {} will be skipped.".format(url))
                    continue

                self.session.bs_obj = BeautifulSoup(html, 'lxml')

                # Before extracting any job data from the job post, check if the job is
                # accepting applications by extracting the message
                # "This job is no longer accepting applications."
                # This notice is located in
                # body > div.container > div#content > aside.s-notice
                # NOTE: Usually when this notice is present in a job post, the json job
                # data is not found anymore within the html of the job post
                self.process_notice()

                # Get linked data from <script type="application/ld+json">
                self.process_linked_data()

                # Get job data (e.g. salary, remote, location) from the <header>
                self.process_header()

                # Get job data from the Overview section
                self.process_overview_items()

                self.print_log("INFO", "Finished Processing {}".format(url))
            except KeyError as e:
                self.print_log("ERROR", "KeyError: {}".format(e.__str__()))
                self.print_log("WARNING", "The current URL {} will be skipped".format(url))
                n_skipped += 1
            else:
                # TODO: save some things
                pass
            finally:
                self.print_log("INFO", "Session ending")
                self.session.reset()

        ipdb.set_trace()

        print()
        # Save scraped data into json file
        # ref.: https://stackoverflow.com/a/31343739 (presence of unicode strings,
        # e.g. EURO currency symbol)
        try:
            g_util.dump_json_with_codecs(SCRAPED_JOB_DATA_FILEPATH, self.scraped_job_posts)
        except OSError as e:
            self.print_log("ERROR", "Scraped data couldn't be saved")
        else:
            self.print_log("INFO", "Scraped data saved in {}".format(SCRAPED_JOB_DATA_FILEPATH))
        finally:
            self.print_log("INFO", "Skipped URLs={}/{}".format(n_skipped, len(rows)))

    def get_dict_value(self, key):
        return self.scraped_job_posts[self.job_id].get(key)

    def get_country_from_dict(self):
        return self.scraped_job_posts.get(self.job_id).get('office_location', {}).get('country')

    def update_dict(self, updated_values):
        for key, new_value in updated_values.items():
            log_msg = "Trying to update {{key, value}}={{{}, {}}}".format(key, new_value)
            self.print_log("DEBUG", log_msg)
            current_value = self.get_dict_value(key)
            if current_value is None:
                # Check that the key is a valid job data key
                if key in self.scraped_job_posts[self.job_id]:
                    # Do a last-resort processing of `value` if it is a string
                    if type(new_value) is str:
                        if new_value[0] == ' ' or new_value[-1] == ' ':
                            log_msg = "There is a space in the value " \
                                      "'{}'. key={}".format(new_value, key)
                            self.print_log("DEBUG", log_msg)
                            new_value = new_value.strip()
                    self.scraped_job_posts[self.job_id].update({key: new_value})
                    log_msg = "The key='{}' was updated with value='{}'".format(key, new_value)
                    self.print_log("DEBUG", log_msg)
                else:
                    self.print_log("CRITICAL", "The key='{}' is not a valid job data key.".format(key))
            else:
                log_msg = "The key='{}' already has a value='{}'. Thus the " \
                          "new_value='{}' will be ignored.".format(key, current_value, new_value)
                self.print_log("DEBUG", log_msg)
                if current_value != new_value:
                    log_msg = "The new_value='{}' is not equal to current_value='{}'".format(new_value, current_value)
                    self.print_log("INFO", log_msg)

    @staticmethod
    def process_company_size(company_size):
        # Example: '1k-5k people' --> '1000-5000'
        # Replace the letter 'k' with '000'
        company_size = company_size.replace('k', '000')
        # Remove 'people' and remove any whitespace around the string
        company_size = company_size.split('people')[0].strip()
        return company_size

    @staticmethod
    # Employment type = job type
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

        # 1. Get title of job post
        # First check if the title was not already extracted before
        title = self.session.data.job_post.title
        if title:
            # Title already extracted
            self.print_log("DEBUG",
                           "The job post's title already has "
                           "a value='{}'".format(title))
            self.print_log("DEBUG",
                           "The job post's title {} is ignored".format(title))
        else:
            # Extract title from text in tag
            try:
                pattern = "header.job-details--header > div.grid--cell > " \
                          "h1.fs-headline1 > a"
                title = self.get_text_in_tag(pattern)
                self.session.data.set_job_post(title=title)
            except js_e.TagNotFoundError as e:
                self.print_log("DEBUG", exception=e)
            except js_e.EmptyTextError as e:
                self.print_log("ERROR", exception=e)
            except ValueOverrideError as e:
                self.print_log("WARNING", exception=e)
            else:
                self.print_log("DEBUG",
                               "The title {} was saved.".format(title))

        # 2. Get company name
        # First check if the company name was not already extracted before
        company_name = self.session.data.company.name
        if company_name:
            # Company name already extracted
            self.print_log("DEBUG",
                           "The company name already has "
                           "a value='{}'".format(company_name))
            self.print_log("DEBUG",
                           "The company name {} is ignored".format(company_name))
        else:
            try:
                pattern = "header.job-details--header > div.grid--cell > " \
                          "div.fc-black-700 > a"
                company_name = self.get_text_in_tag(pattern)
                self.session.data.set_company(name=company_name)
            except js_e.TagNotFoundError as e:
                self.print_log("DEBUG", exception=e)
            except js_e.EmptyTextError as e:
                self.print_log("ERROR", exception=e)
            except ValueOverrideError as e:
                self.print_log("WARNING", exception=e)
            else:
                self.print_log("DEBUG",
                               "The company name {} "
                               "was saved.".format(company_name))

        # 3. Get the office location which is located on the same line as the
        # company name
        try:
            pattern = "header.job-details--header > div.grid--cell > " \
                      "div.fc-black-700 > span.fc-black-500"
            text = self.get_text_in_tag(pattern)
            # Process the location text
            # We want to standardize the country (e.g. Finland --> FI)
            location = self.process_location_text(text)
            self.session.data.set_job_location(**location)
        except js_e.TagNotFoundError as e:
            self.print_log("DEBUG", exception=e)
        except js_e.EmptyTextError as e:
            self.print_log("ERROR", exception=e)
        except ValueOverrideError as e:
            self.print_log("WARNING", exception=e)
        except (KeyError, js_e.InvalidLocationTextError) as e:
            self.print_log("ERROR", exception=e)
        else:
            self.print_log("DEBUG",
                           "The location {} was saved.".format(location))

        # 4. Get the other job data on the next line after the company name
        # and location
        pattern = "header.job-details--header > div.grid--cell > div.mt12"
        div_tag = bs_obj.select_one(pattern)
        ipdb.set_trace()
        if div_tag:
            # Each `div_tag`'s child is associated to a job item
            # (e.g. salary, remote) and is a <span> tag with a class that
            # starts with '-'
            # Example:
            # header.job-details--header > div.grid--cell > .mt12 >
            # span.-salary.pr16
            children = div_tag.findChildren()
            for child in children:
                # Each job data text is found within <span> with a class that
                # starts with '-', e.g. <span class='-salary pr16'>
                # NOTE: we need the child element's class that starts with '-'
                # because we will then know how to name the extracted job
                # data item
                child_class = [tag_class for tag_class in child.attrs['class']
                               if tag_class.startswith('-')]
                if child_class: # class that starts with '-'
                    # Get the <div>'s class name without the '-' at the beginning,
                    # this will correspond to the type of job data (e.g. salary,
                    # remote, relocation, visa)
                    key_name = child_class[0][1:]
                    value = child.text
                    if value:  # value = text
                        self.print_log("INFO",
                                       "The {} is found".format(key_name))
                        # Get the text (e.g. $71k - 85k) by removing any \r and
                        # \n around the string
                        value = value.strip()
                        if key_name == 'salary':
                            salaries = self.session.data.job_salaries
                            if False and self.session.data.job_salaries:
                                # Job salaries already extracted
                                self.print_log("DEBUG",
                                               "The job salaries were already "
                                               "extracted previously {}".format(
                                                   salaries[0]))
                                self.print_log("DEBUG",
                                               "The job salaries {} are "
                                               "ignored".format(company_name))
                            else:
                                try:
                                    updated_values \
                                        = self.process_salary_text(value)
                                except (js_e.CurrencyRateError,
                                        js_e.NoCurrencySymbolError) as e:
                                    self.print_log("ERROR", exception=e)
                                else:
                                    self.print_log("DEBUG",
                                                   "Updating dict with salary "
                                                   "values (min_salary, "
                                                   "max_salary, ...)")
                                    self.session.data.set_job_salary(**updated_values)
                        else:
                            self.print_log("DEBUG",
                                           "Updating dict with {{{}:{}}})".format(
                                               key_name, value))
                            # self.session.data.set_job_salary(**{key_name: value})
                    else:
                        self.print_log("ERROR",
                                       "No text found for the job data "
                                       "with key '{}'.".format(key_name))
                else:
                    self.print_log("ERROR",
                                   "The <span>'s class doesn't start with '-'.")
        else:
            self.print_log("INFO",
                           "Couldn't extract the other job data @ URL {}. "
                           "The other job data should be found in {}".format(
                               url, pattern))

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

            def str_to_date(str_date):
                # e.g. '2018-08-01'
                year, month, day = [int(i) for i in str_date.split('-')]
                return date(year, month, day)

            # Extract data for populating the `job_posts` table
            date_posted = str_to_date(linked_data.get('datePosted'))
            valid_through = str_to_date(linked_data.get('validThrough'))
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
                description=linked_data.get('hiringOrganization', {}).get('description'))

            def process_values(values, set_method_name):
                try:
                    if isinstance(values, str):
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
                        if isinstance(value, dict):
                            set_method(**value)
                        else:
                            set_method(name=value)
                except AttributeError as e:
                    self.print_log("ERROR", exception=e)

            # Extract data for populating the `job_locations` table
            process_values(self.get_loc_in_ld(linked_data), 'set_job_location')
            # Extract data for populating the `experience_levels` table
            process_values(linked_data.get('experienceRequirements'),
                           'set_experience_level')
            # Extract data for populating the `industries` table
            process_values(linked_data.get('industry'), 'set_industry')
            # Extract data for populating the `skills` table
            process_values(linked_data.get('skills'), 'set_skill')
            # Extract data for populating the `job_benefits` table
            process_values(linked_data.get('jobBenefits'), 'set_job_benefit')

            # Convert the minimum and maximum salaries to
            # `DEST_CURRENCY` (e.g. USD)
            try:
                results = self.convert_min_and_max_salaries(min_salary,
                                                            max_salary,
                                                            currency)
            except js_e.CurrencyRateError as e:
                self.print_log("ERROR", exception=e)
            except js_e.SameCurrencyError as e:
                self.print_log("DEBUG", exception=e)
            except js_e.NoneBaseCurrencyError as e:
                self.print_log("DEBUG", exception=e)
            else:
                self.session.data.set_job_salary(min_salary=results[0],
                                                 max_salary=results[1],
                                                 currency=results[2],
                                                 conversion_time=results[3])
            finally:
                self.print_log("INFO",
                               "Finished processing the linked "
                               "data from URL {}".format(url))
        else:
            # Reasons for not finding <script type='application/ld+json'>:
            # maybe the page is not found anymore (e.g. job post removed) or
            # the company is not longer accepting applications
            log_msg = "The page @ URL {} doesn't contain any SCRIPT tag with " \
                      "type='application/ld+json'".format(url)
            self.print_log("INFO", log_msg)

    def process_location_text(self, text):
        updated_values = {}
        # The text where you find the location looks like this:
        # '\n|\r\nNo office location                    '
        # strip() removes the first newline and the right white spaces.
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
            log_msg = "No commas found in location text '{}'. We will assume " \
                      "that the location text '{}' refers to a country.".format(
                        text, text)
            self.print_log("WARNING", log_msg)
            # Save country, no more information can be extracted
            updated_values['country'] = text
        elif text.count(',') == 1:
            # One comma in location text
            # Example 1: 'Bellevue, WA'
            # Example 2: 'Helsinki, Finland'
            self.print_log("DEBUG",
                           "Found 1 comma in the location "
                           "text '{}'".format(text))
            # Save city and country
            updated_values = dict(zip(['city', 'country'], text.split(',')))
            # Do further processing on the country since it might refer in fact
            # to a U.S. state, e.g. 'Bellevue, WA'. For U.S. jobs, the job
            # posts don't specify the country as it is the case for job posts
            # for other countries.
            if self.is_a_us_state(updated_values['country']):
                self.print_log("DEBUG",
                               "The location text '{}' refers to a place "
                               "in the US".format(text))
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
            self.print_log("DEBUG",
                           "Found 2 commas in the location "
                           "text '{}'".format(text))
            updated_values = dict(zip(['city', 'region', 'country'],
                                      text.split(',')))
        else:
            # Incorrect number of commas in location text. Thus we can't extract
            #
            raise js_e.InvalidLocationTextError("Invalid location text '{}'. "
                                                "Incorrect number "
                                                "of commas.".format(text))
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
        except js_e.EmptyTextError as e:
            self.print_log("ERROR", exception=e)
        except js_e.TagNotFoundError as e:
            self.print_log("DEBUG", exception=e)
        except AttributeError as e:
            # TODO: catch AttributeError every time we set a column to a record?
            # check other places where we set a record's column and don't catch
            # the error
            self.print_log("CRITICAL", exception=e)
        else:
            self.print_log("DEBUG",
                           "Job notice found @ URL {}: {}".format(
                               self.session.url, text))

    def process_overview_items(self, bsObj):
        # Get job data from the Overview section. There are three places within
        # the Overview section that will be extracted for more job data:
        # 1. in the "High response rate" sub-section of Overview
        # 2. in the "About this job" sub-section of Overview
        # 3. in the "Technologies" sub-section of Overview
        # NOTE: these sub-sections are located within <div id=""overview-items>
        # [overview-items]
        url = self.get_dict_value('url')

        # [high_response_rate]
        # 1. The high response rate might not be present (it isn't often we get
        # to see this notice)
        self.process_text_in_tag(bsObj, pattern=".-high-response > .-text > .-title",
                                 key_name='high_response_rate')

        # [about_this_job]
        # 2. Get more job data (e.g. role, industry, company size) in the
        # "About this job" section. Each item is located in
        # "#overview-items > .mb32 > .job-details--about > .grid--cell6 > .mb8"
        # NOTE: these job data are presented in two columns, with three items per column
        pattern = "#overview-items > .mb32 > .job-details--about > .grid--cell6 > .mb8"
        div_tags = bsObj.select(pattern)
        # Standardize the key names used in the "About this" section
        # e.g. 'Job type' should be replaced with 'Employment type'
        convert_keys = {'Job type': 'Employment type',
                        'technologies': 'skills'}
        # Prefix to add at the beginning of the log messages
        pre = "[About this]"
        if div_tags:
            # Each `div_tag` corresponds to a job data item
            # e.g. Job type: Full-time, Company type: Private
            for div_tag in div_tags:
                # Sample raw text: '\nJob type: \nContract\n'
                temp = div_tag.text.strip().split(":")
                key_name, value = temp[0].strip(), temp[1].strip()
                # Convert the key name to use the standard key name
                key_name = convert_keys.get(key_name, key_name)
                # The key names should all be lowercase and spaces be replaced
                # with underscores e.g. Employment type ---> employment_type
                key_name = key_name.replace(" ", "_").lower()
                # Comma-separated values should be converted to a list
                # The keys names with comma-separated values are:
                # experience_level, role, industry
                # e.g. Mid-Level, Senior, Lead  --> [Mid-Level, Senior, Lead]
                if key_name in ['experience_level', 'role', 'industry']:
                    self.print_log("DEBUG",
                                   "{} The value '{}' will be converted to "
                                   "a list".format(pre, value))
                    value = self.str_to_list(value)
                elif key_name == 'company_size':
                    # '1k-5k people' --> '1000-5000'
                    new_value = self.process_company_size(value)
                    log_msg = "{} The company size '{}' was processed to " \
                              "'{}'".format(pre, value, new_value)
                    self.print_log("DEBUG", log_msg)
                    value = new_value
                elif key_name == 'employment_type':
                    new_value = self.process_employment_type(value)
                    log_msg = "{} The employment type '{}' was processed to " \
                              "'{}'".format(pre, value, new_value)
                    self.print_log("DEBUG", log_msg)
                    value = new_value
                else:
                    self.print_log("DEBUG",
                                   "{} No extra processing done on "
                                   "'{}: {}'".format(pre, key_name, value))
                self.print_log("INFO",
                               "{} The item '{}: {}' will be "
                               "added".format(pre, key_name, value))
                self.update_dict({key_name: value})
        else:
            log_msg = "{} Couldn't extract job data from the 'About this job' " \
                      "section @ the URL {}. The job data should be found " \
                      "in {}".format(pre, url, pattern)
            self.print_log("WARNING", log_msg)

        # [technologies]
        # 3. Get the list of technologies, e.g. ruby, python, html5
        # NOTE: unlike the other job data in "overview_items", the technologies
        # are returned as a list
        pattern = "#overview-items > .mb32 > div > a.job-link"
        link_tags = bsObj.select(pattern)
        skills = []
        if link_tags:
            for link_tag in link_tags:
                technology = link_tag.text
                if technology:
                    self.print_log("DEBUG",
                                   "Skill {} extracted".format(technology))
                    skills.append(technology)
                else:
                    log_msg = "No text found for the technology with " \
                              "href={}. URL @ {}".format(link_tag["href"], url)
                    self.print_log("WARNING", log_msg)
            if skills:
                log_msg = "These skills {} were successfully extracted from the " \
                          "Technologies section".format(skills)
                self.print_log("INFO", log_msg)
                self.update_dict({'skills': skills})
            else:
                log_msg = "No skills extracted from the Technologies section"
                self.print_log("WARNING", log_msg)
        else:
            log_msg = "Couldn't extract technologies from the Technologies " \
                      "section @ the URL {}. The technologies should be found " \
                      "in {}".format(url, pattern)
            self.print_log("INFO", log_msg)

    def process_salary_range(self, salary_range):
        updated_values = []
        # Extract the currency symbol at the beginning of the salary range text
        # e.g. '€' will be extracted from €42k - 75k'
        # `results` is either:
        #       - a tuple (currency_symbol, end) or
        #       - None (if no currency symbol could be extracted)
        #
        # NOTE: `end` refers to the position of the first number in the
        # `salary_range` text, e.g. end=1 if salary_range='€42k - 75k'
        try:
            results = self.get_currency_symbol(salary_range)
        except js_e.NoCurrencySymbolError as e:
            raise js_e.NoCurrencySymbolError(e)
        # `results` is a tuple
        currency_symbol, end = results
        # Get the currency code based on the currency symbol
        currency_code = self.get_currency_code(currency_symbol)
        # Get the salary range only without the currency symbol at the
        # beginning, e.g. '€42k - 75k' --> '42k - 75k'
        salary_range = salary_range[end:]
        # Replace the letter 'k' with '000', e.g. 42k --> 42000
        salary_range = salary_range.replace('k', '000')
        # Get the minimum and maximum salary separately
        # e.g. '42000 - 75000' --> min_salary=42000, max_salary=75000
        min_salary, max_salary = self.get_min_max_salary(salary_range)
        updated_values.append({'currency': currency_code,
                               'min_salary': min_salary,
                               'max_salary': max_salary
                               })
        # Convert the min and max salaries to DEST_CURRENCY (e.g. USD)
        try:
            results = self.convert_min_and_max_salaries(min_salary,
                                                        max_salary,
                                                        currency_code)
        except js_e.CurrencyRateError as e:
            raise js_e.CurrencyRateError(e)
        except js_e.SameCurrencyError as e:
            self.print_log("DEBUG", exception=e)
        else:
            updated_values.append(results)
        finally:
            return updated_values

    def process_salary_text(self, salary_text):
        updated_values = []
        # Check if the salary text contains 'Equity', e.g. '€42k - 75k | Equity'
        if 'Equity' in salary_text:
            self.print_log("DEBUG",
                           "Equity found in the salary "
                           "text {}".format(salary_text))
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
                self.print_log("DEBUG",
                               "No salary found, only equity "
                               "in '{}'".format(salary_text))
                salary_range = None
            # Save equity but not salary range since salary range must be
            # further processed to extract the min and max salaries which are
            # the useful information we want to save
            updated_values.append({'equity': True})
        else:
            # Case: salary and no equity
            self.print_log("DEBUG",
                           "Equity is not found in the "
                           "salary text '{}'".format(salary_text))
            # Save the salary range
            salary_range = salary_text
        # Process the salary range to extract the min and max salaries
        if salary_range:
            try:
                results = self.process_salary_range(salary_range)
            except js_e.NoCurrencySymbolError as e:
                raise js_e.NoCurrencySymbolError(e)
            except js_e.CurrencyRateError as e:
                raise js_e.CurrencyRateError(e)
            else:
                self.print_log("DEBUG",
                               "The salary text {} was successfully processed!")
                updated_values.extend(results)
        return updated_values

    def get_text_in_tag(self, pattern):
        url = self.session.url
        tag = self.session.bs_obj.select_one(pattern)
        if tag:
            text = tag.text
            # Remove any white spaces around the string
            text = text.strip()
            if text:
                self.print_log("INFO",
                               "The text {} is found.".format(text))
                return text
            else:
                raise js_e.EmptyTextError("The text is empty")
        else:
            raise js_e.TagNotFoundError("Couldn't extract the text. The text "
                                        "should be found in {}. URL @ {}".format(
                                            pattern, url))

    # Convert the min and max salaries to `base_currency` (e.g. USD)
    def convert_min_and_max_salaries(self, min_salary, max_salary, base_currency):
        # Check first that the base currency is different from the destination
        # currency, e.g. USD-->USD
        if base_currency != DEST_CURRENCY:
            self.print_log("DEBUG",
                           "The min and max salaries [{}-{}] will be converted "
                           "from {} to {}".format(
                               min_salary, max_salary, base_currency, DEST_CURRENCY))
            try:
                # Convert the min and max salaries to `DEST_CURRENCY` (e.g. USD)
                min_salary_converted, timestamp = \
                    self.convert_currency(min_salary, base_currency, DEST_CURRENCY)
                max_salary_converted, _ = \
                    self.convert_currency(max_salary, base_currency, DEST_CURRENCY)
            except (RatesNotAvailableError, requests.exceptions.ConnectionError) as e:
                raise js_e.CurrencyRateError(e)
            except js_e.NoneBaseCurrencyError as e:
                raise js_e.NoneBaseCurrencyError(e)
            else:
                return min_salary_converted, max_salary_converted, DEST_CURRENCY, timestamp
        else:
            raise js_e.SameCurrencyError("The min and max salaries [{}-{}] are "
                                         "already in the desired currency {}".format(
                                            min_salary, max_salary, DEST_CURRENCY))

    # Convert an amount from a base currency (e.g. EUR) to a destination currency (e.g. USD)
    # NOTE: `base_currence` and `dest_currency` are currency codes, e.g. USD, EUR, CAD
    def convert_currency(self, amount, base_currency, dest_currency):
        # Sanity check on the base currency
        if base_currency is None:
            raise js_e.NoneBaseCurrencyError("The base currency code is None")
        # Get the rate from cache if it is available
        rate_used = self.cached_rates.get('{}_{}'.format(base_currency,
                                                         dest_currency))
        if rate_used is None:
            # Rate not available from cache
            self.print_log("DEBUG",
                           "No cache rate found for {}-->{}".format(
                               base_currency, dest_currency))
            # Get the rate online and cache it
            try:
                rate_used = get_rate(base_currency, dest_currency)
            except RatesNotAvailableError as e:
                raise RatesNotAvailableError(e)
            except requests.exceptions.ConnectionError as e:
                raise requests.exceptions.ConnectionError(e)
            else:
                # Cache the rate
                self.print_log("DEBUG",
                               "The rate {} is cached for {}-->{}".format(
                                   rate_used, base_currency, dest_currency))
                rate_key = '{}_{}'.format(base_currency, dest_currency)
                self.cached_rates[rate_key] = rate_used
        else:
            # Rate available from cache
            self.print_log("DEBUG",
                           "The cached rate {} is used for {}-->{}".format(
                               rate_used, base_currency, dest_currency))
        # Convert the base currency to the desired currency using the
        # retrieved rate
        converted_amount = int(round(rate_used * amount))
        # NOTE: round(a, 2) doesn't work in python 2.7:
        # >> a = 0.3333333
        # >> round(a, 2),
        # Use the following in python2.7:
        # >> float(format(a, '.2f'))
        return converted_amount, time.time()

    def get_currency_code(self, currency_symbol):
        # First check if the currency symbol is not a currency code already
        if get_currency_name(currency_symbol):
            self.print_log("DEBUG",
                           "The currency symbol '{}' is actually a currency code.")
            return currency_symbol
        # NOTE: there is no 1-to-1 mapping when going from currency symbol
        # to currency code
        # e.g. the currency symbol £ is used for the currency codes EGP, FKP, GDP,
        # GIP, LBP, and SHP
        # Search into the `currency_data` list for all the currencies that have
        # the given `currency_symbol`. Each item in `currency_data` is a dict
        # with the keys ['cc', 'symbol', 'name'].
        results = [item for item in self.currency_data
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
            self.print_log("DEBUG",
                           "Found only one currency code {} associated with the "
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
                # Technically, 'kr' is a valid currency symbol for the Danish krone
                # 'kr' is not recognized because `forex_python` uses 'Kr' as the
                # currency symbol for the Danish krone.
                currency_code = "DKK"
            elif currency_symbol == "R":
                # There are two possibilities: Russian Ruble (RUB) or
                # South African rand (ZAR)
                # Check the job post's country to determine which of the two
                # currency codes to choose from
                country = self.get_country_from_dict()
                if country is None:
                    self.print_log("ERROR",
                                   "Could not get a currency code "
                                   "from '{}'".format(currency_symbol))
                    return None
                elif country == 'ZA':
                    currency_code = 'ZAR'
                else:
                    currency_code = "RUB"
            else:
                self.print_log("ERROR",
                               "Could not get a currency code "
                               "from '{}'".format(currency_symbol))
                return None
            return currency_code

    # Get currency symbol located at the BEGINNING of the string
    # e.g. '€42k - 75k'
    def get_currency_symbol(self, text):
        # returned value is a tuple (currency_symbol, end)
        # NOTE: `end` refers to the position of the first number in the
        # salary `text`, e.g. '€42k - 75k' --> end=1
        # Search for the symbol at the beginning of the text
        regex = r"^(\D+)"
        match = re.search(regex, text)
        if match:
            self.print_log("DEBUG",
                           "Found currency {} in text {}".format(
                               match.group(), text))
            # Some currencies have white spaces at the end,
            # e.g. 'SGD 60k - 79k'. Thus, the strip()
            return match.group().strip(), match.end()
        else:
            raise js_e.NoCurrencySymbolError("No currency symbol could be "
                                             "retrieved from the salary text "
                                             "{}".format(text))

    # Get the location data in a linked data JSON object
    def get_loc_in_ld(self, linked_data):
        job_locations = linked_data.get('jobLocation')
        if job_locations:
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
                processed_locations.append({'city': city,
                                            'region': region,
                                            'country': country})
            return processed_locations
        else:
            # TODO: no need to raise error, just return None
            # raise js_e.NoJobLocationError("No job locations found in the linked data.")
            self.print_log("ERROR", "No job locations found in the linked data.")
            return None

    @staticmethod
    def get_min_max_salary(salary_range):
        min_salary, max_salary = salary_range.replace(" ", "").split("-")
        min_salary = int(min_salary)
        max_salary = int(max_salary)
        return min_salary, max_salary

    def get_webpage(self, url):
        html = None
        current_delay = time.time() - self.last_request_time
        diff_between_delays = current_delay - DELAY_BETWEEN_REQUESTS
        if diff_between_delays < 0:
            self.print_log("INFO",
                           "Waiting {} seconds before sending "
                           "next HTTP request...".format(abs(diff_between_delays)))
            time.sleep(abs(diff_between_delays))
            self.print_log("INFO", "Time is up! HTTP request will be sent.")
        try:
            req = self.req_session.get(url, headers=self.headers,
                                       timeout=HTTP_GET_TIMEOUT)
            html = req.text
        except OSError as e:
            raise OSError(e)
        else:
            if req.status_code == 404:
                raise js_e.PageNotFoundError("404 - PAGE NOT FOUND. "
                                             "The URL {} returned a 404 "
                                             "status code.".format(url))
        self.last_request_time = time.time()
        self.print_log("INFO", "The webpage is retrieved from {}".format(url))
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
        html = None
        # File path where the webpage (only HTML) will be saved
        cached_webpage_filepath = os.path.join(CACHED_WEBPAGES_DIRPATH,
                                               "{}.html".format(
                                                   self.session.job_id))
        webpage_accessed = None
        url = self.session.url

        # First try to load the HTML page from cache
        if CACHED_WEBPAGES_DIRPATH:
            try:
                html = g_util.read_file(cached_webpage_filepath)
            except OSError as e:
                self.print_log("ERROR", exception=e)
            else:
                self.print_log("INFO",
                               "The cached webpage HTML is loaded from {}".format(
                                   cached_webpage_filepath))
                # Get the webpage's datetime modified as the datetime the
                # webpage was originally accessed
                self.session.data.set_job_post(
                    cached_webpage_filepath=cached_webpage_filepath,
                    webpage_accessed=os.path.getmtime(cached_webpage_filepath))
                return html
        else:
            self.print_log("INFO", "The caching option is disabled")

        self.print_log("INFO",
                       "The webpage HTML @ {} will be retrieved with an HTTP "
                       "request".format(url))

        # Secondly, try to get the webpage HTML with an HTTP request
        try:
            html = self.get_webpage(url)
        except (OSError, js_e.HTTP404Error) as e:
            raise js_e.WebPageNotFoundError(e)
        else:
            # Get the datetime the webpage was retrieved (though not 100% accurate)
            webpage_accessed = time.time()
            try:
                self.save_webpage_locally(url, cached_webpage_filepath, html)
            except (OSError, js_e.WebPageSavingError) as e:
                self.print_log("ERROR", exception=e)
                self.print_log("ERROR",
                               "The webpage @ URL {} will not be saved "
                               "locally".format(url))
                cached_webpage_filepath = None
            else:
                self.print_log("INFO",
                               "The webpage was saved in {}. URL is {}".format(
                                   cached_webpage_filepath, url))
            finally:
                self.session.data.set_job_post(
                    cached_webpage_filepath=cached_webpage_filepath,
                    webpage_accessed=webpage_accessed)
                return html

    def print_log(self, level, msg=None, exception=None, length_msg=300):
        # See https://stackoverflow.com/a/900413
        caller_function_name = sys._getframe(1).f_code.co_name
        # caller_function_name = "test"
        if exception:
            assert exception.__class__.__base__ is Exception, \
                "{} is not a subclass of Exception".format(exception)
            # TODO: catch AttributeError if __str__() and __class__ not present
            # The log message template is "exception_name: exception_msg"
            msg = "{}: {}".format(exception.__class__.__name__,
                                  exception.__str__())
        if len(msg) > length_msg:
            msg = msg[:length_msg] + " [...]"
        if level not in ["DEBUG", "INFO"]:
            if self.session is None:
                print("[{}] [{}] {}".format(level, caller_function_name, msg))
            else:
                print("[{}] [{}] [{}] {}".format(level, self.session.job_id,
                                                 caller_function_name, msg))

    def save_webpage_locally(self, url, filepath, html):
        if CACHED_WEBPAGES_DIRPATH:
            try:
                g_util.write_file(filepath, html)
            except OSError as e:
                raise OSError(e)
            else:
                pass
        else:
            error_msg = "The caching option is disabled. Thus, the webpage @ " \
                        "URL {} will not be saved locally.".format(url)
            raise js_e.WebPageSavingError(error_msg)

    def select_entries(self):
        """
        Returns all job_id, author and url from the `entries` table

        :return:
        """
        sql = '''SELECT job_id, author, url FROM entries'''
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def standardize_country(self, country):
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
            self.print_log("DEBUG", invalid_country_log_msg.format(country, 'GB'))
            return 'GB'
        elif country == 'Deutschland':
            # 'Deutschland' (German for Germany) is not recognized by
            # `pycountry_convert` as a valid country. 'Germany' associated with
            # the 'DE' alpha2 code are used instead
            self.print_log("DEBUG", invalid_country_log_msg.format(country, 'DE'))
            return 'DE'
        elif country == 'Österreich':
            # 'Österreich' (German for Austria) is not recognized by
            # `pycountry_convert` as a valid country. 'Austria' associated with
            # the 'AT' alpha2 code are used instead
            self.print_log("DEBUG", invalid_country_log_msg.format(country, 'AT'))
            return 'AT'
        elif country == 'Vereinigtes Königreich':
            # 'Vereinigtes Königreich' (German for United Kingdom) is not
            # recognized by `pycountry_convert` as a valid country.
            # 'United Kingdom' associated with the 'GB' alpha2 code are used
            # instead
            self.print_log("DEBUG", invalid_country_log_msg.format(country, 'GB'))
            return 'GB'
        elif country == 'Schweiz':
            # 'Schweiz' (German for Switzerland) is not recognized
            # by `pycountry_convert` as a valid country. 'Switzerland' associated
            # with the 'CH' alpha2 code are used instead
            self.print_log("DEBUG", invalid_country_log_msg.format(country, 'GB'))
            return 'CH'
        try:
            alpha2 = country_name_to_country_alpha2(country)
        except KeyError as e:
            raise KeyError(e)
        else:
            self.print_log("DEBUG",
                           "The country '{}' will be updated to the "
                           "standard name '{}'.".format(country, alpha2))
            return alpha2

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


def main():
    global CACHED_WEBPAGES_DIRPATH
    if g_util.create_directory_prompt(CACHED_WEBPAGES_DIRPATH) == 1:
        print("[WARNING] The caching option for saving webpages will be disabled")
        CACHED_WEBPAGES_DIRPATH = None

    # Start the scraping of job posts
    try:
        JobsScraper().start_scraping()
    except (AssertionError, sqlite3.OperationalError, sqlite3.Error,
            js_e.EmptyQueryResultSetError) as e:
        print(e)
        return 1


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt as e:
        pass
