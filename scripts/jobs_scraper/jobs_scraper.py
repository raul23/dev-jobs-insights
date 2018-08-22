import json
import os
import re
import sqlite3
import sys
import time

from bs4 import BeautifulSoup
from forex_python.converter import get_currency_name, get_rate, RatesNotAvailableError
import ipdb
from pycountry_convert import country_name_to_country_alpha2
import requests

# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
import js_exceptions as js_e
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
        self.autocommit = autocommit
        # Create db connection
        self.conn = None
        # Establish a session to be used for the GET requests
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit 537.36 (KHTML, like Gecko) Chrome",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"}
        self.scraped_job_posts = {}
        self.last_request_time = -sys.float_info.max
        self.job_data_keys = ['title', 'url', 'job_post_notice', 'job_post_description', 'employment_type',
                              'date_posted', 'valid_through', 'experience_level', 'industry', 'skills', 'job_benefits',
                              'company_description', 'company_name', 'company_site_url', 'company_size', 'company_type',
                              'equity', 'min_salary', 'max_salary', 'currency', 'job_locations',
                              'min_salary_'+DEST_CURRENCY, 'max_salary_'+DEST_CURRENCY, 'currency_conversion_time',
                              'office_location', 'high_response_rate', 'role', 'remote', 'relocation', 'visa',
                              'webpage_accessed', 'cached_webpage_path']
        # Current `job_id` being processed
        self.job_id = None
        # `currency_data` is a list of dicts. Each item in `currency_data` is a
        # dict with the keys ['cc', 'symbol', 'name'] where 'cc' is short for currency code
        self.currency_data = g_util.load_json(CURRENCY_FILEPATH)
        # Load the dict of US states where the keys are the USPS 2-letter codes
        # for the U.S. state and the values are the names
        # e.g. 'AZ': 'Arizona'
        self.us_states = g_util.load_json(US_STATES_FILEPATH)
        # Reverse the dict of U.S. states to search based on the full name
        # instead of the 2-letter codes
        self.reversed_us_states = self.us_states.__class__(map(reversed, self.us_states.items()))
        # Cache the rates that were already used for converting one currency to another
        # Hence, we won't have to send HTTP requests to get these rates if they
        # are already cached
        # `cached_rates` has for keys the name of the rates and the values are
        # the associated rates.
        # The name of the rate is built like this: {base_cur}_{dest_cur}
        # e.g. {'EUR_USD': 1.1391, 'EUR_CAD': 1.4976}
        self.cached_rates = {}

    def init_session(self, job_id):
        self.job_id = job_id

    def reset_session(self):
        self.job_id = None

    def start_scraping(self):
        self.conn = g_util.connect_db(DB_FILEPATH)
        with self.conn:
            # Get all the entries' URLs
            try:
                rows = self.select_entries()
            except sqlite3.OperationalError as e:
                print("[ERROR] sqlite3.OperationalError: {}".format(e))
                self.print_log("ERROR", "sqlite3.OperationalError: {}".format(e))
                self.print_log("WARNING", "Web scraping will end!")
                return
            else:
                if not rows:
                    self.print_log("ERROR",
                                   "The returned SQL result set is empty. Web craping will end!")
                    return

        # For each entry's URL, scrape more job data from the job post's webpage
        count = 1
        at_least_one_succeeded = False
        n_skipped = 0
        self.print_log("INFO", "Total URLs to process = {}".format(len(rows)))
        #debug1, debug2 = True, False  # only one job_id
        debug1, debug2 = False, True
        for job_id, author, url in rows:

            # TODO: debug code
            if debug1 and job_id != 198685:
                continue

            if debug2 and count < 401:
                count += 1
                continue

            if debug2 and count > 601:
                break

            try:
                print()
                self.print_log("WARNING", "#{} Processing {}".format(count, url))
                count += 1

                self.init_session(job_id)
                self.print_log("INFO", "Session initialized")

                # Initialize the dict that will store scraped data from the given job post
                # and update the job post's URL
                self.init_dict({'url': url})

                # Load cached webpage or retrieve it online
                html = self.load_cached_webpage()
                if not html:
                    self.print_log("WARNING", "The current URL {} will be skipped.".format(url))
                    continue

                bsObj = BeautifulSoup(html, 'lxml')

                # Before extracting any job data from the job post, check if the job is
                # accepting applications by extracting the message
                # "This job is no longer accepting applications."
                # This notice is located in
                # body > div.container > div#content > aside.s-notice
                # NOTE: Usually when this notice is present in a job post, the json job
                # data is not found anymore within the html of the job post
                self.process_notice(bsObj)

                # Get linked data from <script type="application/ld+json">
                self.process_linked_data(bsObj)

                # Get job data (e.g. salary, remote, location) from the <header>
                self.process_header(bsObj)

                # Get job data from the Overview section
                self.process_overview_items(bsObj)

                at_least_one_succeeded = True
                self.print_log("INFO", "Finished Processing {}".format(url))
            except KeyError as e:
                self.print_log("ERROR", "KeyError: {}".format(e.__str__()))
                self.print_log("WARNING", "The current URL {} will be skipped".format(url))
                n_skipped += 1
            finally:
                self.print_log("INFO", "Session ending")
                self.reset_session()

        ipdb.set_trace()

        print()
        # Save scraped data into json file
        # ref.: https://stackoverflow.com/a/31343739 (presence of unicode strings,
        # e.g. EURO currency symbol)
        if at_least_one_succeeded and g_util.dump_json_with_codecs(SCRAPED_JOB_DATA_FILEPATH, self.scraped_job_posts) == 0:
            self.print_log("INFO", "Scraped data saved in {}".format(SCRAPED_JOB_DATA_FILEPATH))
        else:
            self.print_log("ERROR", "Scraped data couldn't be saved")
            self.print_log("INFO", "Skipped URLs={}/{}".format(n_skipped, len(rows)))

    def init_dict(self, updated_values):
        self.scraped_job_posts[self.job_id] = {}.fromkeys(self.job_data_keys)
        # Add updated values
        self.update_dict(updated_values)

    def get_dict_value(self, key):
        return self.scraped_job_posts[self.job_id].get(key)

    def get_country_from_dict(self):
        return self.scraped_job_posts.get(self.job_id).get('office_location', {}).get('country')

    def update_dict(self, updated_values):
        for key, new_value in updated_values.items():
            log_msg = "Trying to update the [key, value]=[{}, {}]".format(key, new_value)
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
                    self.print_log("CRITICAL", "The key={} is not a valid job data key.".format(key))
            else:
                log_msg = "The key='{}' already has a value='{}'. Thus the " \
                          "new_value='{}' will be ignored.".format(key, current_value, new_value)
                self.print_log("DEBUG", log_msg)
                if current_value != new_value:
                    log_msg = "The new_value='{}' is not equal to current_value='{}'".format(new_value, current_value)
                    self.print_log("CRITICAL", log_msg)

    @staticmethod
    def process_company_size(company_size):
        # Example: '1k-5k people' --> '1000-5000'
        # Replace the letter 'k' with '000'
        company_size = company_size.replace('k', '000')
        # Remove 'people' and remove any whitespace around the string
        company_size = company_size.split('people')[0].strip()
        return company_size

    @staticmethod
    def process_employment_type(employment_type):
        # Standardize the employment type by modifying to all caps and
        # replacing hyphens with underscores
        # e.g. Full-time --> FULL_TIME
        return employment_type.upper().replace('-', '_')

    def process_header(self, bsObj):
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
        url = self.get_dict_value('url')

        # 1. Get title of job post
        pattern = "header.job-details--header > div.grid--cell > h1.fs-headline1 > a"
        self.process_text_in_tag(bsObj, pattern, key_name='title')

        # 2. Get company name
        pattern = "header.job-details--header > div.grid--cell > div.fc-black-700 > a"
        self.process_text_in_tag(bsObj, pattern, key_name='company_name')

        # 3. Get the office location which is located on the same line as the company name
        pattern = "header.job-details--header > div.grid--cell > div.fc-black-700 > span.fc-black-500"
        self.process_text_in_tag(bsObj, pattern, key_name='office_location',
                                 process_text_method=self.process_location_text
                                 )

        # 4. Get the other job data on the next line after the company name and location
        # TODO: simplify the scraping of these other job data
        pattern = "header.job-details--header > div.grid--cell > div.mt12"
        div_tag = bsObj.select_one(pattern)
        if div_tag:
            # Each `div_tag`'s child is associated to a job item (e.g. salary, remote)
            # and is a <span> tag with a class that starts with '-'
            # Example: header.job-details--header > div.grid--cell > .mt12 > span.-salary.pr16
            children = div_tag.findChildren()
            for child in children:
                # Each job data text is found within <span> with a class that starts
                # with '-', e.g. <span class='-salary pr16'>
                # NOTE: we need the child element's class that starts with '-'
                # because we will then know how to name the extracted job data item
                child_class = [tag_class for tag_class in child.attrs['class'] if tag_class.startswith('-')]
                if child_class: # class that starts with '-'
                    # Get the <div>'s class name without the '-' at the beginning,
                    # this will correspond to the type of job data (e.g. salary,
                    # remote, relocation, visa)
                    key_name = child_class[0][1:]
                    value = child.text
                    if value:  # value = text
                        self.print_log("INFO", "The {} is found. URL @ {}".format(key_name, url))
                        # Get the text (e.g. $71k - 85k) by removing any \r and
                        # \n around the string
                        value = value.strip()
                        if key_name == 'salary':
                            updated_values = self.process_salary_text(value)
                            if updated_values:
                                log_msg = "Updating dict with salary values " \
                                          "(min_salary, max_salary, ...)"
                                self.print_log("DEBUG", log_msg)
                                self.update_dict(updated_values)
                            else:
                                self.print_log("DEBUG", "Salary will be ignored")
                        else:
                            self.print_log("DEBUG", "Updating dict with {{{}:{}}})".format(key_name, value))
                            self.update_dict({key_name: value})
                    else:
                        log_msg = "No text found for the job data key {}. " \
                                  "URL @ {}".format(key_name, url)
                        self.print_log("ERROR", log_msg)
                else:
                    self.print_log("ERROR", "The <span>'s class doesn't start with '-'. URL @ {}".format(url))
        else:
            log_msg = "Couldn't extract the other job data @ URL {}. The other " \
                      "job data should be found in {}".format(url, pattern)
            self.print_log("INFO", log_msg)

    def process_linked_data(self, bsObj):
        # Get linked data from <script type="application/ld+json">:
        # On the webpage of a job post, important data about the job post
        # (e.g. job location or salary) can be found in <script type="application/ld+json">
        # This linked data is a JSON object that stores important job info like
        # employmentType, experienceRequirements, jobLocation
        script_tag = bsObj.find(attrs={'type': 'application/ld+json'})
        url = self.get_dict_value('url')
        if script_tag:
            """
            The linked data found in <script type="application/ld+json"> is a json
            object with the following keys:
            '@context', '@type', 'title', 'skills', 'description', 'datePosted',
            'validThrough', 'employmentType', 'experienceRequirements',
            'industry', 'jobBenefits', 'hiringOrganization', 'baseSalary', 'jobLocation'
            """
            linked_data = json.loads(script_tag.get_text())
            min_salary = linked_data.get('baseSalary', {}).get('value', {}).get('minValue')
            max_salary = linked_data.get('baseSalary', {}).get('value', {}).get('maxValue')
            currency = linked_data.get('baseSalary', {}).get('currency')
            experience_level = linked_data.get('experienceRequirements')
            if experience_level is not None:
                experience_level = self.str_to_list(experience_level)
            else:
                self.print_log("DEBUG", "Experience level is None in linked data")
            updated_values = {'title': linked_data.get('title'),
                              'job_post_description': linked_data.get('description'),
                              'employment_type': linked_data.get('employmentType'),
                              'date_posted': linked_data.get('datePosted'),
                              'valid_through': linked_data.get('validThrough'),
                              'experience_level': experience_level,
                              'industry': linked_data.get('industry'),
                              'skills': linked_data.get('skills'),
                              'job_benefits': linked_data.get('jobBenefits'),
                              'company_description': linked_data.get('hiringOrganization', {}).get('description'),
                              'company_name': linked_data.get('hiringOrganization', {}).get('name'),
                              'company_site_url': linked_data.get('hiringOrganization', {}).get('sameAs'),
                              'min_salary': min_salary,
                              'max_salary': max_salary,
                              'currency': currency,
                              'job_locations': self.get_loc_in_ld(linked_data)
                              }
            # Convert the minimum and maximum salaries to DEST_CURRENCY (e.g. USD)
            converted_salaries = {'min_salary_' + DEST_CURRENCY: None,
                                  'max_salary_' + DEST_CURRENCY: None,
                                  'currency_conversion_time': None
                                  }
            try:
                results = self.convert_min_and_max_salaries(min_salary, max_salary, currency)
            except js_e.CurrencyRateError as e:
                self.print_log("ERROR", exception=e)
            except js_e.SameCurrencyError as e:
                self.print_log("DEBUG", exception=e)
            except js_e.NoneBaseCurrencyError as e:
                self.print_log("DEBUG", exception=e)
            else:
                converted_salaries.update(results)
                updated_values.update(converted_salaries)
            self.update_dict(updated_values)
            self.print_log("INFO", "The linked data from URL {} were successfully scraped".format(url))
        else:
            # Reasons for not finding <script type='application/ld+json'>:
            # maybe the page is not found anymore (e.g. job post removed) or
            # the company is not longer accepting applications
            log_msg = "The page @ URL {} doesn't contain any SCRIPT tag with " \
                      "type='application/ld+json'".format(url)
            self.print_log("INFO", log_msg)

    def process_location_text(self, text):
        updated_values = {}
        std_country = None
        # The text where you find the location looks like this:
        # '\n|\r\nNo office location                    '
        # strip() removes the first '\n' and the right spaces. Then split('\n')[-1]
        # extracts the location string. And the replace() will remove any spaces after
        # the commas.
        # e.g. 'Toronto, ON, Canada' --> 'Toronto,ON,Canada'
        text = text.strip().split('|')[-1].strip().replace(', ', ',')
        if text.count(',') == 0:
            log_msg = "No commas found in location text '{}'. We will assume " \
                      "that the location text '{}' refers to a country.".format(text, text)
            self.print_log("WARNING", log_msg)
            updated_values['country'] = text
        elif text.count(',') == 1:
            # One comma in location text
            # Example 1: 'Bellevue, WA'
            # Example 2: 'Helsinki, Finland'
            self.print_log("DEBUG", "Found 1 comma in the location text '{}'".format(text))
            updated_values = dict(zip(['city', 'country'], text.split(',')))
            # First check if the extracted country refers to a US state or a country
            name = updated_values['country']
            if self.is_a_us_state(name):
                self.print_log("DEBUG", "The location text '{}' refers to a place in the US".format(text))
                # Fix the location information: the country is wrong and the
                # region is missing
                updated_values['region'] = name
                updated_values['country'] = 'US'
                # NOTE: No need to standardize the country name (like we do in
                # the else block) because it is already standard
                return updated_values
        elif text.count(',') == 2:
            # Two commas in location text
            # e.g. Toronto, ON, Canada
            self.print_log("DEBUG", "Found 2 commas in the location text '{}'".format(text))
            updated_values = dict(zip(['city', 'region', 'country'], text.split(',')))
        else:
            # Incorrect number of commas in location text
            self.print_log("ERROR", "Invalid location text '{}'. Incorrect number of commas.".format(text))
            return None
        # Standardize the country, e.g. Finland -> FI
        std_country = self.standardize_country(updated_values['country'])
        if std_country is not None:
            updated_values['country'] = std_country
        return updated_values

    def process_notice(self, bsObj):
        pattern = "body > div.container > div#content > aside.s-notice"
        self.process_text_in_tag(pattern=pattern, key_name='job_post_notice', bsObj=bsObj)

    def process_overview_items(self, bsObj):
        # Get job data from the Overview section. There are three places within
        # the Overview section that will be extracted for more job data:
        # 1. in the "High response rate" sub-section of Overview
        # 2. in the "About this job" sub-section of Overview
        # 3. in the "Technologies" sub-section of Overview
        # NOTE: these sub-sections are located within <div id=""overview-items>
        # [overview-items]
        url = self.get_dict_value('url')
        convert_keys = {'job_type': 'employment_type',
                        'technologies': 'skills'}

        # [high_response_rate]
        # 1. The high response rate might not be present (it isn't often we get
        # to see this notice)
        self.process_text_in_tag(bsObj, pattern=".-high-response > .-text > .-title", key_name='high_response_rate')

        # [about_this_job]
        # 2. Get more job data (e.g. role, industry, company size) in the
        # "About this job" section. Each item is located in
        # "#overview-items > .mb32 > .job-details--about > .grid--cell6 > .mb8"
        # NOTE: these job data are presented in two columns, with three items per column
        pattern = "#overview-items > .mb32 > .job-details--about > .grid--cell6 > .mb8"
        div_tags = bsObj.select(pattern)
        if div_tags:
            # Each `div_tag` corresponds to a job data item
            # e.g. Job type: Full-time, Company type: Private
            for div_tag in div_tags:
                # Sample raw text: '\nJob type: \nContract\n'
                temp = div_tag.text.strip().split(":")
                key_name, value = temp[0].strip(), temp[1].strip()
                # The field names should all be lowercase and spaces be replaced
                # with underscores e.g. Job type ---> job_type
                key_name = key_name.replace(" ", "_").lower()
                # Convert the key name to use the standard key name
                key_name = convert_keys.get(key_name, key_name)
                # Comma-separated values should be converted to a list
                # These comma-separated values are: experience_level, role, industry
                # e.g. Mid-Level, Senior, Lead  --> [Mid-Level, Senior, Lead]
                if key_name in ['experience_level', 'role', 'industry']:
                    self.print_log("DEBUG", "The value {} will be converted to a list".format(value))
                    value = self.str_to_list(value)
                elif key_name == 'company_size':
                    # '1k-5k people' --> '1000-5000'
                    new_value = self.process_company_size(value)
                    log_msg = "The company size '{}' was processed to '{}'".format(value, new_value)
                    self.print_log("DEBUG", log_msg)
                    value = new_value
                elif key_name == 'employment_type':
                    new_value = self.process_employment_type(value)
                    log_msg = "The employment type '{}' was processed to '{}'".format(value, new_value)
                    self.print_log("DEBUG", log_msg)
                    value = new_value
                self.update_dict({key_name: value})
        else:
            log_msg = "Couldn't extract job data from the 'About this job' section @ the URL {}. " \
                      "The job data should be found in {}".format(url, pattern)
            self.print_log("ERROR", log_msg)

        # [technologies]
        # 3. Get the list of technologies, e.g. ruby, python, html5
        # NOTE: unlike the other job data in "overview_items", the technologies
        # are given as a list
        pattern = "#overview-items > .mb32 > div > a.job-link"
        link_tags = bsObj.select(pattern)
        skills = []
        if link_tags:
            for link_tag in link_tags:
                technology = link_tag.text
                if technology:
                    self.print_log("DEBUG", "Skill {} extracted".format(technology))
                    skills.append(technology)
                else:
                    log_msg = "[ERROR] No text found for the technology with " \
                              "href={}. URL @ {}".format(link_tag["href"], url)
                    self.print_log("ERROR", log_msg)
            if skills:
                log_msg = "These skills {} were successfully extracted from the " \
                          "Technologies section".format(skills)
                self.print_log("DEBUG", log_msg)
                self.update_dict({'skills': skills})
            else:
                log_msg = "No skills extracted from the Technologies section"
                self.print_log("DEBUG", log_msg)
        else:
            log_msg = "[ERROR] Couldn't extract technologies from the Technologies " \
                      "section @ the URL {}. The technologies should be found in {}".format(url, pattern)
            self.print_log("ERROR", log_msg)

    def process_salary_range(self, salary_range):
        # Dict that will be returned if everything goes right. If not, then
        # `None` will be returned
        updated_values = {'min_salary': None,
                          'max_salary': None,
                          'currency': None,
                          'min_salary_' + DEST_CURRENCY: None,
                          'max_salary_' + DEST_CURRENCY: None,
                          'currency_conversion_time': None
                          }
        # Extract the currency symbol at the beginning of the salary range text
        # e.g. '€' will be extracted from €42k - 75k'
        # `results` is either:
        #       - a tuple (currency_symbol, end) or
        #       - None (if no currency symbol could be extracted)
        #
        # NOTE: `end` refers to the position of the first number in the
        # `salary_range` text, e.g. end=1 if salary_range='€42k - 75k'
        results = self.get_currency_symbol(salary_range)
        if results:
            # `results` is a tuple
            currency_symbol, end = results
            # Get the currency code based on the currency symbol
            currency_code = self.get_currency_code(currency_symbol)
            # Get the salary range only without the currency symbol at the beginning
            # e.g. '€42k - 75k' --> '42k - 75k'
            salary_range = salary_range[end:]
            # Replace the letter 'k' with '000', e.g. 42k --> 42000
            salary_range = salary_range.replace('k', '000')
            # Get the minimum and maximum salary separately
            # e.g. '42000 - 75000' --> min_salary=42000, max_salary=75000
            min_salary, max_salary = self.get_min_max_salary(salary_range)
            updated_values.update({'currency': currency_code,
                                   'min_salary': min_salary,
                                   'max_salary': max_salary
                                   })
            # Before converting the min and max salaries, check if they were
            # PREVIOUSLY computed from the linked data. We want to avoid making
            # wasteful computations when performing the currency conversions.
            prev_min_salary = self.get_dict_value('min_salary_'+DEST_CURRENCY)
            prev_max_salary = self.get_dict_value('max_salary_'+DEST_CURRENCY)
            if prev_min_salary is not None and prev_max_salary is not None:
                error_msg = "The min and max salaries ({}-{}) were previously " \
                            "computed from the linked data".format(min_salary, max_salary)
                raise js_e.SameComputationError(error_msg)
            # Convert the min and max salaries to DEST_CURRENCY (e.g. USD)
            try:
                results = self.convert_min_and_max_salaries(min_salary, max_salary, currency_code)
            except js_e.CurrencyRateError as e:
                raise js_e.CurrencyRateError(e)
            except js_e.SameCurrencyError:
                return updated_values
            else:
                updated_values.update(results)
                return updated_values
        else:
            error_msg = "NoCurrencySymbolError: No currency symbol could be retrieved from the " \
                        "salary text {}".format(salary_range)
            raise js_e.NoCurrencySymbolError(error_msg)

    def process_salary_text(self, salary_text):
        updated_values = {}
        # Check if the salary text contains 'Equity', e.g. '€42k - 75k | Equity'
        if 'Equity' in salary_text:
            self.print_log("DEBUG", "Equity found in the salary text {}".format(salary_text))
            # Split the salary text to get the `salary_range` and `equity`
            # e.g. '€42k - 75k | Equity' will be splitted as '€42k - 75k' and 'Equity'
            # IMPORTANT: the salary text can consist of 'Equity' only. In that
            # case `salary` must be set to None to avoid processing the salary
            # text any further.
            if '|' in salary_text:
                salary_range, equity = [v.strip() for v in salary_text.split('|')]
            else:
                self.print_log("DEBUG", "No salary found, only equity in '{}'".format(salary_text))
                salary_range = None
                equity = salary_text.strip()
            updated_values['equity'] = equity
        else:
            self.print_log("DEBUG", "Equity is not found in the salary text {}".format(salary_text))
            salary_range = salary_text
        if salary_range:
            try:
                results = self.process_salary_range(salary_range)
            except js_e.SameComputationError as e:
                self.print_log("DEBUG", exception=e)
                return None
            except js_e.NoCurrencySymbolError as e:
                self.print_log("ERROR", "NoCurrencySymbolError: {}".format(e))
                return None
            except js_e.CurrencyRateError as e:
                self.print_log("ERROR", "CurrencyRateError: {}".format(e))
                return None
            else:
                self.print_log("DEBUG", "The salary text {} was successfully processed!")
                updated_values.update(results)
        return updated_values

    def process_text_in_tag(self, bsObj, pattern, key_name, process_text_method=None):
        url = self.get_dict_value('url')
        tag = bsObj.select_one(pattern)
        if tag:
            value = tag.text
            if value:
                self.print_log("INFO", "The {} is found. URL @ {}".format(key_name, url))
                # Process the text with the specified method
                # For example, in the case of a location text, we want to
                # standardize the country (e.g. Finland --> FI)
                if process_text_method:
                    value = process_text_method(value)
                self.update_dict({key_name: value})
            else:
                self.print_log("WARNING", "The {} is empty. URL @ {}".format(key_name, url))
        else:
            log_msg = "Couldn't extract the {} @ the URL {}. The {} should be " \
                      "found in {}".format(key_name, url, key_name, pattern)
            self.print_log("DEBUG", log_msg)

    def convert_min_and_max_salaries(self, min_salary, max_salary, current_currency):
        # Convert the min and max salaries to DEST_CURRENCY (e.g. USD)
        updated_values = {}
        if current_currency != DEST_CURRENCY:
            log_msg = "The min and max salaries [{}-{}] will be converted from " \
                      "{} to {}".format(min_salary, max_salary, current_currency, DEST_CURRENCY)
            self.print_log("DEBUG", log_msg)
            try:
                min_salary_converted, timestamp = self.convert_currency(min_salary, current_currency, DEST_CURRENCY)
                max_salary_converted, _ = self.convert_currency(max_salary, current_currency, DEST_CURRENCY)
            except (RatesNotAvailableError, requests.exceptions.ConnectionError) as e:
                raise js_e.CurrencyRateError(e.__str__())
            except js_e.NoneBaseCurrencyError as e:
                raise js_e.NoneBaseCurrencyError(e.__str__())
            else:
                updated_values.update({'min_salary_' + DEST_CURRENCY: min_salary_converted,
                                       'max_salary_' + DEST_CURRENCY: max_salary_converted,
                                       'currency_conversion_time': timestamp
                                       })
                return updated_values
        else:
            error_msg = "The min and max salaries [{}-{}] are already in the " \
                      "desired currency {}".format(min_salary, max_salary, DEST_CURRENCY)
            raise js_e.SameCurrencyError(error_msg)

    def convert_currency(self, amount, base_cur_code, dest_cur_code='USD'):
        if base_cur_code is None:
            raise js_e.NoneBaseCurrencyError("The base currency code is None")
        converted_amount = None
        try:
            # Get the rate from cache
            rate_used = self.cached_rates.get('{}_{}'.format(base_cur_code, dest_cur_code))
            if rate_used:
                log_msg = "The cached rate {} is used for " \
                          "{}-->{}".format(rate_used, base_cur_code, dest_cur_code)
                self.print_log("DEBUG", log_msg)
            else:
                log_msg = "No cache rate found for " \
                          "{}-->{}".format(base_cur_code, dest_cur_code)
                self.print_log("DEBUG", log_msg)
                # Get the rate and cache it
                rate_used = get_rate(base_cur_code, dest_cur_code)
                log_msg = "The rate {} is cached for " \
                          "{}-->{}".format(rate_used, base_cur_code, dest_cur_code)
                self.print_log("DEBUG", log_msg)
                self.cached_rates['{}_{}'.format(base_cur_code, dest_cur_code)] = rate_used
            # Convert the base currency to the desired currency with the retrieved rate
            converted_amount = rate_used * amount
        except RatesNotAvailableError as e:
            log_msg = "The amount {} in {} couldn't be converted " \
                      "to {}".format(amount, base_cur_code, dest_cur_code)
            self.print_log("ERROR", log_msg)
            error_msg = "RatesNotAvailableError: {}".format(e.__str__())
            raise RatesNotAvailableError(error_msg)
        except requests.exceptions.ConnectionError as e:
            self.print_log("ERROR", "No connection to api.fixer.io (e.g. working offline)")
            error_msg = "requests.exceptions.ConnectionError: {}".format(e.__str__())
            raise requests.exceptions.ConnectionError(error_msg)
        else:
            converted_amount = int(round(converted_amount))
            # NOTE: round(a, 2) doesn't work in python 2.7:
            # >> a = 0.3333333
            # >> round(a, 2),
            # Use the following in python2.7:
            # >> float(format(a, '.2f'))
            return converted_amount, time.time()

    def get_currency_code(self, currency_symbol):
        # First check if the currency symbol is not a currency code already
        if get_currency_name(currency_symbol):
            self.print_log("DEBUG", "The currency symbol '{}' is actually a currency code.")
            return currency_symbol
        # NOTE: there is no 1-to-1 mapping when going from currency symbol
        # to currency code
        # e.g. the currency symbol £ is used for the currency codes EGP, FKP, GDP,
        # GIP, LBP, and SHP
        # Search into the `currency_data` list for all the currencies that have
        # the given `currency_symbol`. Each item in `currency_data` is a dict
        # with the keys ['cc', 'symbol', 'name'].
        results = [item for item in self.currency_data if item["symbol"] == currency_symbol]
        # NOTE: C$ is used as a currency symbol for Canadian Dollar instead of $
        # However, C$ is already the official currency symbol for Nicaragua Cordoba (NIO)
        # Thus we will assume that C$ is related to the Canadian Dollar.
        # NOTE: in stackoverflow job posts, $ alone refers to US$ but $ can refer to multiple
        # currency codes such as ARS (Argentine peso), AUD, CAD. Thus, we will make an
        # assumption that '$' alone will refer to US$ since if it is in AUD or CAD, the
        # currency symbols 'A$' and 'C$' are usually used in job posts, respectively.
        if currency_symbol != "C$" and len(results) == 1:
            log_msg = "Found only one currency code {} associated with the " \
                      "given currency symbol {}".format(self.job_id, results[0]["cc"], currency_symbol)
            self.print_log("DEBUG", log_msg)
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
                # There are two possibilities: Russian Ruble (RUB) or South African rand (ZAR)
                # Check the job post's country to determine which of the two
                # currency codes to choose from
                country = self.get_country_from_dict()
                if country is None:
                    log_msg = "Could not get a currency code from '{}'".format(currency_symbol)
                    self.print_log("ERROR", log_msg)
                    return None
                elif country == 'ZA':
                    currency_code = 'ZAR'
                else:
                    currency_code = "RUB"
            else:
                log_msg = "Could not get a currency code from '{}'".format(currency_symbol)
                self.print_log("ERROR", log_msg)
                return None
            return currency_code

    # Get currency symbol located at the BEGINNING of the string, e.g. '€42k - 75k'
    def get_currency_symbol(self, text):
        # returned value is either:
        #   - a tuple (currency_symbol, end) or
        #   - None
        # NOTE: `end` refers to the position of the first number in the salary `text`
        regex = r"^(\D+)"
        match = re.search(regex, text)
        if match:
            self.print_log("DEBUG", "Found currency {} in text {}".format(match.group(), text))
            # Some currencies have spaces at the end, e.g. 'SGD 60k - 79k'. Thus, the strip()
            return match.group().strip(), match.end()
        else:
            self.print_log("ERROR", "No currency found in text {}".format(text))
            return None

    @staticmethod
    # Get the location data in a linked data JSON object
    def get_loc_in_ld(linked_data):
        job_locations = linked_data.get('jobLocation')
        if job_locations:
            processed_locations = []
            for location in job_locations:
                processed_locations.append({'city': location.get('address', {}).get('addressLocality'),
                                            'region': location.get('address', {}).get('addressRegion'),
                                            'country': location.get('address', {}).get('addressCountry')})
            return processed_locations
        else:
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
                           "Waiting {} seconds before sending next HTTP request...".format(abs(diff_between_delays)))
            time.sleep(abs(diff_between_delays))
            self.print_log("INFO", "Time is up! HTTP request will be sent.")
        try:
            req = self.session.get(url, headers=self.headers, timeout=HTTP_GET_TIMEOUT)
            html = req.text
        except OSError as e:
            # g_util.print_exception("OSError")
            self.print_log("ERROR", "OSError: {}".format(e))
            return None
        else:
            if req.status_code == 404:
                self.print_log("ERROR", "PAGE NOT FOUND. The URL {} returned a 404 status code.".format(url))
                return None
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

    # Load the cached webpage HTML if the webpage is found locally. If it isn't found
    # locally, then we will try to retrieve it with a GET request
    def load_cached_webpage(self):
        html = None
        url = self.get_dict_value('url')
        # Path where the cached webpage's HTML will be saved
        filepath = os.path.join(CACHED_WEBPAGES_DIRPATH, "{}.html".format(self.job_id))

        if CACHED_WEBPAGES_DIRPATH:
            html = g_util.read_file(filepath)
        else:
            self.print_log("INFO", "The caching option is disabled")
        if html:
            self.print_log("INFO", "The cached webpage HTML is loaded from {}".format(filepath))
            # Update cached webpage path and its datetime modified
            self.update_dict({'cached_webpage_path': filepath,
                              'webpage_accessed': os.path.getmtime(filepath)})
        else:
            self.print_log("INFO",
                           "Instead the webpage HTML @ {} will be retrieved with a GET request".format(url))
            # Get the webpage HTML
            html = self.get_webpage(url)
            if html:
                # Update the datetime the webpage was retrieved (though not 100% accurate)
                self.update_dict({'webpage_accessed': time.time()})
                if self.save_webpage_locally(url, filepath, html) == 0:
                    # Update the path the webpage is cached
                    self.update_dict({'cached_webpage_path': filepath})
            else:
                # No html retrieved at all
                return None

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
            if self.job_id is None:
                print("[{}] [{}] {}".format(level, caller_function_name, msg))
            else:
                print("[{}] [{}] [{}] {}".format(level, self.job_id, caller_function_name, msg))

    def save_webpage_locally(self, url, filepath, html):
        if CACHED_WEBPAGES_DIRPATH:
            if g_util.write_file(filepath, html) == 0:
                self.print_log("INFO", "The webpage is saved in {}. URL is {}".format(filepath, url))
                return 0
            else:
                self.print_log("INFO", "The webpage @ URL {} will not be saved locally".format(url))
                return 1
        else:
            msg = "The caching option is disabled. Thus, the webpage @ URL {} " \
                  "will not be saved locally.".format(url)
            self.print_log("INFO", msg)
            return 1

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
        # Do some preliminary pre-processing on `country` before calling
        # country_name_to_country_alpha2()
        invalid_country_log_msg = "The country '{}' is not a valid country. Instead, " \
                                  "'{}' will be used as the alpha2 code."
        if country == 'UK':
            # UK' is not recognized by `pycountry_convert` as a valid
            # country. 'United Kingdom' associated with the 'GB' alpha2 code are
            # used instead
            self.print_log("DEBUG", invalid_country_log_msg.format(country, 'GB'))
            return 'GB'
        elif country == 'Deutschland':
            # 'Deutschland' (German for Germany) is not recognized by `pycountry_convert`
            # as a valid country. 'Germany' associated with the 'DE' alpha2 code are
            # used instead
            self.print_log("DEBUG", invalid_country_log_msg.format(country, 'DE'))
            return 'DE'
        elif country == 'Österreich Königreich':
            # 'Österreich' (German for Austria) is not recognized by `pycountry_convert`
            # as a valid country. 'Austria' associated with the 'AT' alpha2 code are
            # used instead
            self.print_log("DEBUG", invalid_country_log_msg.format(country, 'AT'))
            return 'AT'
        elif country == 'Vereinigtes Königreich':
            # 'Vereinigtes Königreich' (German for United Kingdom) is not recognized
            # by `pycountry_convert` as a valid country. 'United Kingdom' associated
            # with the 'GB' alpha2 code are used instead
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
            self.print_log("ERROR", "KeyError: {}".format(e))
            return None
        else:
            log_msg = "The country '{}' will be updated to the " \
                      "standard name '{}'.".format(country, alpha2)
            self.print_log("DEBUG", log_msg)
            return alpha2

    @staticmethod
    def str_to_list(str_v):
        # If string of comma-separated values (e.g. 'Architecture, Developer APIs, Healthcare'),
        # return a list of values instead, e.g. ['Architecture', 'Developer APIs', 'Healthcare']
        return [v.strip() for v in str_v.split(',')]


def main():
    if g_util.create_directory_prompt(CACHED_WEBPAGES_DIRPATH):
        print("[WARNING] The program will exit")
        return 1

    # Start the scraping of job posts
    try:
        JobsScraper().start_scraping()
    except AssertionError as e:
        print(e)
        return 1


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt as e:
        pass
