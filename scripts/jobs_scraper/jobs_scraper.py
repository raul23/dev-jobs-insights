import codecs
import json
import os
import sqlite3
import sys
import time

from bs4 import BeautifulSoup
import ipdb
import requests

# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
from utility import genutil as g_util


DB_FILEPATH = os.path.expanduser("~/databases/dev_jobs_insights.sqlite")
# NOTE: if `CACHED_WEBPAGES_DIRPATH` is None, then the webpages will not be cached
# The webpages will then be retrieved from the internet.
CACHED_WEBPAGES_DIRPATH = os.path.expanduser("~/data/dev_jobs_insights/cache/webpages/stackoverflow_job_posts/")
SCRAPED_JOB_DATA_FILEPATH = os.path.expanduser("~/data/dev_jobs_insights/scraped_job_data.json")
DELAY_BETWEEN_REQUESTS = 2
HTTP_GET_TIMEOUT = 5
# TODO: debug code
DEBUG = False


class Error(Exception):
    """Raised when the input value is too small"""
    pass


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
                              'cached_webpage_path', 'date_posted', 'valid_through', 'webpage_accessed', 'company_name',
                              'company_description', 'company_url', 'company_size', 'experience_level', 'industry',
                              'skills', 'job_benefits', 'salary_min_value', 'salary_max_value', 'salary_currency',
                              'job_location']

    def start_scraping(self):
        self.conn = g_util.connect_db(DB_FILEPATH)
        with self.conn:
            # Get all the entries' URLs
            try:
                rows = self.select_entries()
            except sqlite3.OperationalError as e:
                # g_util.print_exception("sqlite3.OperationalError")
                print("[ERROR] sqlite3.OperationalError: {}".format(e))
                print("[WARNING] Web scraping will end!")
                return
            else:
                if not rows:
                    print("[ERROR] The returned SQL result set is empty. Web "
                          "scraping will end!")
                    return

        # For each entry's URL, scrape more job data from the job post's webpage
        count = 1
        at_least_one_succeeded = False
        n_skipped = 0
        print("[INFO] Total URLs to process = {}".format(len(rows)))
        for job_id, author, url in rows:
            try:
                print("\n[INFO] [{}] #{} Processing {}".format(job_id, count, url))
                count += 1

                # Initialize the dict that will store scraped data from the given job post
                # and update the job post's URL
                self.init_scraped_job_post(job_id, {'url': url})

                ipdb.set_trace()
                # Load cached webpage or retrieve it online
                html = self.load_cached_webpage(job_id)
                if not html:
                    print("[WARNING] [{}] The current URL {} will be skipped.".format(job_id, url))
                    continue

                bsObj = BeautifulSoup(html, "lxml")

                ipdb.set_trace()

                # Before extracting any job data from the job post, check if the job is
                # accepting applications by extracting the message
                # "This job is no longer accepting applications."
                # This notice is located in
                # body > div.container > div#content > aside.s-notice
                # NOTE: Usually when this notice is present in a job post, the json job
                # data is not found anymore within the html of the job post
                self.process_notice(job_id, bsObj)

                # Get linked data from <script type="application/ld+json">
                self.process_linked_data(job_id, bsObj)

                # Get job data (e.g. salary, remote, location) from the <header>
                self.process_header(job_id, bsObj)

                # Get job data from the Overview section
                self.process_overview_items(job_id, bsObj)

                at_least_one_succeeded = True
                print("[INFO] [{}] Finished Processing {}".format(job_id, url))
            except KeyError as e:
                # g_util.print_exception("[job_id={}] KeyError")
                print("[ERROR] [{}] KeyError: {}".format(job_id, e))
                print("[WARNING] [{}] The current URL {} will be skipped".format(job_id, url))
                n_skipped += 1
                continue
        print()
        # Save scraped data into json file
        # ref.: https://stackoverflow.com/a/31343739 (presence of unicode strings,
        # e.g. EURO currency symbol)
        if at_least_one_succeeded and g_util.dump_json_with_codecs(SCRAPED_JOB_DATA_FILEPATH, self.scraped_job_posts) == 0:
            print("[INFO] Scraped data saved in {}".format(SCRAPED_JOB_DATA_FILEPATH))
        else:
            print("[ERROR] Scraped data couldn't be saved")
            print("[INFO] Skipped URLs={}/{}".format(n_skipped, len(rows)))

    def init_scraped_job_post(self, job_id, updated_values):
        self.scraped_job_posts[job_id] = {}.fromkeys(self.job_data_keys)
        # Add updated values
        self.update_scraped_job_post(job_id, updated_values)

    def get_value_from_scraped_job_post(self, job_id, key):
        job_id = 15
        return self.scraped_job_posts[job_id][key]

    def update_scraped_job_post(self, job_id, updated_values):
        job_id = 15
        for key, new_value in updated_values.items():
            print("[INFO] [{}] Trying to add the key={} with value={}".format(job_id, key, new_value))
            current_value = self.scraped_job_posts[job_id].get('k')
            if current_value:
                print("[WARNING] [{}] The key={} already has a value={}. Thus the "
                      "new_value={} will be ignored.".format(job_id, key, current_value, new_value))
                return 1
            else:
                self.scraped_job_posts[job_id].update({key: new_value})
                print("[INFO] [{}] The key={} was updated with value={}".format(job_id, key, new_value))
                return 0

    @staticmethod
    def get_location_in_linked_data(linked_data):
        job_locations = linked_data.get('jobLocation')
        if job_locations:
            processed_locations = []
            for location in job_locations:
                processed_locations.append({'city': location['address']['addressLocality'],
                                            'country': location['address']['addressCountry']})
            return processed_locations
        else:
            return None

    @staticmethod
    def get_text_from_tag(bsObj, pattern):
        try:
            return bsObj.select_one(pattern).text
        except AttributeError as e:
            # g_util.print_exception("AttributeError")
            print("[ERROR] AttributeError: {}".format(e))
            return None

    def get_webpage(self, url):
        html = None
        current_delay = time.time() - self.last_request_time
        diff_between_delays = current_delay - DELAY_BETWEEN_REQUESTS
        if diff_between_delays < 0:
            print("[INFO] Waiting {} seconds before sending next HTTP request...".format(abs(diff_between_delays)))
            time.sleep(abs(diff_between_delays))
            print("[INFO] Time is up! HTTP request will be sent.")
        try:
            req = self.session.get(url, headers=self.headers, timeout=HTTP_GET_TIMEOUT)
            html = req.text
        except OSError as e:
            # g_util.print_exception("OSError")
            print("[ERROR] OSError: {}".format(e))
            return None
        else:
            if req.status_code == 404:
                print("[ERROR] PAGE NOT FOUND. The URL {} returned a 404 status code.".format(url))
                return None
        self.last_request_time = time.time()
        print("[INFO] The webpage is retrieved from {}".format(url))

        return html

    # Load the cached webpage HTML if the webpage is found locally. If it isn't found
    # locally, then we will try to retrieve it with a GET request
    def load_cached_webpage(self, job_id):
        html = None
        url = self.get_value_from_scraped_job_post(job_id, 'url')
        # Path where the cached webpage's HTML will be saved
        filepath = os.path.join(CACHED_WEBPAGES_DIRPATH, "{}.html".format(job_id))

        if CACHED_WEBPAGES_DIRPATH:
            html = g_util.read_file(filepath)
        else:
            print("[WARNING] [{}] The caching option is disabled".format(job_id))

        if html:
            print("[INFO] [{}] The cached webpage HTML is loaded from {}".format(job_id, filepath))
            # Update cached webpage path and its datetime modified
            self.update_scraped_job_post(job_id, {'cached_webpage_path': filepath,
                                                  'webpage_accessed': os.path.getmtime(filepath)})
        else:
            print("[INFO] Instead the webpage HTML @ {} will be retrieved with a GET request".format(url))
            # Get the webpage HTML
            html = self.get_webpage(job_id, url)
            if html:
                # Update the datetime the webpage was retrieved (though not 100% accurate)
                self.update_scraped_job_post(job_id, {'webpage_accessed': time.time()})
                if self.save_webpage_locally(url, filepath, html) == 0:
                    # Update the path the webpage is cached
                    self.update_scraped_job_post(job_id, {'cached_webpage_path': filepath})
            else:
                # No html retrieved at all
                return None

        return html

    def process_header(self, job_id, url, bsObj):
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
        pass

    def process_linked_data(self, job_id, bsObj):
        # Get linked data from <script type="application/ld+json">:
        # On the webpage of a job post, important data about the job post
        # (e.g. job location or salary) can be found in <script type="application/ld+json">
        # This linked data is a JSON object that stores important job info like
        # employmentType, experienceRequirements, jobLocation
        script_tag = bsObj.find_one(attrs={"type": "application/ld+json"})
        url = self.get_value_from_scraped_job_post(job_id, 'url')
        if script_tag:
            """
            The linked data found in <script type="application/ld+json"> is a json
            object with the following keys:
            '@context', '@type', 'title', 'skills', 'description', 'datePosted',
            'validThrough', 'employmentType', 'experienceRequirements',
            'industry', 'jobBenefits', 'hiringOrganization', 'baseSalary', 'jobLocation'
            """
            linked_data = json.loads(script_tag.get_text())
            updated_values = {'title': linked_data.get('title'),
                              'job_post_description': linked_data.get('description'),
                              'employment_type': linked_data.get('employmentType'),
                              'date_posted': linked_data.get('datePosted'),
                              'valid_through': linked_data.get('validThrough'),
                              'experience_level': self.str_to_list(linked_data.get('experienceRequirements')),
                              'industry': linked_data.get('industry'),
                              'skills': linked_data.get('skills'),
                              'job_benefits': linked_data.get('skills'),
                              'skills': linked_data.get('jobBenefits'),
                              'company_description': linked_data.get('hiringOrganization').get('description'),
                              'company_name': linked_data.get('hiringOrganization').get('name'),
                              'company_site_url': linked_data.get('hiringOrganization').get('sameAs'),
                              'salary_min_value': linked_data.get('baseSalary').get('value').get('minValue'),
                              'salary_max_value': linked_data.get('baseSalary').get('value').get('maxValue'),
                              'salary_currency': linked_data.get('baseSalary').get('currency'),
                              'job_location': self.get_location_in_linked_data(linked_data)
                              }
            self.update_scraped_job_post(job_id, updated_values)
            print("[INFO] [{}] The linked data from URL {} were successfully scraped".format(job_id, url))
        else:
            # Reasons for not finding <script type='application/ld+json'>:
            # maybe the page is not found anymore (e.g. job post removed) or
            # the company is not longer accepting applications
            print("[WARNING] [{}] The page @ URL {} doesn't contain any SCRIPT tag "
                  "with type='application/ld+json'".format(job_id, url))

    def process_notice(self, job_id, bsObj):
        url = self.get_value_from_scraped_job_post(job_id, 'url')
        text = self.get_text_from_tag(bsObj, "body > div.container > div#content > aside.s-notice")
        if text:
            print("[WARNING] [{}] Job notice found for URL {}".format(job_id, url))
            self.update_scraped_job_post(job_id, {'job_post_notice': text})
        else:
            print("[INFO] [{}] No warning job notice found for URL {}.".format(job_id, url))

    def process_overview_items(self, job_id, bsObj):
        # Get job data from the Overview section. There are two places within
        # Overview section that will be extracted for more job data:
        # 1. in the "About this job" sub-section of Overview
        # 2. in the "Technologies" sub-section of Overview
        # NOTE: both sub-sections are located within <div id=""overview-items>
        # [overview-items]
        url = self.get_value_from_scraped_job_post(job_id, 'url')
        convert_keys = {'job_type': 'employment_type',
                        'technologies': 'skills'}

        # 1. Get more job data (e.g. role, industry, company size) in the
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
                job_data_key, job_data_value = temp[0].strip(), temp[1].strip()
                # The field names should all be lowercase and spaces be replaced
                # with underscores e.g. Job type ---> job_type
                job_data_key = job_data_key.replace(" ", "_").lower()
                # Convert the key name to use the standard key name
                job_data_key = convert_keys.get(job_data_key, job_data_key)
                self.update_scraped_job_post(job_id, {job_data_key: job_data_value})
        else:
            print("[ERROR] [{}] Couldn't extract job data from the 'About this job'"
                  "section @ the URL {}. "
                  "The job data should be found in "
                  "{}".format(job_id, url, pattern))

        # [overview-items]
        # 2. Get the list of technologies, e.g. ruby, python, html5
        # NOTE: unlike the other job data in "overview_items", the technologies
        # are given as a list
        pattern = "#overview-items > .mb32 > div > a.job-link"
        link_tags = bsObj.select(pattern)
        if link_tags:
            for link_tag in link_tags:
                technology = link_tag.text
                if technology:
                    pass
                    #entries_data[job_id]["overview_items"]["technologies"].append(technology)
                else:
                    print(
                        "[ERROR] [{}] No text found for the technology with href={}. "
                        "URL @ {}".format(job_id, link_tag["href"], url))
        else:
            print("[ERROR] [{}] Couldn't extract technologies from the 'Technologies'"
                  "section @ the URL {}. "
                  "The technologies should be found in "
                  "{}".format(job_id, url, pattern))

    @staticmethod
    def save_webpage_locally(url, filepath, html):
        if CACHED_WEBPAGES_DIRPATH:
            if g_util.write_file(filepath, html) == 0:
                print("[INFO] The webpage is saved in {}. URL is {}".format(filepath, url))
                return 0
            else:
                print("[WARNING] The webpage @ URL {} will not be saved locally".format(url))
                return 1
        else:
            print("[WARNING] The caching option is disabled. Thus, the webpage"
                  "@ URL {} will not be saved locally.".format(url))
            return 1

    @staticmethod
    def str_to_list(str_v):
        # If string of comma-separated values (e.g. 'Architecture, Developer APIs, Healthcare'),
        # return a list of values instead, e.g. ['Architecture', 'Developer APIs', 'Healthcare']
        return [v.strip() for v in str_v.split(",")]

    def select_entries(self):
        """
        Returns all job_id, author and url from the `entries` table

        :return:
        """
        sql = '''SELECT job_id, author, url FROM entries'''
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()


def main():
    if g_util.create_directory_prompt(CACHED_WEBPAGES_DIRPATH):
        print("[WARNING] The program will exit")
        sys.exit(1)

    # Start the scraping of job posts
    JobsScraper().start_scraping()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt as e:
        # g_util.print_exception("KeyboardInterrupt")
        # print("[ERROR] KeyboardInterrupt: {}".format(e))
        pass
