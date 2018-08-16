import codecs
import json
import os
import pathlib
import sqlite3
import sys
import time

from bs4 import BeautifulSoup
import ipdb
import requests

# TODO: path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
from utility import genutil as gu


DB_FILEPATH = os.path.expanduser("~/databases/dev_jobs_insights.sqlite")
# NOTE: if `CACHED_WEBPAGES_DIRPATH` is None, then the webpages will not be cached
# The webpages will then be retrieved from the internet.
CACHED_WEBPAGES_DIRPATH = os.path.expanduser("~/data/dev_jobs_insights/cache/webpages/stackoverflow_job_posts/")
SCRAPED_JOB_DATA_FILEPATH = os.path.expanduser("~/data/dev_jobs_insights/scraped_job_data.json")
DELAY_BETWEEN_REQUESTS = 2
# TODO: debug code
DEBUG = False


# TODO: utility function
def create_connection(db_filepath, autocommit=False):
    """
    Creates a database connection to the SQLite database specified by `db_filepath`

    :param db_filepath: database filepath
    :param autocommit: TODO
    :return: Connection object or None
    """
    try:
        if autocommit:
            conn = sqlite3.connect(db_filepath, isolation_level=None)
        else:
            conn = sqlite3.connect(db_filepath)
        return conn
    except sqlite3.Error as e:
        print(e)

    return None


def select_all_jobid_author_and_url(conn):
    """
    Returns all job_id, author and url from the `entries` table

    :param conn:
    :return:
    """
    sql = '''SELECT job_id, author, url FROM entries'''
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def main():
    if not gu.check_dir_exists(CACHED_WEBPAGES_DIRPATH):
        print("[ERROR] The cached webpages directory doesn't exist: {}".format(CACHED_WEBPAGES_DIRPATH))
        print("Do you want to create the directory?")
        answer = input("Y/N: ").strip().lower().startswith("y")
        if answer:
            print("[INFO] The directory {} will be created".format(CACHED_WEBPAGES_DIRPATH))
            # NOTE: only works on Python 3.4+ (however Python 3.4 pathlib is
            # missing `exist_ok` option
            # see https://stackoverflow.com/a/14364249 for different methods of
            # creating directories in Python 2.7+, 3.2+, 3.5+
            pathlib.Path(CACHED_WEBPAGES_DIRPATH).mkdir(parents=True, exist_ok=True)
        else:
            print("[WARNING] The program will exit")
            sys.exit(1)

    session = requests.Session()
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit 537.36 (KHTML, like Gecko) Chrome",
               "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"}
    conn = create_connection(DB_FILEPATH)
    with conn:
        # Get all the entries' URLs
        # TODO: check case where there is an error in executing the SQL query, e.g.
        # sqlite3.OperationalError: no such column: id
        job_ids_authors_urls = select_all_jobid_author_and_url(conn)

    # For each entry's URL, scrape more job data from the job post's webpage
    entries_data = {}
    count = 1
    last_request_time = -sys.float_info.max
    print("[INFO] Total URLs to process = {}".format(len(job_ids_authors_urls)))
    for job_id, author, url in job_ids_authors_urls:
        print("\n[INFO] #{} Processing {}".format(count, url))
        count += 1

        entries_data.setdefault(job_id, {})
        entries_data[job_id]["url"] = url
        entries_data[job_id]["webpage_accessed"] = None

        # Path where cached webpage's HTML will be saved
        filepath = os.path.join(CACHED_WEBPAGES_DIRPATH, "{}.html".format(job_id))

        # TODO: don't use a flag, use html=None instead
        get_webpage = True
        # Load the cached webpage's HTML if it is found
        if CACHED_WEBPAGES_DIRPATH:
            try:
                with open(filepath, 'r') as f:
                    html = f.read()
                print("[INFO] The cached webpage HTML is loaded from {}".format(filepath))
                get_webpage = False
                # entries_data[job_id]["webpage_accessed"] = gu.creation_date(filepath)
                entries_data[job_id]["webpage_accessed"] = os.path.getmtime(filepath)
            except OSError as e:
                print("[ERROR] {}".format(e))
                print("[INFO] The webpage HTML @ {} will be retrieved".format(url))

        if get_webpage:
            # Get the webpage HTML
            current_delay = time.time() - last_request_time
            diff_between_delays = current_delay - DELAY_BETWEEN_REQUESTS
            if diff_between_delays < 0:
                print("[INFO] Waiting {} seconds before sending next HTTP request...".format(abs(diff_between_delays)))
                time.sleep(abs(diff_between_delays))
                print("[INFO] Time is up! HTTP request will be sent.")
            try:
                req = session.get(url, headers=headers, timeout=5)
                entries_data[job_id]["webpage_accessed"] = time.time()
                html = req.text
            except OSError as e:
                # TODO: process this exception
                print("[ERROR] {}".format(e))
                print("[WARNING] The current URL {} will be skipped.".format(url))
                continue
            else:
                if req.status_code == 404:
                    print("[ERROR] PAGE NOT FOUND. The URL {} returned a 404 status code.".format(url))
                    print("[WARNING] The current URL {} will be skipped.".format(url))
                    continue
            last_request_time = time.time()
            print("[INFO] The webpage is retrieved from {}".format(url))

            # Save the webpage's HTML locally
            if CACHED_WEBPAGES_DIRPATH:
                # TODO: file path specified as argument to script
                try:
                    with open(filepath, 'w') as f:
                        f.write(html)
                    print("[INFO] The webpage is saved in {}. URL is {}".format(filepath, url))
                except OSError as e:
                    print("[ERROR] {}".format(e))
                    print("[WARNING] The webpage URL will not be saved locally")

                # TODO: debug code
                # if not gu.check_file_exists(filepath):
                #     ipdb.set_trace()

        bsObj = BeautifulSoup(html, "lxml")

        # Before extracting any job data from the job post, check if the job is
        # accepting applications by extracting the message
        # "This job is no longer accepting applications."
        # This notice is located in
        # body > div.container > div#content > aside.s-notice
        # NOTE: Usually when this notice is present in a job post, the json job
        # data is not found anymore within the html of the job post
        aside_tag = bsObj.select_one("body > div.container > div#content > aside.s-notice")
        entries_data[job_id]["job_post_notice"] = ""
        if aside_tag:
            entries_data[job_id]["job_post_notice"] = aside_tag.text

        # Get linked data from <script type="application/ld+json">:
        # On the webpage of a job post, important data about the job post
        # (e.g. job location or salary) can be found in <script type="application/ld+json">
        # This linked data is a JSON object that stores important job info like
        # employmentType, experienceRequirements, jobLocation
        script_tag = bsObj.find(attrs={"type": "application/ld+json"})
        entries_data[job_id]["linked_data"] = {}
        if script_tag:
            """
            The linked data found in <script type="application/ld+json"> is a json
            object with the following keys:
            '@context', '@type', 'title', 'skills', 'description', 'datePosted',
            'validThrough', 'employmentType', 'experienceRequirements',
            'industry', 'jobBenefits', 'hiringOrganization', 'baseSalary', 'jobLocation'
            """
            # TODO: get_text()/getText() or get_text/getText
            linked_data = json.loads(script_tag.get_text())
            entries_data[job_id]["linked_data"] = linked_data
        else:
            # Reasons for not finding <script type='application/ld+json'>:
            # maybe the page is not found anymore (e.g. job post removed) or
            # the company is not longer accepting applications
            print("[WARNING] the page @ URL {} doesn't contain any SCRIPT tag "
                  "with type='application/ld+json'".format(url))

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
        entries_data[job_id]["header"] = {}

        # 1. Get title of job post
        pattern = "header.job-details--header > div.grid--cell > h1.fs-headline1 > a"
        link_tag = bsObj.select_one(pattern)
        entries_data[job_id]["header"]["title"] = ""
        if link_tag:
            # TODO: sanity check. There should be only one tag that matches the above pattern
            title = link_tag.text
            if title:
                entries_data[job_id]["header"]["title"] = title
            else:
                print("[WARNING] The title of the job post is empty. URL @ {}".format(url))
        else:
            print("[ERROR] Couldn't extract the title of the job post @ the URL {}. "
                  "The title should be found in "
                  "{}".format(url, pattern))

        # 2. Get company name
        pattern = "header.job-details--header > div.grid--cell > div.fc-black-700 > a"
        link_tag = bsObj.select_one(pattern)
        entries_data[job_id]["header"]["company_name"] = ""
        if link_tag:
            # TODO: sanity check. There should be only one tag that matches the above pattern
            company_name = link_tag.text
            if company_name:
                entries_data[job_id]["header"]["company_name"] = company_name
            else:
                print("[WARNING] The company name is empty. URL @ {}".format(url))
        else:
            print("[ERROR] Couldn't extract the company name @ the URL {}. "
                  "The company name should be found in "
                  "{}".format(url, pattern))

        # 3. Get the office location which is located on the same line as the company name
        pattern = "header.job-details--header > div.grid--cell > div.fc-black-700 > span.fc-black-500"
        span_tag = bsObj.select_one(pattern)
        entries_data[job_id]["header"]["office_location"] = ""
        if span_tag:
            if span_tag.text:
                # The text where you find the location looks like this:
                # '\n|\r\nNo office location                    '
                # strip() removes the first '\n' and the right spaces. Then split('\n')[-1]
                # extracts the location string
                location = span_tag.text.strip().split('|')[-1].strip()
                entries_data[job_id]["header"]["office_location"] = location
            else:
                print("[WARNING] The office location is empty. URL @ {}".format(url))
        else:
            print("[ERROR] Couldn't extract the office location @ the URL {}. "
                  "The location should be found in "
                  "{}".format(url, pattern))

        # 4. Get the other job data on the next line after the company name and location
        pattern = "header.job-details--header > div.grid--cell > div.mt12"
        div_tag = bsObj.select_one(pattern)
        entries_data[job_id]["header"]["other_job_data"] = {}
        if div_tag:
            # Each `div_tag`'s child is associated to a job item (e.g. salary, remote)
            # and is found within a <span> tag with a class that starts with '-'
            # Example: header.job-details--header > div.grid--cell > .mt12 > span.-salary.pr16
            children = div_tag.findChildren()
            for child in children:
                # Each job data text is found within <span> with a class that starts
                # with '-', e.g. <span class='-salary pr16'>
                # NOTE: we need the child element's class that starts with '-' because
                # we will then know how to name the extracted job data item
                child_class = [tag_class for tag_class in child.attrs['class'] if tag_class.startswith('-')]
                if child_class:
                    # TODO: sanity check. There should be only one class that starts with '-'
                    # len(classes) == 1
                    # Get the <div>'s class name without the '-' at the beginning,
                    # this will correspond to the type of job data (e.g. salary, remote)
                    job_data_key = child_class[0][1:]
                    # Get the text (e.g. $71k - 85l) by removing any \r and \n around the string
                    if child.text:
                        job_data_value = child.text.strip()
                        entries_data[job_id]["header"]["other_job_data"][job_data_key] = job_data_value
                    else:
                        print("[ERROR] No text found for the job data type {}. URL @ {}".format(job_data_key, url))
                else:
                    print("[ERROR] The <span>'s class doesn't start with '-'. "
                          "Thus, we can't extract the job data. URL @ {}".format(url))
        else:
            print("[WARNING] Couldn't extract other job data @ the URL {}. "
                  "The other job data should be found in "
                  "{}".format(url, pattern))

        # Get job data from the Overview section. There are two places within
        # Overview section that will be extracted for more job data:
        # 1. in the "About this job" sub-section of Overview
        # 2. in the "Technologies" sub-section of Overview
        # NOTE: both sub-sections are located within <div id=""overview-items>

        # [overview-items]
        # 1. Get more job data (e.g. role, industry, company size) in the
        # "About this job" section. Each item is located in
        # "#overview-items > .mb32 > .job-details--about > .grid--cell6 > .mb8"
        # NOTE: these job data are presented in two columns, with three items per column
        pattern = "#overview-items > .mb32 > .job-details--about > .grid--cell6 > .mb8"
        div_tags = bsObj.select(pattern)
        entries_data[job_id]["overview_items"] = {}
        if div_tags:
            # Each `div_tag` corresponds to a job data item (e.g. Job type: Full-time, Company type: Private)
            for div_tag in div_tags:
                # Sample raw text: '\nJob type: \nContract\n'
                temp = div_tag.text.strip().split(":")
                job_data_key, job_data_value = temp[0].strip(), temp[1].strip()
                # The field names should all be lowercase and spaces be replaced with underscores
                # e.g. Job type ---> job_type
                job_data_key = job_data_key.replace(" ", "_").lower()
                entries_data[job_id]["overview_items"][job_data_key] = job_data_value
        else:
            print("[ERROR] Couldn't extract job data from the 'About this job'"
                  "section @ the URL {}. "
                  "The job data should be found in "
                  "{}".format(url, pattern))

        # [overview-items]
        # 2. Get the list of technologies, e.g. ruby, python, html5
        # NOTE: unlike the other job data in "overview_items", the technologies
        # are given as a list
        pattern = "#overview-items > .mb32 > div > a.job-link"
        link_tags = bsObj.select(pattern)
        entries_data[job_id]["overview_items"]["technologies"] = []
        if link_tags:
            for link_tag in link_tags:
                technology = link_tag.text
                if technology:
                    entries_data[job_id]["overview_items"]["technologies"].append(technology)
                else:
                    print("[ERROR] No text found for the technology with href={}. URL @ {}".format(link_tag["href"], url))
        else:
            print("[ERROR] Couldn't extract technologies from the 'Technologies'"
                  "section @ the URL {}. "
                  "The technologies should be found in "
                  "{}".format(url, pattern))

        print("[INFO] Finished Processing {}".format(url))

        # TODO: debug code
        if DEBUG and count == 30:
            ipdb.set_trace()

    ipdb.set_trace()

    # Save scraped data into json file
    # ref.: https://stackoverflow.com/a/31343739 (presence of unicode strings,
    # e.g. EURO currency symbol)
    with codecs.open(SCRAPED_JOB_DATA_FILEPATH, 'w', 'utf8') as f:
        f.write(json.dumps(entries_data, sort_keys=True, ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
