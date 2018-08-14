import codecs
import json
import os
import sqlite3
import sys
import time
# Third-party code
from bs4 import BeautifulSoup
import requests
import ipdb
# Own code
# TODO: path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
from utility import genutil


DB_FILENAME = os.path.expanduser("~/databases/dev_jobs_insights.sqlite")
# NOTE: if `CACHED_WEB_PAGES_PATH` is None, then the web pages will not be cached
# The web pages will then be retrieved from the internet.
CACHED_WEB_PAGES_PATH = os.path.expanduser("~/data/dev_jobs_insights/cached/web_pages/stackoverflow_job_posts/")
DELAY_BETWEEN_REQUESTS = 15
DEBUG = True


# TODO: utility function
def create_connection(db_file, autocommit=False):
    """
    Creates a database connection to the SQLite database specified by `db_file`

    :param db_file: database file
    :param autocommit: TODO
    :return: Connection object or None
    """
    try:
        if autocommit:
            conn = sqlite3.connect(db_file, isolation_level=None)
        else:
            conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(e)

    return None


def select_all_job_id_author_and_link(conn):
    """
    Returns all job_id, author and link from the `entries` table

    :param conn:
    :return:
    """
    sql = '''SELECT job_id, author, link FROM entries'''
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


if __name__ == '__main__':
    ipdb.set_trace()
    """
    if not genutil.check_dir_exists(CACHED_WEB_PAGES_PATH):
        print("[ERROR] The cached web pages directory doesn't exist: {}".format(CACHED_WEB_PAGES_PATH))
        # TODO: ask user if directory should be created
        sys.exit(1)
    """

    session = requests.Session()
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit 537.36 (KHTML, like Gecko) Chrome",
               "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"}
    conn = create_connection(DB_FILENAME)
    with conn:
        # Get all the entries' links
        # TODO: check case where there is an error in executing the sql query, e.g.
        # sqlite3.OperationalError: no such column: id
        job_ids_authors_links = select_all_job_id_author_and_link(conn)

    # For each entry's link, get extra information from the given URL such as
    # location and perks
    entries_data = {}
    count = 1
    last_request_time = -sys.float_info.max
    print("[INFO] Total links to process = {}".format(len(job_ids_authors_links)))
    for job_id, author, link in job_ids_authors_links:
        print("\n[INFO] #{} Processing {}".format(count, link))
        count += 1

        entries_data.setdefault(job_id, {})
        entries_data[job_id]["author"] = author
        entries_data[job_id]["link"] = link

        ipdb.set_trace()
        # Path where cached web page's HTML will be saved
        filepath = os.path.join(CACHED_WEB_PAGES_PATH, "{}.html".format(job_id))

        get_web_page = True
        try:
            # Load the cached web page's HTML if it is found
            with open(filepath, 'r') as f:
                html = f.read()
            print("[INFO] The cached web page HTML is loaded from {}".format(filepath))
            get_web_page = False
        except OSError as e:
            print("[ERROR] {}".format(e))
            print("[INFO] The web page HTML @ {} will be retrieved".format(link))

        if get_web_page:
            # Get the web page HTML
            current_delay = time.time() - last_request_time
            diff_between_delays = current_delay - DELAY_BETWEEN_REQUESTS
            if diff_between_delays < 0:
                print("[INFO] Waiting before sending next HTTP request...")
                time.sleep(diff_between_delays)
                print("[INFO] Time is up! HTTP request will be sent.")
            try:
                req = session.get(link, headers=headers)
                html = req.text
            except OSError as e:
                # TODO: process this exception
                print("[ERROR] {}".format(e))
                ipdb.set_trace()
            last_request_time = time.time()
            print("[INFO] The web page is retrieved from {}".format(link))

            # Save the web page's HTML locally
            if CACHED_WEB_PAGES_PATH:
                # TODO: file path specified as argument to script
                try:
                    with open(filepath, 'w') as f:
                        f.write(html)
                    print("[INFO] The web page is saved in {}. URL is {}".format(filepath, link))
                except OSError as e:
                    print("[ERROR] {}".format(e))

        bsObj = BeautifulSoup(html, "lxml")

        # Get job data from <script type="application/ld+json">:
        # On the web page of a job post, important data about the job post
        # (e.g. job location or salary) can be found in <script type="application/ld+json">
        script_tag = bsObj.find(attrs={"type": "application/ld+json"})
        entries_data[job_id]["json_job_data"] = {}
        entries_data[job_id]["json_job_data_warning"] = None
        if script_tag:
            # TODO: Sanity check: there should be only one script tag with type="application/ld+json"
            """
            The job data found in <script type="application/ld+json"> is a json
            object with the following keys:
            '@context', '@type', 'title', 'skills', 'description', 'datePosted',
            'validThrough', 'employmentType', 'experienceRequirements',
            'industry', 'jobBenefits', 'hiringOrganization', 'baseSalary', 'jobLocation'
            """
            # TODO: get_text()/getText() or get_text/getText
            job_data = json.loads(script_tag.get_text())
            entries_data[job_id]["json_job_data"] = job_data
        else:
            # Reasons for not finding <script>: maybe the page is not found
            # anymore (e.g. job post removed) or the company is not longer
            # accepting applications
            # TODO: extract the message "This job is no longer accepting applications." located in
            # body > div.container > div#content > aside.s-notice
            print("[WARNING] the page @ URL {} doesn't contain any SCRIPT tag "
                  "with type='application/ld+json'".format(link))
            aside_tag = bsObj.select_one("body > div.container > div#content > aside.s-notice")
            if aside_tag:
                entries_data[job_id]["json_job_data_warning"] = aside_tag.text
            else:
                print("[WARNING] the page @ URL {} doesn't contain any ASIDE tag. "
                      "Notice text couldn't be extracted.".format(link))

        # Get more job data (e.g. salary, remote, location) from the <header>
        # The job data in the <header> are found in this order:
        # 1. company name
        # 2. office location
        # 3. Other job data: Salary, Remote, Visa sponsor, Paid relocation, ...
        # NOTE: the company name and office location are found on the same line
        # separated by a vertical line. The other job data are to be all found on
        # the same line (after the company name and office location) and these
        # job data are all part of a class that starts with '-', e.g. '-salary',
        # '-remote' or '-visa'
        entries_data[job_id]["job_data_in_header"] = {}

        # 1. Get company name
        link_tag = bsObj.select_one("header.job-details--header > div.grid--cell > div.fc-black-700 > a")
        entries_data[job_id]["job_data_in_header"]["company_name"] = {}
        if link_tag:
            # TODO: sanity check. There should be only one tag that matches the above pattern
            company_name = link_tag.text
            if company_name:
                entries_data[job_id]["job_data_in_header"]["company_name"] = company_name
            else:
                print("[WARNING] The company name is empty. URL @ {}".format(link))
        else:
            print("[ERROR] Couldn't extract the company name @ the URL {}. "
                  "The company name should be found in "
                  "header.job-details--header > div.grid--cell > .fc-black-700 > a".format(link))

        # 2. Get the office location which is located on the same line as the company name
        span_tag = bsObj.select_one("header.job-details--header > div.grid--cell > div.fc-black-700 > span.fc-black-500")
        entries_data[job_id]["job_data_in_header"]["office_location"] = {}
        if span_tag:
            if span_tag.text:
                # The text where you find the location looks like this:
                # '\n|\r\nNo office location                    '
                # strip() removes the first '\n' and the right spaces. Then split('\n')[-1]
                # extracts the location string
                location = span_tag.text.strip().split('|')[-1].strip()
                entries_data[job_id]["job_data_in_header"]["office_location"] = location
            else:
                print("[WARNING] The office location is empty. URL @ {}".format(link))
        else:
            print("[ERROR] Couldn't extract the office location @ the URL {}. "
                  "The location should be found in "
                  "header.job-details--header > div.grid--cell > .fc-black-700 > .fc-black-500".format(link))

        # 3. Get the other job data on the next line after the company name and location
        div_tag = bsObj.select_one("header.job-details--header > div.grid--cell > div.mt12")
        entries_data[job_id]["job_data_in_header"]["other_job_data"] = {}
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
                    job_data_type = child_class[0][1:]
                    # Get the text (e.g. $71k - 85l) by removing any \r and \n around the string
                    if child.text:
                        job_data_value = child.text.strip()
                        entries_data[job_id]["job_data_in_header"]["other_job_data"][job_data_type] = job_data_value
                    else:
                        print("[ERROR] No text found for the job data type {}. URL @ {}".format(job_data_type, link))
                else:
                    print("[ERROR] The <span>'s class doesn't start with '-'. "
                          "Thus, we can't extract the job data. URL @ {}".format(link))
        else:
            print("[WARNING] Couldn't extract other job data @ the URL {}. "
                  "The other job data should be found in "
                  "header.job-details--header > div.grid--cell > .mt12".format(link))

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
        div_tags = bsObj.select("#overview-items > .mb32 > .job-details--about > .grid--cell6 > .mb8")
        entries_data[job_id]["overview_items"] = {}
        if div_tags:
            # Each `div_tag` corresponds to a job data item (e.g. Job type: Full-time, Company type: Private)
            for div_tag in div_tags:
                # Sample raw text: '\nJob type: \nContract\n'
                temp = div_tag.text.strip().split(":")
                job_data_type, job_data_value = temp[0].strip(), temp[1].strip()
                entries_data[job_id]["overview_items"][job_data_type] = job_data_value
        else:
            print("[ERROR] Couldn't extract job data from the 'About this job'"
                  "section @ the URL {}. "
                  "The job data should be found in "
                  "#overview-items > .mb32 > .job-details--about > .grid--cell6".format(link))

        # [overview-items]
        # 2. Get the list of technologies, e.g. ruby, python, html5
        link_tags = bsObj.select("#overview-items > .mb32 > div > a.job-link")
        entries_data[job_id]["overview_items"]["technologies"] = []
        if link_tags:
            for link_tag in link_tags:
                technology = link_tag.text
                if technology:
                    entries_data[job_id]["overview_items"]["technologies"].append(technology)
                else:
                    print("[ERROR] No text found for the technology with href={}. URL @ {}".format(link_tag["href"], link))
        else:
            print("[ERROR] Couldn't extract technologies from the 'Technologies'"
                  "section @ the URL {}. "
                  "The technologies should be found in "
                  "#overview-items > .mb32 > div > a.job-link".format(link))

        print("[INFO] Finished Processing {}".format(link))

        # TODO: debug code
        if DEBUG and count == 30:
            ipdb.set_trace()

    ipdb.set_trace()

    # Save scraped data into json file
    # ref.: https://stackoverflow.com/a/31343739 (presence of unicode strings,
    # e.g. EURO currency symbol)
    with codecs.open('data.json', 'w', 'utf8') as f:
        f.write(json.dumps(entries_data, sort_keys=True, ensure_ascii=False))

    ipdb.set_trace()

    # Load json data (scraped data)
    f = codecs.open('data.json', 'r', 'utf8')
    # TODO: json.load or json.loads?
    data = json.load(f)
    f.close()

    ipdb.set_trace()