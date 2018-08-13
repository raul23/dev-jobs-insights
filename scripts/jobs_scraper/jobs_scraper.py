import json
import os
import re
import sqlite3
import time
# from urllib.request import urlopen
# Third-party code
from bs4 import BeautifulSoup
import requests
import ipdb


DB_FILENAME = os.path.expanduser("~/databases/dev_jobs_insights.sqlite")


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
    print("[INFO] Total links to process = {}".format(len(job_ids_authors_links)))
    for job_id, author, link in job_ids_authors_links:
        print("\n[INFO] #{} Processing {}".format(count, link))
        count += 1

        entries_data.setdefault(job_id, {})
        entries_data[job_id]["author"] = author
        entries_data[job_id]["link"] = link

        # html = urlopen("https://stackoverflow.com/jobs/...")
        # html = urlopen(link)
        # ipdb.set_trace()

        try:
            req = session.get(link, headers=headers)
        except OSError:
            # TODO: process this exception
            ipdb.set_trace()
        bsObj = BeautifulSoup(req.text, "lxml")

        # Get job data from <script type="application/ld+json">:
        # On the web page of a job post, important data about the job post
        # (e.g. job location or salary) can be found in <script type="application/ld+json">
        # TODO: bsObj.find_all(type="application/ld+json") does the same thing?
        script_tag = bsObj.find(attrs={"type": "application/ld+json"})
        entries_data[job_id]["json_job_data"] = None
        if script_tag:
            # TODO: Sanity check: there should be only one script tag with type="application/ld+json"
            """
            The job data found in <script type="application/ld+json"> is a json
            object with the following keys:
            '@context', '@type', 'title', 'skills', 'description', 'datePosted',
            'validThrough', 'employmentType', 'experienceRequirements',
            'industry', 'jobBenefits', 'hiringOrganization', 'baseSalary', 'jobLocation'
            """
            job_data = json.loads(script_tag.get_text())
            entries_data[job_id]["json_job_data"] = job_data
        else:
            # Reasons for not finding <script>: maybe the page is not found
            # anymore (e.g. job post removed) or the company is not longer
            # accepting applications
            print("[WARNING] the page @ URL {} doesn't contain any SCRIPT tag "
                  "with type='application/ld+json'".format(link))

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
        link_tag = bsObj.select_one("header.job-details--header > div.grid--cell > .fc-black-700 > a")
        entries_data[job_id]["job_data_in_header"]["company_name"] = None
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

        # 2. Get the office location
        span_tag = bsObj.select_one("header.job-details--header > div.grid--cell > .fc-black-700 > .fc-black-500")
        entries_data[job_id]["job_data_in_header"]["office_location"] = None
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
        div_tag = bsObj.select_one("header.job-details--header > div.grid--cell > .mt12")
        entries_data[job_id]["job_data_in_header"]["other_job_data"] = {}
        if div_tag:
            children = div_tag.findChildren()
            for child in children:
                # Each job data is found within <span> with a class that starts
                # with '-', e.g. <span class='-salary pr16'
                classes = [tag_class for tag_class in child.attrs['class'] if tag_class.startswith('-')]
                if classes:
                    # TODO: sanity check. There should be only one class that starts with '-'
                    # len(classes) == 1
                    # Get the <div>'s class name without the '-' at the beginning,
                    # this will correspond to the type of job data (e.g. salary, remote)
                    job_data_type = classes[0][1:]
                    # Get the text (e.g. $71k - 85l) by removing any \r and \n around the string
                    if child.text:
                        job_data_value = child.text.strip()
                        entries_data[job_id]["job_data_in_header"]["other_job_data"][job_data_type] = job_data_value
                    else:
                        print("[WARNING] No text found for the job data type {}. URL @ {}".format(job_data_type, link))
                else:
                    print("[WARNING] The <span>'s class doesn't start with '-'. "
                          "Thus, we can't extract the job data. URL @ {}".format(link))
        else:
            print("[ERROR] Couldn't extract other job data @ the URL {}. "
                  "The other job data should be found in "
                  "header.job-details--header > div.grid--cell > .mt12".format(link))

        # Get more job data (e.g. role, company size, technologies) from <div id="overview-items">:
        div_tag = bsObj.find(id="overview-items")
        entries_data[job_id]["overview_items"] = None
        if div_tag:
            # TODO: Sanity check: there should be only one script tag with id="overview-items"
            entries_data[job_id]["overview_items"] = {'job_type': None,
                                                      'exp_level': None,
                                                      'job_role': None,
                                                      'industry': None,
                                                      'company_size': None,
                                                      'company_type': None,
                                                      'technologies': None
                                                      }
            # Get Job type
            # Get Experience level
            # Get Role
            # Get Industry
            # Get Company size
            # Get Company type
            # Get technologies
            pass
        else:
            print("[ERROR] the page @ URL {} doesn't contain any DIV tag "
                  "with id='overview-items'".format(link))

        print("[INFO] Finished Processing {}".format(link))
        print("[INFO] Sleeping zzzZZZZ")
        time.sleep(2)
        print("[INFO] Waking up")

        # TODO: debug code
        # if count == 30:
        #    ipdb.set_trace()

        # TODO: old code to be removed, it was based the old web page layout; thus the code is broken
        """
        # From <div class="job-detail-header">...</div>, get the location and perks (e.g. salary)
        job_detail_header = bsObj.find_all(class_="job-detail-header")
        perks_info = {}
        location = None
        if job_detail_header:
            # Get location
            location = job_detail_header[0].find_all(class_="-location")
            if location:
                location = location[0].get_text(strip=True)
                if location.startswith("- \n"):
                    location = location[3:]
            # Get perks
            perks = job_detail_header[0].find_all(class_="-perks g-row")
            if len(perks):
                for perk in perks[0].find_all("p"):
                    if 'class' in perk.attrs:
                        perk_type = perk.attrs['class'][0]
                        if perk_type.startswith("-"):
                            perk_type = perk_type[1:]
                        perks_info.setdefault(perk_type, [])
                        perk_values = perk.get_text(strip=True).split("|")
                        for perk_value in perk_values:
                            perk_value = perk_value.strip()
                            perks_info[perk_type].append(perk_value)

        # From <div id="overview-items">...</div>, get some important keyword about
        # the job such as the job type, the experience level, and the role
        tags = bsObj.find_all(id="overview-items")
        overview_items = {}
        if tags:
            items = tags[0].find_all(class_="-item g-col")
            if items:
                for item in items:
                    key = item.find(class_="-key")
                    if key:
                        key = key.get_text(strip=True)
                        if key.endswith(":"):
                            key = key[:-1]
                        value = item.find(class_="-value")
                        if value:
                            value = value.get_text(strip=True)
                            overview_items.setdefault(key, value)

        entries_data[job_id]["location"] = location
        entries_data[job_id]["perks"] = perks_info
        entries_data[job_id]["overview_items"] = overview_items
        """

    ipdb.set_trace()
