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
        tag = bsObj.find_all(attrs={"type": "application/ld+json"})
        if tag:
            # TODO: Sanity check: there should be only one script tag with type="application/ld+json"
            # assert len(tag) == 1
            """
            The job data found in <script type="application/ld+json"> is a json
            object with the following keys:
            '@context', '@type', 'title', 'skills', 'description', 'datePosted',
            'validThrough', 'employmentType', 'experienceRequirements',
            'industry', 'jobBenefits', 'hiringOrganization', 'baseSalary', 'jobLocation'
            """
            job_data = json.loads(tag[0].get_text())
            entries_data[job_id]["json_job_data"] = job_data
        else:
            # Maybe the page is not found anymore or the company is not longer
            # accepting applications
            print("[WARNING] the page @ URL {} doesn't contain any SCRIPT tag "
                  "with type='application/ld+json'".format(link))
            entries_data[job_id]["json_job_data"] = None

        # Get more job data from <header class="job-details">:
        # On the web page of a job post, more job data (e.g. salary, remote,
        # location) can be found in <header class="job-details">
        tag = bsObj.find("header", class_="job-details--header")
        entries_data[job_id]["header_job_details"] = None
        if tag:
            # TODO: Sanity check: there should be only one header tag with class="overview-items"
            # assert len(tag) == 1
            entries_data[job_id]["header_job_details"] = {"company_name": None,
                                                          "location": None
                                                          }

            # NOTE: the company name and location are the only two piece of job
            # data in <header class="job-details"> that don't have a direct parent
            # of <span class="-name_of_data"> where name_of_data can be {salary, remote}

            # NOTE: in <header class="job-details">, the company name and location
            # are to be found one beside the other.

            # Get company name and location
            link_tags = tag.find_all("a", href=re.compile("^/jobs/companies"))
            # TODO: sanity check. There should be only two places that match
            # the pattern "^/jobs/companies"
            # assert len(link_tags) == 2
            for link_tag in link_tags:
                # In <header>, there are potentially two places where you can
                # find href="/jobs/companies". The first place is associated with
                # the image of the company within <div class="s-avatar"> which doesn't
                # contain the text of the company name. The second place is where you
                # will find the text of the company name and it is usually found in
                # <div class="grid--cell">. However, here we are just testing if there
                # is text in <a href="/jobs/companies"> and if it's the case then we found
                # the good <a> that contains the company name.
                if link_tag.text:
                    entries_data[job_id]["header_job_details"]["company_name"] = link_tag.text
                    # Get location which is found right after the company name
                    next_sibling = link_tag.find_next_sibling()
                    # The text where you find the location looks like this:
                    # '\n|\r\nNo office location                    '
                    # This the first strip() removes the first '\n' and the right
                    # spaces. Then the split('\n')[-1] extracts the location info
                    location = next_sibling.text.strip().split('\n')[-1]
                    entries_data[job_id]["header_job_details"]["location"] = location
                    break

            # Get other job data
            # In the other job data
            pass
        else:
            pass

        # Get more job data from <div id="overview-items">:
        # On the web page of a job post, more job data (e.g. role, company
        # size, technologies) can be found in <div id="overview-items">
        tag = bsObj.find_all(id="overview-items")
        entries_data[job_id]["overview_items"] = None
        if tag:
            # TODO: Sanity check: there should be only one script tag with id="overview-items"
            # assert len(tag) == 1
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
            print("[WARNING] the page @ URL {} doesn't contain any DIV tag "
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
