import sqlite3
import time
from urllib.request import urlopen

from bs4 import BeautifulSoup
import requests
import ipdb


DB_FILENAME = "../../feeds.sqlite"


# TODO: utility function
def create_connection(db_file, autocommit=False):
    """
    Creates a database connection to the SQLite database specified by the db_file

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


def select_all_id_author_and_link(conn):
    """
    Returns all id, author and link from the `entries` table

    :param conn:
    :return:
    """
    sql = '''SELECT id, author, link FROM entries'''
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
        ids_authors_links = select_all_id_author_and_link(conn)

    # For each entry's link, get extra information from the given URL such as location and perks
    entries_data = {}
    count = 1
    print("[INFO] Total links={}".format(len(ids_authors_links)))
    for id, author,link in ids_authors_links:
        print("[INFO] #{} Processing {}".format(count, link))
        count += 1
        entries_data.setdefault(id, {})
        entries_data[id]["author"] = author
        entries_data[id]["link"] = link

        #html = urlopen("https://stackoverflow.com/jobs/147545/senior-backend-developer-ruby-on-rails-m-f-careerfoundry-gmbh")
        #html = urlopen("https://stackoverflow.com/jobs/153905/team-lead-php-auto1-group-gmbh?a=PC7V42U86uA")
        #html = urlopen("https://stackoverflow.com/jobs/154426/backend-software-engineer-asapp")
        #html = urlopen(link)

        #ipdb.set_trace()
        #link = "https://stackoverflow.com/jobs/158730/software-tester-m-w-testautomatisierung-in-staff-gmbh?a=Res6Ng8kG9a"

        try:
            req = session.get(link, headers=headers)
        except OSError:
            ipdb.set_trace()
        bsObj = BeautifulSoup(req.text, "lxml")

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

        entries_data[id]["location"] = location
        entries_data[id]["perks"] = perks_info
        entries_data[id]["overview_items"] = overview_items

        #ipdb.set_trace()

        print("[INFO] Finished Processing {}".format(link))
        print("[INFO] Sleeping zzzZZZZ")
        time.sleep(2)
        print("[INFO] Waking up\n")

        if count==30:
            ipdb.set_trace()

    ipdb.set_trace()
