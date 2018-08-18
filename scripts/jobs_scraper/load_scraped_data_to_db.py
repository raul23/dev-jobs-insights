import codecs
import json
import os
import re
import sqlite3
import sys

from forex_python.converter import convert, get_symbol, RatesNotAvailableError
import ipdb
import requests

# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
from utility import genutil as gu


SCRAPED_JOB_DATA_FILEPATH = os.path.expanduser("~/data/dev_jobs_insights/scraped_job_data.json")
DB_FILEPATH = os.path.expanduser("~/databases/dev_jobs_insights.sqlite")
CURRENCY_FILEPATH = os.path.expanduser("~/data/dev_jobs_insights/currencies.json")
DEST_CURRENCY = "USD"
DEST_SYMBOL = "$"


def replace_letter(string):
    regex = r"(?P<number>\d)k"
    subst = "\g<number>000"
    new_string = re.sub(regex, subst, string, 0)
    return new_string


def get_currency_code(currency_symbol, currency_data):
    # NOTE: there is a not 1-to-1 mapping when going from currency symbols
    # to currency code
    # e.g. the currency symbol £ is used for the currency codes EGP, FKP, GDP,
    # GIP, LBP, and SHP
    results = [item for item in currency_data if item["symbol"] == currency_symbol]
    # NOTE: C$ is used as a currency symbol for Canadian Dollar instead of $
    # However, C$ is already the official currency symbol for Nicaragua Cordoba (NIO)
    # Thus we will assume that C$ is related to the Canadian Dollar.
    if currency_symbol != "C$" and len(results) == 1:
        # Found only one currency code associated with the given currency symbol
        return results[0]["cc"]
    else:
        # Two possible cases
        # 1. Too many currency codes associated with the given currency symbol
        # 2. It is not a valid currency symbol
        if currency_symbol == "A$":  # Australian dollar
            currency_code = "AUD"
        elif currency_symbol == "C$":  # Canadian dollar
            currency_code = "CAD"
        elif currency_symbol == "£":  # We assume £ is always associated with the British pound
            currency_code = "GBP"     # However, it could have been EGP, FKP, GIP, ...
        else:
            print("WARNING: Could not get a currency code from {}".format(currency_symbol))
            return None
        return currency_code


def convert_currency(amount, base_currency, dest_currency="USD"):
    # Sanity check on `amount`
    assert type(amount) in [float, int], "amount is not of type int or float"
    # Sanity check for `base_currency` to make sure it is a valid currency code
    assert get_symbol(base_currency) is not None, "currency code '{}' is not a valid currency ".format(base_currency)
    if get_symbol(base_currency) is None:
        # `prefix_currency` is not a valid currency code. It might be a currency symbol
        #
        # Get currency code from possible currency symbol
        base_currency = get_currency_code(base_currency)
        if base_currency is None:
            return None
    try:
        converted_amount = convert(base_currency, dest_currency, amount)
    except RatesNotAvailableError:
        ipdb.set_trace()
        return None
    except requests.exceptions.ConnectionError:
        # When no connection to api.fixer.io (e.g. working offline)
        # TODO: retrieve the rates beforehand, it is a lot quicker
        # or at least cache the rates
        ipdb.set_trace()
        converted_amount = amount * 1.25
    converted_amount = int(round(converted_amount))
    # TODO: save this note somewhere else
    # NOTE: round(a, 2) doesn't work in python 2.7:
    # >> a = 0.3333333
    # >> round(a, 2),
    # Use the following in python2.7:
    # >> float(format(a, '.2f'))
    return converted_amount


def append_perks(prefix_item, input_items, output_items):
    for name, values in input_items.items():
        name_orig = name
        if type(values) is not list:
            values = [values]
        for val in values:
            for v in val.split(","):
                # Remove white spaces
                v = v.strip()
                if name in ["company_size", "salary"]:
                    v = replace_letter(v)
                if name == "salary" and "Equity" != v:
                    # Get the min and max salary
                    min_salary, max_salary = get_min_max_salary(v)
                    # Remove trailing white spaces from searched string
                    prefix_currency = re.search('^\D+', v).group().strip()
                    # TODO: remove debugging
                    #if prefix_currency == "C$":
                    #    ipdb.set_trace()
                    if not prefix_currency.startswith(DEST_SYMBOL):
                        if get_symbol(prefix_currency) is None:
                            base_currency_code = get_currency_code(prefix_currency)
                            if base_currency_code is None:
                                base_currency_code = prefix_currency
                        else:
                            base_currency_code = prefix_currency
                        min_salary = convert_currency(min_salary,
                                                      base_currency=base_currency_code,
                                                      dest_currency=DEST_CURRENCY)
                        max_salary = convert_currency(max_salary,
                                                      base_currency=base_currency_code,
                                                      dest_currency=DEST_CURRENCY)
                        if min_salary and max_salary:
                            output_items["job_perks"].append((prefix_item,
                                                              "salary ({})".format(DEST_CURRENCY),
                                                              "{}{} - {}".format(DEST_SYMBOL, min_salary, max_salary)))
                    else:
                        # Sanity check on `prefix_currency`
                        assert prefix_currency == DEST_SYMBOL, \
                            "'{}' is not equal to the destination symbol '{}'".format(prefix_currency, DEST_SYMBOL)
                        base_currency_code = DEST_CURRENCY
                    if min_salary and max_salary:
                        output_items["job_salary"].append((prefix_item,
                                                           "min salary ({})".format(DEST_CURRENCY),
                                                           min_salary))
                        output_items["job_salary"].append((prefix_item,
                                                           "max salary ({})".format(DEST_CURRENCY),
                                                           max_salary))
                    name = 'salary ({})'.format(base_currency_code)
                output_items["job_perks"].append((prefix_item, name, v))
                name = name_orig


def get_min_max_salary(salary_range):
    # Sanity check for `salary_range`
    assert "-" in salary_range, "salary range '{}' doesn't have a valid format".format(salary_range)
    regex = r"(^\D+)(?P<number>\d+)"
    subst = "\g<number>"
    salary_range = re.sub(regex, subst, salary_range, 0)
    min_salary, max_salary = salary_range.replace(" ", "").split("-")
    min_salary = round(int(min_salary))
    max_salary = round(int(max_salary))
    return min_salary, max_salary


# Build SQL query for the job_posts table
def build_job_posts_query(job_data):
    if hasattr(job_data, 'linked_data'):
        # Get job post title: try first to get it from the linked data
        title = job_data['linked_data'].get(['title'], "")

    else:
        # Fallback
        pass


def str_to_list(str_v):
    # If string of comma-separated values (e.g. 'Architecture, Developer APIs, Healthcare'),
    # return a list of values instead, e.g. ['Architecture', 'Developer APIs', 'Healthcare']
    return [v.strip() for v in str_v.split(",")]


if __name__ == '__main__':
    with open(CURRENCY_FILEPATH) as f1, codecs.open(SCRAPED_JOB_DATA_FILEPATH, 'r', 'utf8') as f2:
        currency_data = json.loads(f1.read())
        # TODO: json.load or json.loads?
        scraped_data = json.load(f2)

    conn = gu.connect_db(DB_FILEPATH)
    with conn:

        # Initialize SQL queries
        job_posts_queries = []
        hiring_company_queries = []
        experience_level_queries = []
        industry_queries = []
        skills_queries = []
        job_benefits_queries = []
        job_salary_queries = []
        location_queries = []

        count = 1
        print("[INFO] Total job posts to process = {}".format(len(scraped_data)))
        for job_id, job_data in scraped_data.items():
            print("\n[INFO] #{} Processing job post with job_id={}".format(count, job_id))
            count += 1

            ipdb.set_trace()

            # Extract the relevant job data that will be used to populate the tables in the sqlite db
            url = job_data['url']
            job_notice = job_data['job_post_notice']
            cached_webpage_path = job_data['cached_webpage_path']
            webpage_accessed = job_data['webpage_accessed']

            # From the header section
            # TODO: fallback if linked data's job_post_title not found
            title = job_data['header']['title']
            # TODO: fallback if linked data's company_name not found
            company_name = job_data['header']['company_name']
            office_location = job_data['header']['office_location']
            other_job_data = job_data['header']['other_job_data']

            # From the overview-items section
            company_size = job_data['overview_items']['company_size']
            # TODO: fallback if linked data's experience_level not found
            experience_level = job_data['overview_items']['experience_level']
            # TODO: fallback if linked data's industry not found
            industry = job_data['overview_items']['industry']
            # TODO: fallback if linked data's employment_type not found
            job_type = job_data['overview_items']['job_type']
            role = job_data['overview_items']['role']
            # TODO: fallback if linked data's skills not found
            technologies = job_data['overview_items']['technologies']

            # From the linked data section
            job_post_title = job_data['linked_data']['title']
            job_post_description = job_data['linked_data']['description']
            employment_type = job_data['linked_data']['employmentType']
            date_posted = job_data['linked_data']["datePosted"]
            valid_through = job_data['linked_data']["validThrough"]
            experience_level = str_to_list(job_data['linked_data']['experienceRequirements'])
            industry = job_data['linked_data']['industry']
            skills = job_data['linked_data']['skills']
            job_benefits = job_data['linked_data']['job_benefits']
            # [hiringOrganization]
            company_description = job_data['linked_data']['hiringOrganization']['description']
            company_name = job_data['linked_data']['hiringOrganization']['name']
            company_site_url = job_data['linked_data']['hiringOrganization']['sameAs']
            # [baseSalary]
            min_value = job_data['linked_data']['baseSalary']['value']['minValue']
            max_value = job_data['linked_data']['baseSalary']['value']['maxValue']
            currency = job_data['linked_data']['baseSalary']['currency']
            # [jobLocation]
            locations = []
            for location in job_data['linked_data']['jobLocation']:
                locations.append({'city': location['address']['addressLocality'],
                                 'country': location['address']['addressCountry']})
            ipdb.set_trace()

            # Build SQL query for the job_posts table
            # Get job post title: try first from the linked data
            title = job_data['linked_data']["title"]

            query = (job_id)
            job_posts_queries.append(query)

            # `location` can have two locations in one separated by ;
            # e.g. Teunz, Germany; Kastl, Germany
            location = job_data['location']
            job_posts.append((id, author, url, location))
            perks = job_data['perks']
            overview_items = job_data['overview_items']
            append_perks(prefix_item=id, input_items=perks, output_items={'job_perks': job_perks,
                                                                          'job_salary': job_salary})
            # TODO: uncomment when done with debugging
            #append_overview(prefix_item=id, input_items=overview_items, output_items={'job_overview': job_overview})

        ipdb.set_trace()
        cur = conn.cursor()
        # TODO: uncomment when done with debugging
        #cur.executemany("INSERT INTO job_posts VALUES(?, ?, ?, ?)", job_posts)
        cur.executemany("INSERT INTO job_perks (job_id, name, value) VALUES(?, ?, ?)", job_perks)
        cur.executemany("INSERT INTO job_salary (job_id, name, value) VALUES(?, ?, ?)", job_salary)
        # TODO: uncomment when done with debugging
        #cur.executemany("INSERT INTO job_overview (job_id, name, value) VALUES(?, ?, ?)", job_overview)
        conn.commit()
