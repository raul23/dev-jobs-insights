import json
import os
import pickle
import re
import sqlite3

from forex_python.converter import convert, get_symbol, RatesNotAvailableError
import ipdb
import requests


DB_FILENAME = os.path.expanduser("~/databases/jobs_insights.sqlite")
CURRENCY_FILENAME = os.path.expanduser("~/databases/currencies.json")
CURRENCY_DATA = None
DEST_CURRENCY = "USD"
DEST_SYMBOL = "$"


# TODO: utility function
def create_connection(db_file, autocommit=False):
    """
    Creates a database connection to the SQLite database specified by the db_file

    :param db_file: database file
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


def replace_letter(string):
    regex = r"(?P<number>\d)k"
    subst = "\g<number>000"
    new_string = re.sub(regex, subst, string, 0)
    return new_string


def get_currency_code(currency_symbol):
    # NOTE: there is a not 1-to-1 mapping when going from currency symbols
    # to currency code
    # e.g. the currency symbol £ is used for the currency codes EGP, FKP, GDP,
    # GIP, LBP, and SHP
    #
    # Sanity check for CURRENCY_DATA
    assert CURRENCY_DATA is not None, "CURRENCY_DATA is None; not loaded with data."
    results = [item for item in CURRENCY_DATA if item["symbol"] == currency_symbol]
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


if __name__ == '__main__':
    ipdb.set_trace()
    with open(CURRENCY_FILENAME) as f:
        CURRENCY_DATA = json.loads(f.read())

    conn = create_connection(DB_FILENAME)
    with conn:
        f = open("entries_data.pkl", "rb")
        data = pickle.load(f)
        f.close()

        job_posts = []
        job_perks = []
        job_salary = []
        job_overview = []

        count = 1
        for k, v in data.items():
            print(count)
            count += 1
            id = k
            author = v['author']
            link = v['link']
            # `location` can have two locations in one separated by ;
            # e.g. Teunz, Germany; Kastl, Germany
            location = v['location']
            job_posts.append((id, author, link, location))
            perks = v['perks']
            overview_items = v['overview_items']
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

"""
$120k - 130k
A$100k - 130k
C$100k - 170k
CHF 84k - 108k
Equity
NZD 80k - 100k
RM72k - 96k
zł13k - 19k
£15k - 25k
€30k - 49k
₹1000k - 3000k
"""