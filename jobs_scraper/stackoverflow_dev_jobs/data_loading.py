import json
import os
import pickle
import re
import sqlite3

from forex_python.converter import convert, get_symbol
import ipdb


DB_FILENAME = os.path.expanduser("~/databases/jobs_insights.sqlite")
CURRENCIES_FILENAME = os.path.expanduser("~/databases/currencies.json")
CURRENCY_DATA = None


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
    # e.g. the currency symbol £ is used for EGP, FKP, GDP, GIP, LBP, and SHP
    #
    # Sanity check for CURRENCY_DATA
    assert CURRENCY_DATA is not None, "CURRENCY_DATA is None; not loaded with data."
    results = [item for item in CURRENCY_DATA if item["symbol"] == currency_symbol]
    if len(results) == 1:
        # Found only one currency code associated with the given currency symbol
        return results[0]
    else:
        # Two possible cases
        # 1. Too many currency codes associated with the given currency symbol
        # 2. It is not a currency symbol
        if currency_symbol == "A$":  # Australian dollar
            currency_code = "AUD"
        elif currency_symbol == "C$":  # Canadian dollar
            currency_code = "CAD"
        else:
            print("WARNING: Could not get a currency code from {}".format(currency_symbol))
            return None
        return currency_code


def convert_currency(amount, target_currency="USD"):
    ipdb.set_trace()
    if "Equity" in amount:
        return None
    prefix_currency = re.search('^\D+', amount).group()
    if get_symbol(prefix_currency) is None:
        # `prefix_currency` is not a valid currency code. It might be a currency symbol
        #
        # Get currency code from possible currency symbol
        currency_code = get_currency_code(prefix_currency)
        if currency_code is None:
            return None
    else:
        currency_code = prefix_currency
    converted_amount = convert(currency_code, target_currency, amount)
    return converted_amount


def append_items(prefix_item, input_items, output_items):
    for name, values in input_items.items():
        if type(values) is not list:
            values = [values]
        for val in values:
            for v in val.split(","):
                if name in ["company_size", "salary"]:
                    v = replace_letter(v)
                if name == "salary":
                    if not v.startswith("$"):
                        convert_currency(v)
                output_items.append((prefix_item, name, v))


if __name__ == '__main__':
    ipdb.set_trace()
    with open(CURRENCIES_FILENAME) as f:
        CURRENCIES_DATA = json.loads(f.read())

    conn = create_connection(DB_FILENAME)
    with conn:
        f = open("entries_data.pkl", "rb")
        data = pickle.load(f)
        f.close()

        job_posts = []
        job_perks = []
        job_overview = []

        for k, v in data.items():
            id = k
            author = v['author']
            link = v['link']
            location = v['location']
            job_posts.append((id, author, link, location))
            perks = v['perks']
            overview_items = v['overview_items']
            append_items(prefix_item=id, input_items=perks, output_items=job_perks)
            append_items(prefix_item=id, input_items=overview_items, output_items=job_overview)

        ipdb.set_trace()
        cur = conn.cursor()
        cur.executemany("INSERT INTO job_posts VALUES(?, ?, ?, ?)", job_posts)
        cur.executemany("INSERT INTO job_perks (job_id, name, value) VALUES(?, ?, ?)", job_perks)
        cur.executemany("INSERT INTO job_overview (job_id, name, value) VALUES(?, ?, ?)", job_overview)
        conn.commit()
