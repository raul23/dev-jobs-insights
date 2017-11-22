import ast
from configparser import ConfigParser, NoOptionError, NoSectionError
import json
import linecache
import os
import pickle
import sqlite3
import sys


def create_connection(db_path, autocommit=False):
    """
    Creates a database connection to the SQLite database specified by the db_file

    :param db_path: path to database file
    :param autocommit: TODO
    :return: sqlite3.Connection object  or None
    """
    # Check if db filename exists
    db_path = os.path.expanduser(db_path)
    if not check_file_exists(db_path):
        print("Database filename '{}' doesn't exist".format(db_path))
        return None
    try:
        if autocommit:
            conn = sqlite3.connect(db_path, isolation_level=None)
        else:
            conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error:
        print_exception()
    return None


def check_file_exists(path):
    """
    Checks if both a file exists and it is a file. Returns True if it is the
    case (can be a file or file symlink).

    ref.: http://stackabuse.com/python-check-if-a-file-or-directory-exists/

    :param path: path to check if it points to a file
    :return bool: True if it file exists and is a file. False otherwise.
    """
    path = os.path.expanduser(path)
    return os.path.isfile(path)


def check_dir_exists(path):
    """
    Checks if both a directory exists and it is a directory. Returns True if it
    is the case (can be a directory or directory symlink).

    ref.: http://stackabuse.com/python-check-if-a-file-or-directory-exists/

    :param path: path to check if it points to a directory
    :return bool: True if it directory exists and is a directory. False otherwise.
    """
    path = os.path.expanduser(path)
    return os.path.isdir(path)


def check_path_exists(path):
    """
    Checks if a path exists where path can either points to a file, directory,
    or symlink. Returns True if it is the case.

    ref.: http://stackabuse.com/python-check-if-a-file-or-directory-exists/

    :param path: path to check if it exists
    :return bool: True if it path exists. False otherwise.
    """
    path = os.path.expanduser(path)
    return os.path.exists(path)


def load_json(path):
    path = os.path.expanduser(path)
    try:
        with open(path, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        print_exception("FileNotFoundError")
        return None
    else:
        return data


def dump_json(data, path, update=False):
    path = os.path.expanduser(path)

    def dump_data(data, path):
        try:
            with open(path, "w") as f:
                json.dump(data, f)
        except FileNotFoundError:
            print_exception("FileNotFoundError")
            return None
        else:
            return 0
    if os.path.isfile(path) and update:
        retval = load_json(path)
        if retval is None:
            return None
        else:
            assert type(data) == dict, "Type of '{}' is not a dict".format(data)
            retval.update(data)
            return dump_data(retval, path)
    else:
        return dump_data(data, path)


def load_pickle(path):
    """
    Opens a pickle file and returns its contents or None if file not found.

    :param path: path to the pickle file
    :return: content of the pickle file or None if error
    """
    path = os.path.expanduser(path)
    try:
        with open(path, "rb") as f:
            data = pickle.load(f)
    except FileNotFoundError as e:
        print(e)
        return None
    return data


def dump_pickle(data, path):
    """
    Dumps a pickle file on disk and returns 0 if everything went right or None
    if file not found.

    :param path: path to the pickle file where data will be written
    :param data: data to be saved on disk
    :return: 0 if success or None if error
    """
    path = os.path.expanduser(path)
    try:
        with open(path, "wb") as f:
            pickle.dump(data, f)
    except FileNotFoundError as e:
        print(e)
        return None
    else:
        return 0


def get_data_type(val):
    """
    Given a string, returns its corresponding data type

    ref.: https://stackoverflow.com/a/10261229

    :param val: string value
    :return: Data type of string value
    """
    try:
        t = ast.literal_eval(val)
    except ValueError:
        return str
    except SyntaxError:
        return str
    else:
        if type(t) is bool:
            return bool
        elif type(t) is int:
            return int
        elif type(t) is float:
            return float
        else:
            return str


def get_option_value(parser, section, option):
    value_type = get_data_type(parser.get(section, option))
    try:
        if value_type == int:
            return parser.getint(section, option)
        elif value_type == float:
            return parser.getfloat(section, option)
        elif value_type == bool:
            return parser.getboolean(section, option)
        else:
            return parser.get(section, option)
    except NoSectionError:
        print_exception()
        return None
    except NoOptionError:
        print_exception()
        return None


def read_config(config_path):
    parser = ConfigParser()
    found = parser.read(config_path)
    if config_path not in found:
        print("ERROR: {} is empty".format(config_path))
        return None
    options = {}
    for section in parser.sections():
        options.setdefault(section, {})
        for option in parser.options(section):
            options[section].setdefault(option, None)
            value = get_option_value(parser, section, option)
            if value is None:
                print("ERROR: The option '{}' could not be retrieved from {}".format(option, config_path))
                return None
            options[section][option] = value
    return options


def print_exception(error=None):
    """
    For a given exception, PRINTS filename, line number, the line itself, and
    exception description.

    ref.: https://stackoverflow.com/a/20264059

    :return: None
    """
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    if error is None:
        err_desc = exc_obj
    else:
        err_desc = "{}: {}".format(error, exc_obj)
    # TODO: find a way to add the error description (e.g. AttributeError) without
    # having to provide the error description as input to the function
    print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), err_desc))


def filter_data(data, min_threshold, max_threshold):
    # Sanity check on input thresholds
    if not (data.max() >= min_threshold >= data.min()):
        min_threshold = data.min()
    if not (data.max() >= max_threshold >= data.min()):
        max_threshold = data.max()
    first_cond = data >= min_threshold
    second_cond = data <= max_threshold
    return np.where(first_cond & second_cond)