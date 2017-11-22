import os


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