class DuplicateEntryError(Exception):
    """Raised when inserting a feed's entry with a primary key that is already
    in the database from a previous insertion."""


class FeedNotFoundError(Exception):
    """Raised when a given feed's name is not found in the database."""
