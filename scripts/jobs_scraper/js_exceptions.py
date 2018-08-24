class CurrencyRateError(Exception):
    """Raised when there is any error when calling the method get_rate()
    from forex_python.converter.CurrencyRates, e.g. RatesNotAvailableError or
    requests.exceptions.ConnectionError"""


class NoneBaseCurrencyError(Exception):
    """Raised when the specified base currency is `None`"""


class NoCurrencySymbolError(Exception):
    """Raised when a currency symbol couldn't be found at the start of a salary
     range, e.g. '42k - 75k' doesn't have any currency symbol (e.g. $) at
     the beginning."""


class SameCurrencyError(Exception):
    """Raised when the specified base and destination currencies are the same,
    i.e. an amount is being converted to the same currency it is already"""


class SameComputationError(Exception):
    """Raised when the same computation will be performed that was already made,
    e.g. an amount is being converted again to a specified currency"""


class TagNotFoundError(Exception):
    """Raised when an HTML tag is not found in the HTML document"""


class EmptyTextError(Exception):
    """Raised when an HTML tag doesn't contain any text"""


class HTTP404Error(Exception):
    """Raised when the server returns a 404 status code because the page is
    not found."""


class WebPageNotFoundError(Exception):
    """Raised when the webpage HTML could not be retrieved for any reasons,
    e.g. 404 error, or OSError."""


class WebPageSavingError(Exception):
    """Raised when the webpage HTML couldn't be saved locally, e.g. the caching
    option is disabled."""


class EmptyQueryResultSetError(Exception):
    """Raised when a SQL query returns an empty result set, i.e. no rows."""


class NoJobLocationError(Exception):
    """Raised when no job location is found in the JSON linkded data."""
