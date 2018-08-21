class CurrencyConversionError(Exception):
    """Raised when there is an error in converting a currency when using
    forex_python.converter.get_rate"""


class SameCurrencyError(Exception):
    """Raised when the specified base and destination currencies are the same,
    i.e. an amount is being converted to the same currency it is already"""


class NoCurrencySymbolError(Exception):
    """Raised when a currency symbol couldn't be found at the start of a salary
     range, e.g. '42k - 75k' doesn't have any currency symbol (e.g. $) at
     the beginning."""


class SameComputationError(Exception):
    """Raised when the same computation will be performed that was already made,
    e.g. an amount is being converted again to a specified currency"""