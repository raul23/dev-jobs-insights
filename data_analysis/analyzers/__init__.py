import os
import sys
try:
    from utility import genutil as util
except ImportError:
    sys.path.insert(0, os.path.expanduser("~/PycharmProjects/tools"))
    from utility import genutil as util