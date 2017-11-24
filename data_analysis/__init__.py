import sys
try:
    import analyzers
except ImportError:
    sys.path.append("data_analysis")
    import analyzers
