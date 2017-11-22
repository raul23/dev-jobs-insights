import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import plotly
from plotly.graph_objs import Scatter, Figure, Layout


def generate_bar_chart(plt_config):
    default_config = {"x": None,
                      "y": None,
                      "xlabel": "",
                      "ylabel": "",
                      "title": "",
                      "grid_which": "major",
                      "yaxis_major_mutiplelocator": 20,
                      "yaxis_minor_mutiplelocator": 10}
    # Sanity check on config dicts
    assert len(default_config) >= len(plt_config), \
        "generate_bar_chart(): plt_config has {} keys and default_config has {} keys"\
            .format(len(plt_config), len(default_config))
    default_config.update(plt_config)
    plt_config = default_config
    x = plt_config["x"]
    y = plt_config["y"]
    xlabel = plt_config["xlabel"]
    ylabel = plt_config["ylabel"]
    title = plt_config["title"]
    grid_which = plt_config["grid_which"]
    # Sanity check on the input arrays
    assert type(x) == type(np.array([])), "generate_bar_chart(): wrong type on input array 'x'"
    assert type(y) == type(np.array([])), "generate_bar_chart(): wrong type on input array 'y'"
    assert x.shape == y.shape, "generate_bar_chart(): wrong shape with 'x' and 'y'"
    assert grid_which in ["minor", "major", "both"], \
        "generate_bar_chart(): wrong value for grid_which='{}'".format(grid_which)
    ax = plt.gca()
    index = np.arange(len(x))
    plt.bar(index, y)
    plt.xticks(index, x)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    labels = ax.get_xticklabels()
    plt.setp(labels, rotation=270.)
    ax.yaxis.set_major_locator(ticker.MultipleLocator(plt_config["yaxis_major_mutiplelocator"]))
    ax.yaxis.set_minor_locator(ticker.MultipleLocator(plt_config["yaxis_minor_mutiplelocator"]))
    # plt.minorticks_on()
    plt.grid(which=grid_which)
    plt.tight_layout()
    plt.show()


def generate_histogram(plt_config):
        default_config = {"data": None,
                          #"bin_width": 10000, # TODO: not used
                          "bins": None,
                          "xlabel": "",
                          "ylabel": "",
                          "title": "",
                          "grid_which": "major",
                          "xaxis_major_mutiplelocator": 10000,
                          "xaxis_minor_mutiplelocator": 1000,
                          "yaxis_major_mutiplelocator": 5,
                          "yaxis_minor_mutiplelocator": 1
                          }
        # Sanity check on config dicts
        assert len(default_config) >= len(plt_config), \
            "plt_config has {} keys and default_config has {} keys"\
                .format(len(plt_config), len(default_config))
        default_config.update(plt_config)
        plt_config = default_config
        data = plt_config["data"]
        bins = plt_config["bins"]
        xlabel = plt_config["xlabel"]
        ylabel = plt_config["ylabel"]
        title = plt_config["title"]
        grid_which = plt_config["grid_which"]
        xaxis_major_mutiplelocator = plt_config["xaxis_major_mutiplelocator"]
        xaxis_minor_mutiplelocator = plt_config["xaxis_minor_mutiplelocator"]  # TODO: not used
        yaxis_major_mutiplelocator = plt_config["yaxis_major_mutiplelocator"]
        yaxis_minor_mutiplelocator = plt_config["yaxis_minor_mutiplelocator"]
        # Sanity check on the input array
        assert type(data) == type(np.array([])), "wrong type on input array 'data'"
        assert grid_which in ["minor", "major", "both"], \
            "wrong value for grid_which='{}'".format(grid_which)
        ax = plt.gca()
        ax.hist(data, bins=bins, color="r")
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.xaxis.set_major_locator(ticker.MultipleLocator(xaxis_major_mutiplelocator))
        ax.yaxis.set_major_locator(ticker.MultipleLocator(yaxis_major_mutiplelocator))
        ax.yaxis.set_minor_locator(ticker.MultipleLocator(yaxis_minor_mutiplelocator))
        plt.xlim(0, data.max())
        labels = ax.get_xticklabels()
        plt.setp(labels, rotation=270.)
        plt.grid(True, which="major")
        plt.tight_layout()
        # TODO: add function to save image instead of showing it
        plt.show()


def generate_pie_chart(plt_config):
        default_config = {"values": None,
                          "labels": None,
                          "title": ""}
        # Sanity check on config dicts
        assert len(default_config) >= len(plt_config), \
            "plt_config has {} keys and default_config has {} keys"\
                .format(len(plt_config), len(default_config))
        default_config.update(plt_config)
        values = plt_config["values"]
        labels = plt_config["labels"]
        title = plt_config["title"]
        # Sanity check on the input arrays
        assert isinstance(values, type(np.array([]))), "Wrong type on input array 'values'"
        assert isinstance(labels, type(np.array([]))), "Wrong type on input array 'labels'"
        assert values.shape == labels.shape, "Wrong shape with 'labels' and 'values'"
        ax = plt.gca()
        plt.pie(values, labels=labels, autopct="%1.1f%%")
        ax.set_title(title)
        plt.axis("equal")
        plt.show()


def generate_scatter_plot(plt_config):
        # TODO: add labels to axes
        default_config = {"x": None,
                          "y": None,
                          "mode": "markers",
                          "text": None,
                          "title": "",
                          "hovermode": "closest",
                          "yaxis_tickformat": "$0.0f"
                          }
        # Sanity check on config dicts
        assert len(default_config) >= len(plt_config), \
            "plt_config has {} keys and default_config has {} keys"\
                .format(len(plt_config), len(default_config))
        default_config.update(plt_config)
        plt_config = default_config
        x = plt_config["x"]
        y = plt_config["y"]
        mode = plt_config["mode"]
        text = plt_config["text"]
        title = plt_config["title"]
        hovermode = plt_config["hovermode"]
        yaxis_tickformat = plt_config["yaxis_tickformat"]
        assert type(x) == type(np.array([])), "wrong type on input array 'x'"
        assert type(y) == type(np.array([])), "wrong type on input array 'y'"
        assert type(text) == type(np.array([])), "wrong type on input array 'text'"
        plotly.offline.plot({
            "data": [Scatter(x=list(x.flatten()),
                             y=list(y.flatten()),
                             mode=mode,
                             text=list(text.flatten()))],
            "layout": Layout(title=title, hovermode=hovermode,
                             yaxis=dict(tickformat=yaxis_tickformat))
        })
