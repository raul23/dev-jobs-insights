import os
import sys
import time
# Third-party modules
import geopy
from geopy.geocoders import Nominatim
import ipdb
import numpy as np
from pycountry_convert import country_alpha2_to_continent_code, map_countries
# Own modules
from .analyzer import Analyzer
# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
from utility.genutil import add_plural, dump_pickle_with_logger, \
    get_geo_coords_with_logger, load_pickle, load_pickle_with_logger
from utility.script_boilerplate import LoggingBoilerplate


class JobLocationsAnalyzer(Analyzer):
    def __init__(self, conn, db_session, main_config, logging_config):
        # Locations stats to compute
        self.stats_names = [
            "sorted_all_countries_count", "sorted_eu_countries_count",
            "sorted_us_states_count"]
        super().__init__(conn, db_session, main_config, logging_config,
                         self.stats_names)
        # TODO: the logging boilerplate code should be done within the parent
        # class `Analyzer`
        sb = LoggingBoilerplate(
            module_name=__name__,
            module_file=__file__,
            cwd=os.getcwd(),
            logging_config=logging_config)
        self.logger = sb.get_logger()
        # String of alpha_2 european countries where each alpha_2 are separated
        # by ', ', e.g. "'GI', 'LU', 'PL', 'BE', 'RS', 'AX'"
        # NOTE: each alpha_2 must be within single quotes because the SQL
        # expression used in `_count_european_countries()` (that retrieves the
        # count of each european country) won't work, more specifically the `IN`
        # SQL operator will break
        # TODO: `european_countries` is used for method 4 in
        # `_count_european_countries()`, once the most efficient method will be
        # chosen, this variable will be removed if method 4 is not the chosen one
        self.european_countries = self._get_european_countries()
        self.addresses_geo_coords = load_pickle_with_logger(os.path.expanduser(
                self.main_config['data_filepaths']['cache_adr_geo_coords']),
                self.logger)
        self.locations_mappings = load_pickle_with_logger(os.path.expanduser(
            self.main_config['data_filepaths']['cache_loc_mappings']),
            self.logger)

    def run_analysis(self):
        # Reset all locations stats to be computed
        self.reset_stats()
        self.stats['locations_info'] = {}
        ###############################
        #    All countries analysis
        ###############################
        # Get counts of countries, i.e. for each country we want to know its
        # number of occurrences in job posts
        # NOTE: these are all the countries and they are sorted in order of
        # decreasing number of occurrences (i.e. most popular country at first)
        countries_count = self._count_all_countries()
        self.stats['sorted_countries_count'] = countries_count
        self.logger.debug(
            "There are {} distinct countries".format(len(countries_count)))
        self.logger.debug(
            "There are {} occurrences of countries in the job posts".format(
                sum(j for i, j in countries_count)))
        bar_config = self.main_config['graphs_config']['bar_chart_all_countries']
        """
        self._generate_bar_chart(
            sorted_stats_count=self.stats["sorted_all_countries_count"],
            bar_chart_config=bar_config)
        """
        ###############################
        #      US states analysis
        ###############################
        # Get counts of US states, i.e. for each US state we want to know its
        # number of occurrences in job posts
        # NOTE: These are all the US states and they are sorted in order of
        # decreasing number of occurrences (i.e. most popular US state at first)
        us_states_count = self._count_us_states()
        self.stats['sorted_us_states_count'] = us_states_count
        self.logger.debug(
            "There are {} distinct US states".format(len(us_states_count)))
        self.logger.debug(
            "There are {} occurrences of US states in the job posts".format(
                sum(j for i, j in us_states_count)))
        # TODO: Pycharm complains about using '==' but if I use 'is', the
        # `np.where` statement won't work
        indices = np.where(np.array(us_states_count) == None)
        if indices[0]:
            assert len(indices) == 2, "There should be 2 indices"
            self.logger.debug("There are {} 'None' US state".format(
                np.array(us_states_count)[indices[0]][0][1]))
        bar_config = self.main_config['graphs_config']['bar_chart_us_states']
        """
        self._generate_bar_chart(
            sorted_stats_count=self.stats['sorted_us_states_count'],
            bar_chart_config=bar_config)
        """
        ###############################
        #       European analysis
        ###############################
        # Get counts of european countries, i.e. for each country we want to
        # know its number of occurrences in job posts
        # NOTE: These are all the european countries and they are sorted in
        # order of decreasing number of occurrences (i.e. most popular european
        # country at first)
        eu_countries_count = self._count_european_countries()
        self.stats['sorted_eu_countries_count'] = eu_countries_count
        self.logger.debug(
            "There are {} distinct european countries".format(
                len(eu_countries_count)))
        self.logger.debug(
            "There are {} occurrences of european countries in the job "
            "posts".format(sum(j for i, j in eu_countries_count)))
        bar_config = self.main_config['graphs_config']['bar_chart_eu_countries']
        """
        self._generate_bar_chart(
            sorted_stats_count=self.stats['sorted_eu_countries_count'],
            bar_chart_config=bar_config)
        """
        ###############################
        #           Maps
        ###############################
        # Generate map with markers added on US states that have job posts
        # self._generate_usa_map()
        # Generate map with markers added on countries that have job posts
        self._generate_world_map()
        # Generate map with markers added on european countries that have job
        # posts
        # self._generate_europe_map()

    def _count_all_countries(self):
        """
        Returns countries sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (country, count).

        :return: list of tuples of the form (country, count)
        """
        sql = "SELECT country, COUNT(country) as CountOf FROM " \
              "job_locations GROUP BY country ORDER BY CountOf DESC"
        return self.db_session.execute(sql).fetchall()

    def _count_european_countries(self, method=4):
        """
        Returns european countries sorted in decreasing order of their
        occurrences in job posts. A list of tuples is returned where a tuple is
        of the form (country, count).

        :return: list of tuples of the form (country, count)
        """
        retval = []
        # TODO: use timeit to time each method and choose the quickest
        if method == 1:
            # TODO: 1st method: pure Python using `dict`
            pass
        elif method == 2:
            # TODO: 2nd method: numpy using ...
            pass
        elif method == 3:
            # TODO: 3rd method: pandas using ...
            pass
        else:
            # 4th method: SQL using `GROUP BY`
            sql = "SELECT country, COUNT(country) as CountOf FROM " \
                  "job_locations WHERE country in ({}) GROUP BY country ORDER " \
                  "BY CountOf DESC".format(self.european_countries)
            retval = self.db_session.execute(sql).fetchall()
        return retval

    def _count_us_states(self):
        """
        Returns US states sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (us_state, count).

        NOTE: in the SQL expression, the column region refers to a state

        :return: list of tuples of the form (us_state, ount)
        """
        sql = "SELECT region, COUNT(country) as CountOf FROM job_locations " \
              "WHERE country='US' GROUP BY region ORDER BY CountOf DESC"
        return self.db_session.execute(sql).fetchall()

    def _get_all_locations(self):
        """
        Returns all locations. A list of tuples is returned where a tuple is of
        the form (city, region, country, count).

        :return: list of tuples of the form (city, region, country, count)
        """
        sql = "SELECT city, region, country FROM job_locations"
        return self.db_session.execute(sql).fetchall()

    def _get_us_states(self):
        """
        Returns all US states. A list of tuples is returned where a tuple is of
        the form (city, region, country, count).

        :return: list of tuples of the form (city, region, country, count)
        """
        # TODO: concatenate the three columns (city, region, country) into a
        # single string, e.g. 'Colorado Springs, CO, US'. You might get also
        # `None` within the string since not all job locations have a city or a
        # region.
        sql = "SELECT city, region, country FROM job_locations WHERE country='US'"
        return self.db_session.execute(sql).fetchall()

    def _generate_bar_chart(self, sorted_locations_count, bar_chart_config):
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        self.logger.info("loading module 'utility.graphutil' ...")
        from utility.graphutil import draw_bar_chart
        self.logger.debug("finished loading module 'utility.graphutil'")
        self.logger.info(
            "Generating bar chart: {} vs Number of job posts ...".format(
                bar_chart_config['xlabel']))
        topk = bar_chart_config['topk']
        new_labels = self._shrink_labels(
            labels=sorted_locations_count[:topk, 0],
            max_length=bar_chart_config['max_xtick_label_length'])
        draw_bar_chart(
            x=np.array(new_labels),
            y=sorted_locations_count[:topk, 1].astype(np.int32),
            xlabel=bar_chart_config['xlabel'],
            ylabel=bar_chart_config['ylabel'],
            title=bar_chart_config['title'].format(topk),
            grid_which=bar_chart_config['grid_which'],
            fig_width=bar_chart_config['fig_width'],
            fig_height=bar_chart_config['fig_height'])

    def _generate_europe_map(self):
        pass

    def _generate_usa_map(self):
        map_cfg = self.main_config['maps_config']['usa_map']
        locations_geo_coords = self._get_locations_geo_coords(
            locations=self._get_us_states())
        # TODO: annotation is disabled because the names overlap
        """
        topk = map_cfg['annotation']['topk']
        topk_locations = sorted(locations_geo_coords.items(),
                                key=lambda x: x[1]['count'])[-topk:]
        topk_locations = dict(topk_locations).keys()
        """
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        self.logger.info("loading module 'utility.graphutil' ...")
        from utility.graphutil import draw_usa_map
        self.logger.debug("finished loading module 'utility.graphutil'")
        shape_filepath = os.path.expanduser(
            self.main_config['data_filepaths']['shape'])
        # TODO: explain why reversed US states is used
        us_states_filepath = os.path.expanduser(
            self.main_config['data_filepaths']['reversed_us_states'])
        draw_usa_map(
            locations_geo_coords=locations_geo_coords,
            shape_filepath=shape_filepath,
            us_states_filepath=us_states_filepath,
            title=map_cfg['title'],
            fig_width=map_cfg['fig_width'],
            fig_height=map_cfg['fig_height'],
            # annotate_locations=topk_locations,
            # annotation_cfg=map_cfg['annotation'],
            basemap_cfg=map_cfg['basemap'],
            map_coords_cfg=map_cfg['map_coordinates'],
            draw_parallels=map_cfg['draw_parallels'],
            draw_meridians=map_cfg['draw_meridians'])

    def _generate_world_map(self):
        map_cfg = self.main_config['maps_config']['world_map']
        locations_geo_coords = self._get_locations_geo_coords(
            locations=self._get_all_locations())
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        self.logger.info("loading module 'utility.graphutil' ...")
        from utility.graphutil import draw_world_map
        self.logger.debug("finished loading module 'utility.graphutil'")
        draw_world_map(
            locations_geo_coords=locations_geo_coords,
            title=map_cfg['title'],
            fig_width=map_cfg['fig_width'],
            fig_height=map_cfg['fig_height'],
            # annotate_locations=topk_locations,
            # annotation_cfg=map_cfg['annotation'],
            basemap_cfg=map_cfg['basemap'],
            map_coords_cfg=map_cfg['map_coordinates'],
            draw_coastlines=map_cfg['draw_coastlines'],
            draw_countries=map_cfg['draw_countries'],
            draw_map_boundary=map_cfg['draw_map_boundary'],
            draw_meridians=map_cfg['draw_meridians'],
            draw_parallels=map_cfg['draw_parallels'],
            draw_states=map_cfg['draw_states'],
            fill_continents=map_cfg['fill_continents'])

    # `locations` is a list of tuples where each tuple is of the form
    # (city, region, country)
    # e.g. [('Colorado Springs', 'CO', 'US'), ('New York', 'NY', 'US')]
    # NOTE: city or region can be `None`
    # TODO: check country is always present
    def _get_locations_geo_coords(self, locations, use_country_fallback=False):
        new_geo_coords = False
        # Current addresses' geographic coordinates. By current we mean the
        # actual session. Thus, this dictionary (and other similar dictionaries)
        # records anything that is needed as data for the current session
        # computations.
        # keys: addresses as given by the geocode service
        # values: dict
        #           key1: 'geo_coords'
        #           val1: `geopy.location.Location` object (storing the geo
        #                  coordinates)
        #           key2: 'count'
        #           val2: int, number of locations with the given geo coordinates
        #           key3: 'locations'
        #           val3: set, set of the location names having the given geo
        #                 coordinates
        cur_adrs_geo_coords = {}
        # Current locations mappings to addresses
        # keys: location name
        # values: addresses as given by the geocode service
        cur_loc_mappings = {}
        counts = 0
        # Skipped locations stats
        # TODO: explain fields of dict
        skipped_locs = {'empty_locations': 0,
                        'already_added': [],
                        'first_try_geopy_error': [],
                        'second_try_geopy_error': [],
                        'second_try_geopy_none': []}
        # Waiting time in seconds between requests to geocode service
        wait_time = self.main_config['maps_config']['wait_time']
        # Get the location's longitude and latitude
        # We are using the module geopy to get the longitude and latitude of
        # locations which will then be transformed into map coordinates so we
        # can draw markers on a map with Basemap
        # NOTE: using the default `user_agent` "geopy/1.16.0" results in a
        # `UserWarning` that recommends using a custom `user_agent`
        # TODO: if `timeout` is used, do we still have to wait between requests?
        # If `timeout` is not used, I get the dreaded error message:
        # geopy.exc.GeocoderTimedOut: Service timed out
        # ref.: https://stackoverflow.com/a/27914845
        geolocator = Nominatim(user_agent="my-application", timeout=10)
        self.logger.info(
            "Requesting geographic coordinates for {} locations ...".format(
                len(locations)))
        # TODO: add a progress bar
        ipdb.set_trace()
        filepath = os.path.expanduser("~/data/dev_jobs_insights/cache/locations_geo_coords.pkl")
        locations_geo_coords = load_pickle(filepath)
        for i, (city, region, country) in enumerate(locations, start=1):

            # Build location string from list of strings (city, region, country)
            def get_location(list_of_str):
                loc = ""
                for i, s in enumerate(list_of_str, start=1):
                    if s is None:
                        continue
                    loc += "{}, ".format(s)
                return loc.strip(", ")

            location = get_location([city, region, country])
            self.logger.info("Location #{}: {}".format(i, location))
            if not location:
                # We ignore the case where the location is empty
                # NOTE: This case shouldn't happen because all job locations
                # have at least a country
                self.logger.warning("The location is empty")
                skipped_locs['empty_locations'] += 1
                continue
            elif location in cur_loc_mappings:
                self.logger.debug(
                    "Location '{}' was already added!".format(location))
                skipped_locs['already_added'].append(location)
                address = self.locations_mappings.get(location)
                cur_adrs_geo_coords[address]['count'] += 1
                counts += 1
                continue
            elif location in self.locations_mappings:
                # We already computed the location's latitude and longitude with
                # the geopy service
                address = self.locations_mappings.get(location)
                geo_coords = self.addresses_geo_coords[address]
                self.logger.debug(
                    "Location '{}' found in cache! Geo coordinates: {}".format(
                        location, geo_coords.point))
            else:
                try:
                    """
                    geo_coords = get_geo_coords_with_logger(
                        geolocator, location, self.logger)
                    """
                    geo_coords = locations_geo_coords.get(location)
                except (geopy.exc.GeocoderTimedOut,
                        geopy.exc.GeocoderServiceError):
                    skipped_locs['first_try_geopy_error'].append(location)
                    # TODO: test this part
                    ipdb.set_trace()
                    continue
                if geo_coords is None and use_country_fallback:
                    # TODO: test this part
                    ipdb.set_trace()
                    # The geopy service could not provide the geo coordinates.
                    # Use the country instead of the region
                    # IMPORTANT: we assume that `location` is a region
                    self.logger.warning(
                        "The geopy could not provide the geo coordinates for "
                        "the location '{}'. We will use the country '{}' only in "
                        "the second request to geopy".format(location, country))
                    self.logger.debug(
                        "Waiting {} second{} for the next geopy request "
                        "...".format(wait_time, add_plural(wait_time)))
                    time.sleep(wait_time)
                    try:
                        """
                        geo_coords = get_geo_coords_with_logger(
                            geolocator, country, self.logger)
                        """
                        geo_coords = locations_geo_coords.get(location)
                    except (geopy.exc.GeocoderTimedOut,
                            geopy.exc.GeocoderServiceError) as e:
                        skipped_locs['second_try_geopy_error'].append(location)
                        continue
                    if geo_coords is None:
                        self.logger.error(
                            "The geopy service could not for a second try"
                            "provide the geo coordinates this time using only "
                            "with the country '{}'".format(country))
                        self.logger.critical(
                            "The location '{}' will be skipped".format(location))
                        skipped_locs['second_try_geopy_none'].append(location)
                        continue
                    else:
                        self.logger.debug(
                            "Geo coordinates of '{}': {} lat, {} long "
                            "[{}]".format(
                                geo_coords.address, geo_coords.latitude,
                                geo_coords.longitude, geo_coords.point))
                self.logger.debug(
                    "Waiting {} second{} for the next geopy request "
                    "...".format(wait_time, add_plural(wait_time)))
                time.sleep(wait_time)
                new_geo_coords = True
                # Update the cached dict with the geo coords
                self.logger.debug(
                    "Dictionaries updated!")
                self.addresses_geo_coords.setdefault(
                    geo_coords.address, geo_coords)
                self.locations_mappings.setdefault(location, geo_coords.address)
            self.logger.debug(
                "Location '{}' [{}] added!".format(location, geo_coords.point))
            cur_adrs_geo_coords.setdefault(geo_coords.address, {})
            cur_adrs_geo_coords[geo_coords.address].setdefault('geo_coords',
                                                               geo_coords)
            cur_adrs_geo_coords[geo_coords.address].setdefault('count', 0)
            cur_adrs_geo_coords[geo_coords.address]['count'] += 1
            cur_adrs_geo_coords[geo_coords.address].setdefault('locations', set())
            cur_adrs_geo_coords[geo_coords.address]['locations'].add(location)
            cur_loc_mappings.setdefault(location, geo_coords.address)
            counts += 1
        ipdb.set_trace()
        # Sanity check
        n_locs = len(skipped_locs['already_added']) + len(cur_loc_mappings)
        assert n_locs == counts, \
            "Inconsistency in the number of valid locations and the count of " \
            "geo coordinates"
        self.logger.info("Finished collecting all geo coordinates")
        self.logger.info("***** Report *****")
        self.logger.info("# of successfully added locations: {}".format(
                len(cur_adrs_geo_coords)))
        self.logger.info("# of empty locations: {}".format(
            skipped_locs['empty_locations']))
        self.logger.info("# of duplicated locations: {}".format(
                len(skipped_locs['already_added'])))
        self.logger.info(
            "# of skipped locations with first-try-geopy-error: {}".format(
                len(skipped_locs['first_try_geopy_error'])))
        self.logger.info(
            "# of skipped locations with second-try-geopy-error: {}".format(
                len(skipped_locs['second_try_geopy_error'])))
        self.logger.info(
            "# of skipped locations with first-try-geopy-none: {}".format(
                len(skipped_locs['second_try_geopy_none'])))
        self.logger.info("********************")
        if new_geo_coords:
            # Save the geo coords
            self.logger.info("Saving the geographic coordinates ...")
            dump_pickle_with_logger(
                os.path.expanduser(
                    self.main_config['data_filepaths']['cache_adr_geo_coords']),
                self.addresses_geo_coords,
                self.logger)
            # Save the locations mappings
            self.logger.info("Saving the locations mappings ...")
            dump_pickle_with_logger(
                os.path.expanduser(
                    self.main_config['data_filepaths']['cache_loc_mappings']),
                self.locations_mappings,
                self.logger)
        return cur_adrs_geo_coords, cur_loc_mappings

    @staticmethod
    def _get_european_countries():
        european_countries = set()
        for _, values in map_countries().items():
            try:
                continent = country_alpha2_to_continent_code(values['alpha_2'])
                if continent == "EU":
                    european_countries.add(values['alpha_2'])
            except KeyError:
                # Possible cause: Invalid alpha_2, e.g. could be Antarctica which
                # is not associated to a continent code
                continue
        european_countries = ", ".join(
            map(lambda a: "'{}'".format(a), european_countries))
        return european_countries
