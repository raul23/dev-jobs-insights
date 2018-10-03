import os
import sys
import time
# Third-party modules
import geopy
from geopy.geocoders import Nominatim
import ipdb
import numpy as np
from pycountry_convert import country_alpha2_to_country_name
# Own modules
from .analyzer import Analyzer
from utility.genutil import add_plural


class JobLocationsAnalyzer(Analyzer):
    def __init__(self, analysis_type, conn, db_session, main_cfg, logging_cfg):
        # Locations stats to compute
        # TODO: store numpy arrays instead and check other modules also
        # so you won't have to convert list as a numpy array when generating the
        # horizontal bar chart
        self.stats_names = [
            "sorted_all_countries_count", "sorted_eu_countries_count",
            "sorted_us_states_count"]
        super().__init__(analysis_type,
                         conn,
                         db_session,
                         main_cfg,
                         logging_cfg,
                         self.stats_names,
                         __name__,
                         __file__,
                         os.getcwd())
        # Load data from pickle and JSON files
        # IMPORTANT: if the pickle is not found, an empty dict is used instead
        self.addresses_geo_coords = self._load_dict_from_pickle(
            os.path.expanduser(
                self.main_cfg['data_filepaths']['cache_adr_geo_coords']))
        self.locations_mappings = self._load_dict_from_pickle(
            os.path.expanduser(
                self.main_cfg['data_filepaths']['cache_loc_mappings']))
        # If the JSON is not found, an exception is triggered
        self.us_states = self._load_json(os.path.expanduser(
            self.main_cfg['data_filepaths']['us_states']))

    def run_analysis(self):
        # Reset all locations stats to be computed
        self.reset_stats()
        ###############################
        #       European analysis
        ###############################
        # Get counts of european countries, i.e. for each country we want to
        # know its number of occurrences in job posts
        # NOTE: These are all the european countries and they are sorted in
        # order of decreasing number of occurrences (i.e. most popular european
        # country at first)
        # TODO: even if `display_graph` and `save_graph` are set to False,
        # `_count_european_countries()` gets called
        barh_cfg = self.main_cfg['job_locations']['barh_chart_europe']
        eu_countries_count = self._count_european_countries(
            use_fullnames=barh_cfg['use_fullnames'])
        self.stats['sorted_eu_countries_count'] = eu_countries_count
        self.logger.debug(
            "There are {} distinct european countries".format(
                len(eu_countries_count)))
        self.logger.debug(
            "There are {} occurrences of european countries in the job "
            "posts".format(sum(j for i, j in eu_countries_count)))
        self._generate_barh_chart(
            barh_type='barh_chart_europe',
            sorted_topic_count=np.array(self.stats['sorted_eu_countries_count']),
            barh_chart_cfg=barh_cfg)
        ###############################
        #      US states analysis
        ###############################
        # Get counts of US states, i.e. for each US state we want to know its
        # number of occurrences in job posts
        # NOTE: These are all the US states and they are sorted in order of
        # decreasing number of occurrences (i.e. most popular US state at first)
        barh_cfg = self.main_cfg['job_locations']['barh_chart_usa']
        us_states_count = self._count_us_states(
            use_fullnames=barh_cfg['use_fullnames'],
            ignore_none=barh_cfg['ignore_none'])
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
        self._generate_barh_chart(
            barh_type='barh_chart_usa',
            sorted_topic_count=np.array(self.stats['sorted_us_states_count']),
            barh_chart_cfg=barh_cfg)
        ###############################
        #       World analysis
        ###############################
        # Get counts of countries, i.e. for each country we want to know its
        # number of occurrences in job posts
        # NOTE: these are all the countries and they are sorted in order of
        # decreasing number of occurrences (i.e. most popular country at first)
        barh_cfg = self.main_cfg['job_locations']['barh_chart_world']
        countries_count = self._count_all_countries(
            use_fullnames=barh_cfg['use_fullnames']
        )
        self.stats['sorted_all_countries_count'] = countries_count
        self.logger.debug(
            "There are {} distinct countries".format(len(countries_count)))
        self.logger.debug(
            "There are {} occurrences of countries in the job posts".format(
                sum(j for i, j in countries_count)))
        self._generate_barh_chart(
            barh_type='barh_chart_world',
            sorted_topic_count=np.array(self.stats["sorted_all_countries_count"]),
            barh_chart_cfg=barh_cfg)
        ###############################
        #           Maps
        ###############################
        # Generate map with markers added on US states that have job posts
        self._generate_map_usa(
            map_type='map_usa',
            map_cfg=self.main_cfg['job_locations']['map_usa'])
        # Generate map with markers added on countries that have job posts
        self._generate_map_world(
            map_type='map_world',
            map_cfg=self.main_cfg['job_locations']['map_world'])
        # Generate map with markers added on european countries that have job
        # posts
        # TODO: implement `_generate_map_europe()`
        # self._generate_map_europe()

    def _count_all_countries(self, use_fullnames=False):
        """
        Returns countries sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (country, count).

        :return: list of tuples of the form (country, count)
        """
        # IMPORTANT: 'No office location' is not ignored if `use_fullnames=False`
        sql = "SELECT country, COUNT(country) as CountOf FROM " \
              "job_locations GROUP BY country ORDER BY CountOf DESC"
        results = self.db_session.execute(sql).fetchall()
        if use_fullnames:
            # IMPORTANT: 'No office location' will be ignored
            # Convert country names' alpha2 to their full country names
            list_countries_names = \
                self._list_country_alpha2_to_country_name(results)
            diff = len(results) - len(list_countries_names)
            if diff > 0:
                self.logger.warning("{} countr{} missing".format(
                                     diff, add_plural(diff, "ies are", "y is")))
            results = list_countries_names
        return results

    def _count_european_countries(self, method=1, use_fullnames=False):
        """
        Returns european countries sorted in decreasing order of their
        occurrences in job posts. A list of tuples is returned where a tuple is
        of the form (country, count).

        :return: list of tuples of the form (country, count)
        """
        # TODO: use timeit to time each method and choose the quickest
        if method == 1:
            # 4th method: SQL using `GROUP BY`
            sql = "SELECT country, COUNT(country) as CountOf FROM " \
                  "job_locations WHERE country in ({}) GROUP BY country ORDER " \
                  "BY CountOf DESC".format(self._get_european_countries_as_str())
            results = self.db_session.execute(sql).fetchall()
        elif method == 2:
            # TODO: pure Python using `dict`
            raise NotImplementedError("Method 2 (pure python using dict) not "
                                      "implemented!")
        elif method == 3:
            # TODO: numpy using ...
            raise NotImplementedError("Method 3 (numpy) not implemented!")
        else:
            # TODO: pandas using ...
            raise NotImplementedError("Method 4 (pandas) not implemented!")
        if use_fullnames:
            self.logger.debug("The resulset '{}' will be processed "
                              "(use fullnames)".format(results))
            # Convert country names' alpha2 to their full country names
            list_countries_names = \
                self._list_country_alpha2_to_country_name(results)
            assert len(results) == len(list_countries_names), \
                "Some countries are missing"
            results = list_countries_names
        self.logger.debug("Returned resulset: {}".format(results))
        return results

    def _count_us_states(self, use_fullnames=False, ignore_none=False):
        """
        Returns US states sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (us_state, count).

        NOTE: in the SQL expression, the column region refers to a state

        :return: list of tuples of the form (us_state, count)
        """
        # NOTE: if you use `COUNT(region)`, the 'None' region is not counted,
        # i.e. you get '(None, 0)' instead of '(None, 22)'
        sql = "SELECT region, COUNT(country) as CountOf FROM job_locations " \
              "WHERE country='US' GROUP BY region ORDER BY CountOf DESC"
        results = self.db_session.execute(sql).fetchall()
        if not use_fullnames and not ignore_none:
            self.logger.debug("Resultset: {}".format(results))
            return results
        if use_fullnames or ignore_none:
            self.logger.debug("The resulset '{}' will be processed (use fullnames "
                              "or ignore 'None') ...".format(results))
            temp = []
            for short_name, count in results:
                if short_name is None and ignore_none:
                    self.logger.debug("The 'None' country will be skipped")
                    continue
                if use_fullnames:
                    if short_name is None:
                        full_name = None
                    else:
                        full_name = self.us_states[short_name]
                    temp.append((full_name, count))
                else:
                    temp.append((short_name, count))
            results = temp
        self.logger.debug("Returned resultset: {}".format(results))
        return results

    def _generate_map_europe(self):
        raise NotImplementedError

    # TODO: get the `map_type` from the name of the function
    def _generate_map_usa(self, map_type, map_cfg):
        if not map_cfg['display_graph'] and not map_cfg['save_graph']:
            self.logger.warning("The map '{}' is disabled for the '{}' "
                                "analysis".format(map_type, self.analysis_type))
            return 1
        addresses_data, _ = self._get_locations_geo_coords(
            locations=self._get_us_states(),
            fallbacks=['region+country', 'country'])
        # TODO: annotation is disabled because the names overlap
        """ 
        # IMPORTANT: Old code using the old dict structure with locations 
        # instead of addresses as keys
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
        self.logger.info("Generating map '{}' ...".format(map_type))
        shape_filepath = os.path.expanduser(
            self.main_cfg['data_filepaths']['shape'])
        # TODO: explain why reversed US states is used
        us_states_filepath = os.path.expanduser(
            self.main_cfg['data_filepaths']['reversed_us_states'])
        draw_usa_map(
            addresses_data=addresses_data,
            shape_filepath=shape_filepath,
            us_states_filepath=us_states_filepath,
            title=map_cfg['title'],
            fig_width=map_cfg['fig_width'],
            fig_height=map_cfg['fig_height'],
            # annotate_addresses=topk_addresses,
            # annotation_cfg=map_cfg['annotation'],
            basemap_cfg=map_cfg['basemap'],
            map_coords_cfg=map_cfg['map_coordinates'],
            draw_parallels=map_cfg['draw_parallels'],
            draw_meridians=map_cfg['draw_meridians'],
            display_graph=map_cfg['display_graph'],
            save_graph=map_cfg['save_graph'],
            fname=os.path.join(self.main_cfg['saving_dirpath'],
                               map_cfg['fname']))

    def _generate_map_world(self, map_type, map_cfg):
        if not map_cfg['display_graph'] and not map_cfg['save_graph']:
            self.logger.warning("The map '{}' is disabled for the '{}' "
                                "analysis".format(map_type, self.analysis_type))
            return 1
        addresses_data, _ = self._get_locations_geo_coords(
            locations=self._get_all_locations(),
            fallbacks=['region+country', 'country'])
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        self.logger.info("loading module 'utility.graphutil' ...")
        from utility.graphutil import draw_world_map
        self.logger.debug("finished loading module 'utility.graphutil'")
        self.logger.info("Generating map '{}' ...".format(map_type))
        draw_world_map(
            addresses_data=addresses_data,
            title=map_cfg['title'],
            fig_width=map_cfg['fig_width'],
            fig_height=map_cfg['fig_height'],
            # annotate_addresses=topk_addresses,
            # annotation_cfg=map_cfg['annotation'],
            basemap_cfg=map_cfg['basemap'],
            map_coords_cfg=map_cfg['map_coordinates'],
            draw_coastlines=map_cfg['draw_coastlines'],
            draw_countries=map_cfg['draw_countries'],
            draw_map_boundary=map_cfg['draw_map_boundary'],
            draw_meridians=map_cfg['draw_meridians'],
            draw_parallels=map_cfg['draw_parallels'],
            draw_states=map_cfg['draw_states'],
            fill_continents=map_cfg['fill_continents'],
            display_graph=map_cfg['display_graph'],
            save_graph=map_cfg['save_graph'],
            fname=os.path.join(self.main_cfg['saving_dirpath'],
                               map_cfg['fname']))

    # TODO: add `ignore_no_office_location` option in main_cfg.yaml
    def _get_all_locations(self, ignore_no_office_location=True):
        """
        Returns all locations. A list of tuples is returned where a tuple is of
        the form (job_post_id, city, region, country).

        :return: list of tuples of the form
                 (job_post_id, city, region, country)
        """
        if ignore_no_office_location:
            where = " WHERE country!='No office location'"
        else:
            where = ""
        sql = "SELECT job_post_id, city, region, country FROM " \
              "job_locations{}".format(where)
        return self.db_session.execute(sql).fetchall()

    def _get_us_states(self):
        """
        Returns all US states. A list of tuples is returned where a tuple is of
        the form (job_post_id, city, region, country, count).

        :return: list of tuples of the form
                 (job_post_id, city, region, country, count)
        """
        # TODO: concatenate the three columns (city, region, country) into a
        # single string, e.g. 'Colorado Springs, CO, US'. You might get also
        # `None` within the string since not all job locations have a city or a
        # region. If you find out how to to concatenate the three columns, then
        # `get_location()` won't be needed within `_get_locations_geo_coords()`
        sql = "SELECT job_post_id, city, region, country FROM job_locations " \
              "WHERE country='US'"
        return self.db_session.execute(sql).fetchall()

    def _get_geo_coords(self, geolocator, location):
        try:
            self.logger.debug("Sending request to the geocoding service for "
                              "location '{}'".format(location))
            geo_coords = geolocator.geocode(location)
        except (geopy.exc.GeocoderTimedOut,
                geopy.exc.GeocoderServiceError) as exception:
            self.logger.exception(exception)
            self.logger.critical(
                "The location '{}' will be skipped".format(location))
            raise exception
        else:
            if geo_coords is None:
                self.logger.warning(
                    "The geo coordinates for '{}' are 'None'".format(location))
            else:
                self.logger.debug(
                    "Geo coordinates received from the geocoding service")
                self.logger.debug("Address: {}".format(geo_coords.address))
                self.logger.debug("Geo coordinates: {} lat, {} long [{}]".format(
                    geo_coords.latitude, geo_coords.longitude, geo_coords.point))
            return geo_coords

    def _get_location_from_fallback(self, city, region, country, location,
                                    fallback):
        self.logger.warning("The fallback is '{}'".format(fallback))
        if fallback == 'country':
            # Use the country only
            if city is None and region is None:
                self.logger.warning(
                    "The city and region are 'None'. Thus, this fallback will be "
                    "skipped since it corresponds to the first attempt "
                    "('{}')".format(location))
                return None
            self.logger.warning(
                "The geocoding service could not provide the geo coordinates for "
                "the location '{}'. We will use only the country '{}' in the next "
                "request to geopy".format(location, country))
            return country
        elif fallback == 'region+country':
            # Use the region and country
            if city is None and region is not None:
                self.logger.warning(
                    "The city is 'None' and the region is '{}'. Thus, this "
                    "fallback will be skipped since it corresponds to the first "
                    "attempt ('{}').".format(region, location))
                return None
            if region is None:
                self.logger.warning(
                    "The region is 'None'. Thus, this fallback will be skipped.")
                return None
            new_location = "{}, {}".format(region, country)
            self.logger.warning(
                "The geocoding service could not provide the geo coordinates for "
                "the location '{}'. We will use the region and country '{}' in "
                "the next request to geopy".format(location, new_location))
            return new_location
        else:
            self.logger.warning(
                "The fallback is 'None'. Thus, it will be skipped")
            return None

    # `locations` is a list of tuples where each tuple is of the form
    # (city, region, country)
    # e.g. [('Colorado Springs', 'CO', 'US'), ('New York', 'NY', 'US')]
    # NOTE: city or region can be `None`
    # TODO: check country is always present
    # `fallbacks` is a list with choices: 'region+country', 'country', None
    # e.g. fallbacks=['region+country', 'country']
    def _get_locations_geo_coords(self, locations, fallbacks=None):
        if fallbacks is None:
            fallbacks = []
        new_geo_coords = False
        # `cur_adrs_data`: current addresses' geographic coordinates. By current
        # we mean the actual session. Thus, this dictionary (and other similar
        # dictionaries) records anything that is needed as data for the current
        # session computations.
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
        cur_adrs_data = {}
        # Current locations mappings to addresses
        # keys: location name
        # values: addresses as given by the geocode service
        cur_loc_mappings = {}
        counts = 0
        # Skipped locations stats
        # TODO: explain fields of dict
        report = {'empty_locations': 0,
                  'already_added': [],
                  'similar_locations': {},
                  'first_try_geocoder_error': set(),
                  'first_try_geocoder_none': set(),
                  'next_try_geocoder_error': set(),
                  'next_try_geocoder_none': set()}
        # Waiting time in seconds between requests to geocoding service
        wait_time = self.main_cfg['job_locations']['wait_time']
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
        # TODO: testing code to be removed
        # filepath = os.path.expanduser("~/data/dev_jobs_insights/cache/locations_geo_coords.pkl")
        # locations_geo_coords = load_pickle(filepath)
        for i, (job_post_id, city, region, country) in \
                enumerate(locations, start=1):
            location = build_location([city, region, country])
            self.logger.info("Location #{}: {} (job_post_id={})".format(
                              i, location, job_post_id))
            if not location:
                # We ignore the case where the location is empty
                # NOTE: This case shouldn't happen because all job locations
                # have at least a country
                self.logger.warning("The location is empty")
                report['empty_locations'] += 1
                continue
            elif location in cur_loc_mappings:
                # Location already added
                report['already_added'].append(location)
                address = self.locations_mappings.get(location)
                cur_adrs_data[address]['count'] += 1
                counts += 1
                self.logger.debug(
                    "Location '{}' was already added!".format(location))
                self.logger.debug("Address '{}'".format(address))
                continue
            elif location in self.locations_mappings:
                # We already computed the location's latitude and longitude with
                # the geocoding service
                address = self.locations_mappings.get(location)
                geo_coords = self.addresses_geo_coords[address]
                self.logger.debug(
                    "Location '{}' found in cache!".format(location))
                self.logger.debug("Geo coordinates: {}".format(geo_coords.point))
                self.logger.debug("Address '{}'".format(address))
            else:
                try:
                    geo_coords = self._get_geo_coords(geolocator, location)
                except (geopy.exc.GeocoderTimedOut,
                        geopy.exc.GeocoderServiceError):
                    report['first_try_geocoder_error'].add(location)
                    # TODO: test this part
                    ipdb.set_trace()
                    continue
                if geo_coords is None:
                    report['first_try_geocoder_none'].add(location)
                    ipdb.set_trace()
                    for fallback in fallbacks:
                        # The geocoding service could not provide the geo
                        # coordinates
                        new_location = self._get_location_from_fallback(
                            city, region, country, location, fallback)
                        if new_location is None:
                            continue
                        self.logger.debug(
                            "Waiting {} second{} before the next geocoding "
                            "request ...".format(
                             wait_time, add_plural(wait_time)))
                        time.sleep(wait_time)
                        try:
                            geo_coords = self._get_geo_coords(geolocator,
                                                              new_location)
                        except (geopy.exc.GeocoderTimedOut,
                                geopy.exc.GeocoderServiceError):
                            report['next_try_geocoder_error'].add(location)
                            # TODO: test this part
                            ipdb.set_trace()
                            continue
                        if geo_coords is None:
                            # TODO: test this part
                            ipdb.set_trace()
                            self.logger.error(
                                "The geocoding service could not provide the geo "
                                "coordinates this time using '{}'".format(
                                 new_location))
                            report['next_try_geocoder_none'].add(location)
                            continue
                        else:
                            break
                    if geo_coords is None:
                        # TODO: test this part
                        self.logger.critical("The geo coordinates for '{}' are "
                                             "'None'".format(location))
                        self.logger.critical(
                            "The location '{}' will be skipped".format(
                                location))
                        continue
                self.logger.debug(
                    "Waiting {} second{} for the next geocoding request "
                    "...".format(wait_time, add_plural(wait_time)))
                time.sleep(wait_time)
                new_geo_coords = True
                # Update the cached dict with the geo coords
                self.addresses_geo_coords.setdefault(
                    geo_coords.address, geo_coords)
                self.locations_mappings.setdefault(location, geo_coords.address)
                self.logger.debug("Dictionaries updated!")
            cur_adrs_data.setdefault(geo_coords.address, {})
            cur_adrs_data[geo_coords.address].setdefault('geo_coords',
                                                               geo_coords)
            cur_adrs_data[geo_coords.address].setdefault('count', 0)
            cur_adrs_data[geo_coords.address]['count'] += 1
            cur_adrs_data[geo_coords.address].setdefault('locations', set())
            cur_adrs_data[geo_coords.address]['locations'].add(location)
            cur_loc_mappings.setdefault(location, geo_coords.address)
            if len(cur_adrs_data[geo_coords.address]['locations']) > 1:
                # Simlar locations: same locations but different spellings
                sim_locs = cur_adrs_data[geo_coords.address]['locations']
                report['similar_locations'].setdefault(geo_coords.address, set())
                report['similar_locations'][geo_coords.address].update(sim_locs)
            self.logger.debug(
                "Location '{}' added!".format(location))
            counts += 1
        # Sanity check
        n_valid_locs = len(report['already_added']) + len(cur_loc_mappings)
        assert n_valid_locs == counts, \
            "Inconsistency in the number of valid locations and the number of " \
            "times geo coordinates were computed"
        self.logger.info("Finished collecting all geo coordinates")
        self.logger.info("***** Report *****")
        self.logger.info("# of total locations: {}".format(len(locations)))
        self.logger.info("# of valid locations: {}".format(counts))
        self.logger.info("# of successfully added addresses: {}".format(
                len(cur_adrs_data)))
        self.logger.info("# of empty locations: {}".format(
            report['empty_locations']))
        self.logger.info("# of duplicated locations: {}".format(
                len(report['already_added'])))
        self.logger.info("# of distinct locations: {}".format(
            len(cur_loc_mappings)))
        self.logger.info("# of addresses with more than one location: {}".format(
            len(report['similar_locations'])))
        self.logger.info("# of similar locations: {}".format(
            sum(len(v) for k, v in report['similar_locations'].items())))
        self.logger.info(
            "# of skipped locations with first-try-geocoder-error: {}".format(
                len(report['first_try_geocoder_error'])))
        self.logger.info(
            "# of skipped locations with first-try-geocoder-none: {}".format(
                len(report['first_try_geocoder_none'])))
        self.logger.info(
            "# of skipped locations with next-try-geocoder-error: {}".format(
                len(report['next_try_geocoder_error'])))
        self.logger.info(
            "# of skipped locations with next-try-geocoder-none: {}".format(
                len(report['next_try_geocoder_none'])))
        self.logger.info("********************")
        self.logger.info(
            "These are all the addresses with more than one location:")
        for i, (address, locations) in \
                enumerate(report['similar_locations'].items(), start=1):
            self.logger.info(
                "#{}. Address '{}' --> {} locations {}".format(
                    i, address, len(locations), locations))
        if new_geo_coords:
            # Save the geo coords
            self.logger.info("Saving the geographic coordinates ...")
            self._dump_pickle(
                os.path.expanduser(
                    self.main_cfg['data_filepaths']['cache_adr_geo_coords']),
                self.addresses_geo_coords)
            # Save the locations mappings
            self.logger.info("Saving the locations mappings ...")
            self._dump_pickle(
                os.path.expanduser(
                    self.main_cfg['data_filepaths']['cache_loc_mappings']),
                self.locations_mappings)
        # TODO: is `cur_loc_mappings` necessary to return?
        return cur_adrs_data, cur_loc_mappings

    def _list_country_alpha2_to_country_name(self, list_countries_alpha2):
        # Convert country names' alpha2 to their full country names
        # TODO: is it better to have another column with the full country
        # names? or will it take more storage?
        list_countries_fullnames = []
        # TODO: catch errors with `country_alpha2_to_country_name()`
        for country_alpha2, count in list_countries_alpha2:
            if country_alpha2 is None:
                # Shouldn't happen but in any case ...
                country_name = None
            else:
                try:
                    country_name = country_alpha2_to_country_name(country_alpha2)
                except KeyError as e:
                    # self.logger.exception(e)
                    self.logger.critical(
                        "No country name for '{}'".format(country_alpha2))
                    continue
            list_countries_fullnames.append((country_name, count))
        return list_countries_fullnames


# Build location string from list of strings (city, region, country)
def build_location(list_of_str):
    loc = ""
    for i, s in enumerate(list_of_str, start=1):
        if s is None:
            continue
        loc += "{}, ".format(s)
    return loc.strip(", ")