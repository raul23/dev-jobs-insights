import os
import time
# Third-party modules
import geopy
from geopy.geocoders import Nominatim
import ipdb
import numpy as np
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
        # TODO: for the `barh` field, all the sub-fields are based on the whole
        # data not only the topk used for drawing the bar chart. There should be
        # an `overall` field that should store those values and `barh` should only
        # store values actually used for drawing the bar chart. See also other
        # modules that use the same structure for the report `dict`.
        self.report = {
            'europe': {
                'barh': {
                    'duplicates': [],  #
                    'items': {
                        'labels': ['country', 'count_desc'],
                        'data': [],
                        'number_of_items': None,
                        'sum_of_counts': None
                    },
                    'job_posts_ids': [],  #
                    'number_of_job_posts': None,  #
                    'published_dates': [],  #
                    'skipped': []
                },
                'map': {
                    'items1': {
                        'labels': ['address', 'count_desc'],
                        'data': [],
                        'number_of_items': None,
                        'sum_of_counts': None
                    },
                    'items2': {
                        'labels': ['country', 'count_desc'],
                        'data': [],
                        'number_of_items': None,
                        'sum_of_counts': None
                    },
                    'duplicates': [],
                    'job_posts_ids': [],
                    'number_of_job_posts': None,
                    'published_dates': [],
                },
            },
            'usa': {
                'barh': {
                    'duplicates': [],  #
                    'items': {
                        'labels': ['us_state', 'count_desc'],
                        'data': [],
                        'number_of_items': None,
                        'sum_of_counts': None
                    },
                    'job_posts_ids': [],  #
                    'number_of_job_posts': None,  #
                    'published_dates': [],  #
                    'skipped': []
                },
                'map': {
                    'items1': {
                        'labels': ['address', 'count_desc'],
                        'data': [],
                        'number_of_items': None,
                        'sum_of_counts': None
                    },
                    'items2': {
                        'labels': ['us_state', 'count_desc'],
                        'data': [],
                        'number_of_items': None,
                        'sum_of_counts': None
                    },
                    'duplicates': [],
                    'job_posts_ids': [],
                    'number_of_job_posts': None,
                    'published_dates': [],
                },
            },
            'world': {
                'barh': {
                    'duplicates': [],  #
                    'items': {
                        'labels': ['country', 'count_desc'],
                        'data': [],
                        'number_of_items': None,
                    },
                    'job_posts_ids': [],  #
                    'number_of_job_posts': None,  #
                    'published_dates': [],  #
                    'skipped': []
                },
                'map': {
                    'items1': {
                        'labels': ['address', 'count_desc'],
                        'data': [],
                        'number_of_items': None,
                    },
                    'items2': {
                        'labels': ['country', 'count_desc'],
                        'data': [],
                        'number_of_items': None,
                    },
                    'duplicates': [],
                    'job_posts_ids': [],
                    'number_of_job_posts': None,
                    'published_dates': [],
                },
            }
        }
        super().__init__(analysis_type,
                         conn,
                         db_session,
                         main_cfg,
                         logging_cfg,
                         self.stats_names,
                         self.report,
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
        self.cache_addresses = {'europe': None, 'usa': None, 'world': None}

    def run_analysis(self):
        # Reset all locations stats to be computed
        self.reset_stats()
        # =====================================================================
        #                       European analysis
        # =====================================================================
        # Get counts of european countries, i.e. for each country we want to
        # know its number of occurrences in job posts
        # NOTE: These are all the european countries and they are sorted in
        # order of decreasing number of occurrences (i.e. most popular european
        # country at first)
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
        # =====================================================================
        #                       US states analysis
        # =====================================================================
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
        # =====================================================================
        #                           World analysis
        # =====================================================================
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
        # =====================================================================
        #                           Maps
        # =====================================================================
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
        # =====================================================================
        #                               Report
        # =====================================================================
        self._update_all_map_reports()
        if self.main_cfg['job_locations']['save_report']:
            self._save_report(self.main_cfg['job_locations']['report_filename'])

    def _count_all_countries(self, ignore_none=False, use_fullnames=False):
        """
        Returns countries sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (country, count).

        :return: list of tuples of the form (country, count)
        """
        # IMPORTANT: 'No office location' is not ignored if `use_fullnames=False`
        self.logger.debug("Counting all countries")
        sql = "SELECT job_post_id, country FROM job_locations"
        results = self.db_session.execute(sql).fetchall()
        if use_fullnames:
            converter = self._get_country_name
        else:
            converter = None
        results, list_ids, duplicates, skipped = self._count_items(
            list_items=results,
            converter=converter,
            ignore_none=ignore_none,
            ignore_duplicates=True)
        # Update report for World
        self._update_graph_report(
            graph_report=self.report['world']['barh'],
            items=results,
            job_post_ids=list_ids,
            duplicates=duplicates,
            skipped=skipped)
        return results

    # A country is counted only once for each job post
    def _count_european_countries(self, ignore_none=False, use_fullnames=False):
        """
        Returns european countries sorted in decreasing order of their
        occurrences in job posts. A list of tuples is returned where a tuple is
        of the form (country, count).

        :return: list of tuples of the form (country, count)
        """
        self.logger.debug("Counting european countries")
        sql = "SELECT job_post_id, country FROM job_locations WHERE country in " \
              "({})".format(self._get_european_countries_as_str())
        results = self.db_session.execute(sql).fetchall()
        if use_fullnames:
            converter = self._get_country_name
        else:
            converter = None
        results, list_ids, duplicates, skipped = self._count_items(
            list_items=results,
            converter=converter,
            ignore_none=ignore_none,
            ignore_duplicates=True)
        # Update report for Europe
        self._update_graph_report(
            graph_report=self.report['europe']['barh'],
            items=results,
            job_post_ids=list_ids,
            duplicates=duplicates,
            skipped=skipped)
        return results

    # A US state is counted only once for each job post
    def _count_us_states(self, ignore_none=False, use_fullnames=False):
        """
        Returns US states sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (us_state, count).

        NOTE: in the SQL expression, the column region refers to a state

        :return: list of tuples of the form (us_state, count)
        """
        self.logger.debug("Counting US states")
        sql = "SELECT job_post_id, region FROM job_locations WHERE country='US'"
        results = self.db_session.execute(sql).fetchall()
        if use_fullnames:
            converter = self.us_states.get
        else:
            converter = None
        results, list_ids, duplicates, skipped = self._count_items(
            list_items=results,
            converter=converter,
            ignore_none=ignore_none,
            ignore_duplicates=True)
        # Update report for USA
        self._update_graph_report(
            graph_report=self.report['usa']['barh'],
            items=results,
            job_post_ids=list_ids,
            duplicates=duplicates,
            skipped=skipped)
        return results

    def _generate_map_europe(self):
        raise NotImplementedError

    # TODO: get the `map_type` from the name of the function
    def _generate_map_usa(self, map_type, map_cfg):
        if not map_cfg['display_graph'] and not map_cfg['save_graph']:
            self.logger.warning("The map '{}' is disabled for the '{}' "
                                "analysis".format(map_type, self.analysis_type))
            return 1
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
        from utility.graphutil import draw_map_usa
        self.logger.debug("finished loading module 'utility.graphutil'")
        self.logger.info("Generating map '{}' ...".format(map_type))
        addresses_data, valid_locations = self._get_locations_geo_coords(
            locations=self._get_us_states(),
            fallbacks=['region+country', 'country'])
        self.cache_addresses['usa'] = [addresses_data, valid_locations]
        shape_filepath = os.path.expanduser(
            self.main_cfg['data_filepaths']['shape'])
        # TODO: explain why reversed US states is used
        us_states_filepath = os.path.expanduser(
            self.main_cfg['data_filepaths']['reversed_us_states'])
        draw_map_usa(
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
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        self.logger.info("loading module 'utility.graphutil' ...")
        from utility.graphutil import draw_map_world
        self.logger.debug("finished loading module 'utility.graphutil'")
        self.logger.info("Generating map '{}' ...".format(map_type))
        addresses_data, valid_locations = self._get_locations_geo_coords(
            locations=self._get_all_locations(),
            fallbacks=['region+country', 'country'])
        self.cache_addresses['world'] = [addresses_data, valid_locations]
        draw_map_world(
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
            self.logger.debug(
                "All locations with 'No office location' will be ignored")
        else:
            where = ""
        sql = "SELECT job_post_id, city, region, country FROM " \
              "job_locations{}".format(where)
        return self.db_session.execute(sql).fetchall()

    def _get_us_states(self):
        """
        Returns all US states. A list of tuples is returned where a tuple is of
        the form (job_post_id, city, region, country).

        :return: list of tuples of the form
                 (job_post_id, city, region, country)
        """
        # TODO: concatenate the three columns (city, region, country) into a
        # single string, e.g. 'Colorado Springs, CO, US'. You might get also
        # `None` within the string since not all job locations have a city or a
        # region. If you find out how to to concatenate the three columns, then
        # `get_location()` won't be needed within `_get_locations_geo_coords()`
        """
        if ignore_no_office_location:
            where = " and country!='No office location'"
            self.logger.debug(
                "All locations with 'No office location' will be ignored")
        else:
            where = ""
        """
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
        # `addresses_data`: addresses' geographic coordinates for the current
        # session
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
        addresses_data = {}
        # Locations mappings to addresses for the current session
        # keys: location name
        # values: addresses as given by the geocode service
        loc_mappings = {}
        valid_locations = []
        # Skipped locations stats
        # TODO: explain fields of dict
        unsual_report = {'empty_locations': set(),
                         'no_office_location': set(),
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
        for i, (job_post_id, city, region, country) in \
                enumerate(locations, start=1):
            # Get location from three components of city, region, and country
            # e.g. [Montreal, Quebec, Canada] --> "Montreal, Quebec, Canada"
            location = build_location([city, region, country])
            self.logger.info("Location #{}: {} (job_post_id={})".format(
                              i, location, job_post_id))
            # TODO: remove hack!!
            if city == 'ód':
                self.logger.warning("City 'ód' ignored!")
                continue
            if not location:
                # We ignore the case where the location is empty
                # NOTE: This case shouldn't happen because all job locations
                # have at least a country
                self.logger.warning(
                    "The location is empty for '{}'".format(job_post_id))
                unsual_report['empty_locations'].add(job_post_id)
                continue
            if location == "No office location":
                self.logger.warning(
                    "'No office location' detected for '{}'".format(job_post_id))
                unsual_report['no_office_location'].add(job_post_id)
                continue
            elif location in loc_mappings:
                # Location already added
                unsual_report['already_added'].append((job_post_id, location))
                # Update count of this locations's address
                address = self.locations_mappings.get(location)
                addresses_data[address]['count'] += 1
                valid_locations.append((job_post_id, city, region, country))
                self.logger.debug(
                    "Location '{}' was already added!".format(location))
                self.logger.debug("Address '{}'".format(address))
                # Location skipped!
                continue
            elif location in self.locations_mappings:
                # We previously computed the location's latitude and longitude
                # with the geocoding service
                # Get the location's geo coordinates from its address
                address = self.locations_mappings.get(location)
                geo_coords = self.addresses_geo_coords[address]
                self.logger.debug(
                    "Location '{}' found in cache!".format(location))
                self.logger.debug("Geo coordinates: {}".format(geo_coords.point))
                self.logger.debug("Address '{}'".format(address))
            else:
                # New location!
                # Retrieve the location's geo coordinates with the geocoding
                # service
                try:
                    geo_coords = self._get_geo_coords(geolocator, location)
                except (geopy.exc.GeocoderTimedOut,
                        geopy.exc.GeocoderServiceError):
                    unsual_report['first_try_geocoder_error'].add(location)
                    # TODO: test this part
                    ipdb.set_trace()
                    continue
                if geo_coords is None:
                    # No geo coordinates could be retrieved the first time
                    # Retry with fallbacks
                    unsual_report['first_try_geocoder_none'].add(location)
                    ipdb.set_trace()
                    for fallback in fallbacks:
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
                            unsual_report['next_try_geocoder_error'].add(location)
                            # TODO: test this part
                            ipdb.set_trace()
                            continue
                        if geo_coords is None:
                            # Again, the geo coordinates couldn't be retrieved
                            # with the selected fallback. Try again with another
                            # fallback
                            # TODO: test this part
                            ipdb.set_trace()
                            self.logger.error(
                                "The geocoding service could not provide the geo "
                                "coordinates this time using '{}'".format(
                                 new_location))
                            unsual_report['next_try_geocoder_none'].add(location)
                            continue
                        else:
                            break
                    if geo_coords is None:
                        # TODO: test this part
                        # After all fallbacks tried, still no geo coordinates
                        # found for the location. Location will be skipped!
                        self.logger.critical("The geo coordinates for '{}' are "
                                             "'None'".format(location))
                        self.logger.critical(
                            "The location '{}' will be skipped!".format(
                                location))
                        continue
                # Geo coordinates were found for the location
                self.logger.debug(
                    "Waiting {} second{} for the next geocoding request "
                    "...".format(wait_time, add_plural(wait_time)))
                time.sleep(wait_time)
                new_geo_coords = True
                # Update the cached `dict`s with the geo coordinates
                self.addresses_geo_coords.setdefault(
                    geo_coords.address, geo_coords)
                self.locations_mappings.setdefault(location, geo_coords.address)
                self.logger.debug("Dictionaries updated!")
            # Save the addresses and extra data for the current session
            addresses_data.setdefault(geo_coords.address, {})
            addresses_data[geo_coords.address].setdefault('geo_coords',
                                                          geo_coords)
            addresses_data[geo_coords.address].setdefault('count', 0)
            addresses_data[geo_coords.address]['count'] += 1
            addresses_data[geo_coords.address].setdefault('locations', set())
            addresses_data[geo_coords.address]['locations'].add(location)
            loc_mappings.setdefault(location, geo_coords.address)
            if len(addresses_data[geo_coords.address]['locations']) > 1:
                # Similar locations: same locations but different spellings
                sim_locs = addresses_data[geo_coords.address]['locations']
                unsual_report['similar_locations'].setdefault(
                    geo_coords.address, set())
                unsual_report['similar_locations'][geo_coords.address].update(
                    sim_locs)
            self.logger.debug("Location '{}' added!".format(location))
            valid_locations.append((job_post_id, city, region, country))
        # Sanity check
        n_valid_locs = len(unsual_report['already_added']) + len(loc_mappings)
        assert n_valid_locs == len(valid_locations), \
            "Inconsistency between the two methods of computing the number of " \
            "valid locations: {} != {}".format(n_valid_locs, len(valid_locations))
        self.logger.info("Finished collecting all geo coordinates")
        report_str = """
***** Report *****
# of total locations: {}
# of valid locations: {}
# of successfully added addresses: {}
# of duplicate locations: {}
# of empty locations: {}
# of 'No office location': {}
# of distinct locations: {}
# of addresses with more than one location: {}
# of similar locations: {}
# of skipped locations with first-try-geocoder-error: {}
# of skipped locations with first-try-geocoder-none: {}
# of skipped locations with next-try-geocoder-error: {}
# of skipped locations with next-try-geocoder-none: {}
********************
""".format(
            len(locations),  # total locations
            len(valid_locations),  # valid locations
            len(addresses_data),  # successfully added addresses
            len(unsual_report['already_added']),  # duplicate locations
            len(unsual_report['empty_locations']),
            len(unsual_report['no_office_location']),
            len(loc_mappings),  # distinct locations
            len(unsual_report['similar_locations']),  # addresses one + location
            sum(len(v) for k, v in unsual_report['similar_locations'].items()),
            len(unsual_report['first_try_geocoder_error']),
            len(unsual_report['first_try_geocoder_none']),
            len(unsual_report['next_try_geocoder_error']),
            len(unsual_report['next_try_geocoder_none']),
        )
        self.logger.info(report_str)
        self.logger.info(
            "These are all the addresses with more than one location:")
        for i, (address, locations) in \
                enumerate(unsual_report['similar_locations'].items(), start=1):
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
        return addresses_data, valid_locations

    @staticmethod
    def _get_topk_addresses(addresses_data, topk=10):
        top_addresses = sorted(addresses_data.items(),
                               key=lambda x: x[1]['count'],
                               reverse=True)[:topk]
        return [(addr[0], addr[1]['count']) for addr in top_addresses]

    def _update_all_map_reports(self):
        # Update report for all
        self.logger.info("Updating map reports")
        self.logger.warning("Europe map not yet implemented. Thus, the report "
                            "for Europe will not be updated.")
        maps_metadata = {
            'usa': {
                'location_col_idx': 2,
                'locations_func': self._get_us_states,
                'converter_func': self.us_states.get
            },
            'world': {
                'location_col_idx': 3,
                'locations_func': self._get_all_locations,
                'converter_func': self._get_country_name
            },
        }
        for k, v in maps_metadata.items():
            self.logger.info("Updating the report for the '{}' map".format(k))
            map_report = self.report[k]['map']
            location_col_idx = v['location_col_idx']
            locations_func = v['locations_func']
            converter_func = v['converter_func']
            if self.cache_addresses[k]:
                addresses_data, valid_locations = self.cache_addresses[k]
            else:
                addresses_data, valid_locations = self._get_locations_geo_coords(
                        locations=locations_func(),
                        fallbacks=['region+country', 'country'])
            valid_locations = np.array(valid_locations)
            unique_job_post_ids = np.unique(valid_locations[:, 0]).tolist()
            addresses_count_desc = sorted(
                [(k, v['count']) for k, v in addresses_data.items()],
                key=lambda x: x[1],
                reverse=True)
            list_locations = [(i[0], i[location_col_idx])
                              for i in valid_locations]
            results, list_ids, duplicates, skipped = self._count_items(
                list_items=list_locations,
                converter=converter_func,
                ignore_duplicates=True)
            min_date, max_date = self._get_min_max_published_dates(
                unique_job_post_ids)
            # Addresses (items1)
            map_report['items1']['data'] = addresses_count_desc
            map_report['items1']['number_of_items'] = \
                len(addresses_count_desc)
            map_report['items1']['sum_of_counts'] = \
                sum([v['count'] for k, v in addresses_data.items()])
            # Countries or US states (items2)
            map_report['items2']['data'] = results
            map_report['items2']['number_of_items'] = len(results)
            map_report['items2']['sum_of_counts'] = sum([i[1] for i in results])
            map_report['duplicates'] = duplicates
            map_report['job_posts_ids'] = unique_job_post_ids
            map_report['number_of_job_posts'] = len(unique_job_post_ids)
            map_report['published_dates'] = [min_date, max_date]
            map_report['skipped'] = skipped


# Build location string from list of strings (city, region, country)
def build_location(list_of_str):
    loc = ""
    for i, s in enumerate(list_of_str, start=1):
        if s is None:
            continue
        loc += "{}, ".format(s)
    return loc.strip(", ")