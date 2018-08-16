import codecs
import json
import os

import ipdb
import iso3166
from pycountry_convert import country_alpha2_to_continent_code


COUNTRIES_FILEPATH = os.path.expanduser("~/data/dev_jobs_insights/countries.json")


if __name__ == '__main__':
    countries = {}
    invalid_countries = []
    for alpha2, country in iso3166.countries_by_alpha2.items():
        try:
            continent = country_alpha2_to_continent_code(alpha2)
        except KeyError:
            invalid_countries.append(country)
            continue
        countries.update({country.name: {
                          "continent": continent,
                          "alpha2": alpha2}
                         })
    ipdb.set_trace()
    with codecs.open(COUNTRIES_FILEPATH, 'w', 'utf8') as f:
        f.write(json.dumps(countries, sort_keys=True, ensure_ascii=False))
