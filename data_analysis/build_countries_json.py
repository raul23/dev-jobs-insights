import json
import ipdb

import iso3166
from pycountry_convert import convert_country_alpha2_to_continent


if __name__ == '__main__':
    countries = {}
    invalid_countries = []
    for alpha2, country in iso3166.countries_by_alpha2.items():
        try:
            continent = convert_country_alpha2_to_continent(alpha2)
        except KeyError:
            invalid_countries.append(country)
            continue
        countries.update({country.name: {
                          "continent": continent,
                          "alpha2": alpha2}
                         })
    ipdb.set_trace()
    with open("countries_info.json", "w") as f:
        json.dump(countries, f)
