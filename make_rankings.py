import pandas as pd
import numpy as np
from hashlib import md5
import json

def harmonize(x, max_value, min_value, year):
    return (x - min_value) / (max_value - min_value)

def make_year_avg(x):
    existing_values = x.count()
    total_values = x.fillna(-1).count()
    missing_values = total_values - existing_values
    if missing_values > 3:
        return np.nan
    else:
        return x.mean()

def make_avg(x):
    return x.apply(make_year_avg)

def make_indicator(indicators_from_app):
    indicators = []
    indicators_names = []
    indicators_harmonized = []
    years = ["2017", "2016", "2015", "2014", "2013", "2012", "2011", "2010", "2009", "2008", "2007", "2006"]

    indicators_df = pd.read_csv("./data/indicators-list.csv")

    countries_df = pd.read_csv("./data/countries-list.csv")

    for indicator_from_app in indicators_from_app:
        # I added an empty space by mistake in the code names
        indicator_code = indicator_from_app["code"]
        indicator_name = indicator_from_app["name"]
        indicators_names.append(indicator_name)
        indicator_df = pd.read_csv("./data/wb-data/API_"+ indicator_code +"%20_DS2_en_csv_v2.csv"
                                   , skiprows=4
                                   , index_col="Country Name"
                                   , usecols=years + ["Country Name"])
        
        # Remove all rows which are not countries
        indicator_df = indicator_df.loc[indicator_df.index.isin(countries_df["name"])]
        
        # Find max and min prior to harmonization
        max_value = indicator_df.max(numeric_only=True).max()
        min_value = indicator_df.min(numeric_only=True).min()
        
        # For negative series, use the opposite
        if indicator_from_app["is_reverse"] == 1:
            max_value = indicator_df.min(numeric_only=True).min()
            min_value = indicator_df.max(numeric_only=True).max()
            
        indicator_harmonized_df = pd.DataFrame()
        
        for year in years:
            indicator_harmonized_df[year] = indicator_df[year].apply(harmonize, max_value=max_value, min_value=min_value, year=year)

        indicators.append(indicator_df)
        indicators_harmonized.append(indicator_harmonized_df)

    index_df = pd.concat(indicators_harmonized)
    index_df = index_df.groupby(level=0).apply(make_avg).dropna(axis=1, thresh=70).dropna(axis=0, thresh=1)
    index_df = index_df.sort_values(index_df.columns[0], ascending=False)

    last_year = index_df.iloc[:,0].name
    lead_name = index_df.iloc[:,0].index[0]
    years_number = len(list(index_df.columns.values))
    countries_num = len(list(index_df.iloc[:,0].dropna()))
    json_data = {
        "last_year": index_df.iloc[:,0].name,
        "years" : list(index_df.columns.values),
        "top_country": lead_name,
        "bottom_country": index_df.iloc[:,0].dropna().index[-1],
        "indicators": [],
        "data": []
        }

    for indicator in indicators_names:
        try:
            indicator_name = indicator.split("(")[0].strip()
            indicator_unit = indicator.split("(")[1].strip().replace(")", "")
        except IndexError:
            try:
                indicator_name = indicator.split(",")[0].strip()
                indicator_unit = indicator.split(",")[1].strip()
            except IndexError:
                indicator_name = indicator
                indicator_unit = ""

        json_data["indicators"].append({"name": indicator_name, "unit": indicator_unit})

    ranking = list(index_df.iloc[:,0].dropna().index)
    for country_name in ranking:
        rank = ranking.index(country_name) + 1
        scores = index_df.loc[index_df.index == country_name].values[0]
        country_code = countries_df.loc[countries_df["name"] == country_name, "code"].tolist()[0]
        components = []

        for indicator in indicators_names:
            indicator_index = indicators_names.index(indicator)
            indicator_df = indicators[indicator_index]
            indicator_normalized_df = indicators_harmonized[indicator_index]
            score = indicator_df.loc[indicator_df.index == country_name][last_year][0]
            score_normalized = indicator_normalized_df.loc[indicator_df.index == country_name][last_year][0]
            components.append({"score": score, "score_normalized": score_normalized})
            
        json_data["data"].append({"country_name": country_name, 
                                  "country_code": country_code,
                                  "rank": rank, 
                                  "scores": list(scores),
                                  "components": components})
        

    return json_data, lead_name, last_year, years_number, countries_num