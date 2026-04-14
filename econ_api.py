import re
import os
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# load in values from .env file
client = MongoClient(os.getenv('MONGO_URI'))
db = client[os.getenv('DB_NAME')]
yearly_trends = db[os.getenv('COLLECTION1')]
countries = db.countries

country_id_map = {'Japan': 'JPN', 'Taiwan': 'TWN', 'China': 'CHN',
                  'Germany': 'DEU', 'Sweden': 'SWE', 'Italy': 'ITA',
                  'United States': 'USA', 'Canada': 'CAN', 'Mexico': 'MEX'}

country_region_map = {'Japan': 'Asia', 'Taiwan': 'Asia', 'China': 'Asia',
                      'Germany': 'Europe', 'Sweden': 'Europe', 'Italy': 'Europe',
                      'United States': 'Americas', 'Canada': 'Americas','Mexico': 'Americas'}

#initialize csv data into database
def import_csv(csv):
    yearly_trends.drop()
    countries.drop()
    df = pd.read_csv(csv)
    yearly_trends.insert_many(df.to_dict(orient='records'))

    yearly_trends.update_many({},[
        {'$set': {
            "labor_contributions": {
                "quantity": "$labor_quantity_contribution",
                "quality": "$labor_quality_contribution"
            },
            "capital_contributions": {
                "ict": "$ict_capital_contribution",
                "non_ict": "$non_ict_capital_contribution",
                "total": "$total_capital_contribution"}}
    },
        {'$unset': ["labor_quantity_contribution",
                       "labor_quality_contribution",
                        "ict_capital_contribution",
                       "non_ict_capital_contribution",
                       "total_capital_contribution"]}])

    for country in df['country'].unique():
        countries.insert_one({
            '_id': country_id_map[country],
            'name': country,
            'region': country_region_map[country]
        })

def define_outputs(fields):
    return {field: 1 for field in fields} | {'_id': 0}

def mongo_filter(filters):
    """
    Initialize filters for Mongo syntax.

    Args:
        filters: list of tuples of filters as string that the user wants to ask, allows >, <, >=, <=, ==, and contains
        e.g. [('country', 'Japan'), ('real_gdp', ('>', 50000)), ('country', ('contains', 'can'))]

    Returns: filter as a dictionary in Mongo syntax.
    """
    operator_map = {'>': '$gt', '>=': '$gte', '<': '$lt', '<=': '$lte', '==': '$eq', 'contains': '$regex'}
    output_filter = {}
    if not isinstance(filters, list):
        filters = [filters]

    for field, condition in filters:
        if not isinstance(condition, tuple):
            output_filter[field] = condition
        else:
            op, val = condition
            if op == 'contains':
                output_filter[field] = re.compile(val, re.IGNORECASE)
            else:
                output_filter[field] = {operator_map[op]: val}
    return output_filter

def find_records(filters=None, fields=None, sort=None, num_records=5):
    """
    Find records by filter, output fields, sort and number of records.

    Args:
        filters: filters for querying, for format see mongo_filter above
        fields: fields to include in output, for format see define_outputs above
        sort (list of tuples): sorting order, e.g., [('real_gdp', -1), ('year', -1)]
        num_records (int): number of records to return

    Example:
        find_records([('country', 'Japan')], fields=['year', 'real_gdp'], sort=[('year', 1)])
    """
    if filters:
        filters = mongo_filter(filters)
    else:
        filters = {}

    if fields:
        outputs = define_outputs(fields)
    else:
        outputs = {'_id': 0}

    results = yearly_trends.find(filters, outputs)

    if sort:
        if not isinstance(sort, list):
            sort = [sort]
        results = results.sort(sort)

    results = results.limit(num_records)

    for r in results:
        print(r)

def aggregate_records(matches=None, group_by=None, metrics=None, sort=None, fields=None, num_records=5):
    """
    Find records using MongoDB aggregation.

    Parameters:
        matches: matches to apply before aggregation, for format see mongo_filter above
        group_by: field(s) to group by, e.g., 'country' or ['country', 'region']
        metrics (tuple): metrics to compute, e.g., [('avg', 'tfp_growth'), ('max', 'real_gdp')]
        sort: sorting order after aggregation, e.g., [('avg_tfp_growth', -1)]
        num_records (int): number of results to return

    Example:
        aggregate_records(group_by='country', metrics=[('avg', 'tfp_growth')], sort=[('avg_tfp_growth', -1)])
    """
    agg = []

    if matches:
        agg.append({'$match': mongo_filter(matches)})

    if group_by:
        if isinstance(group_by, list):
            groups = {'_id': {field: f'${field}' for field in group_by}}
        else:
            groups = {'_id': f'${group_by}'}

        if metrics:
            if not isinstance(metrics, list):
                metrics = [metrics]

            for metric in metrics:
                agg_type, agg_metric = metric
                groups[f'{agg_type}_{agg_metric}'] = {f'${agg_type}': f'${agg_metric}'}

        agg.append({'$group': groups})

    if sort:
        if isinstance(sort, list):
            sort = dict(sort)
        agg.append({'$sort': sort})

    if fields:
        projection = define_outputs(fields)
        agg.append({'$project': projection})

    agg.append({'$limit': num_records})

    results = yearly_trends.aggregate(agg)
    for r in results:
        print(r)

def add_country(country_id, name, region):
    """
    Add a new country to the countries collection.

    Example:
        add_country("AUS", "Australia", "Oceania")
    """
    countries.insert_one({
        "_id": country_id,
        "name": name,
        "region": region
    })

def add_characteristic(filter_field, filter_val, new_field, char_value):
    """
    Add or update a field on all documents matching a condition.

    Example:
        add_characteristic('country', 'Japan', 'high_tech_economy', True)
    """
    yearly_trends.update_many(
        {filter_field: filter_val},
        {'$set': {new_field: char_value}}
    )

def update_country_profile():
    metrics = ['real_gdp', 'gdp_growth', 'labor_productivity', 'labor_contributions.quantity',
               'labor_contributions.quality', 'capital_contributions.ict', 'capital_contributions.non_ict',
               'capital_contributions.total', 'tfp_growth']
    results = {}

    for metric in metrics:
        name = f"avg_{metric.replace('.', '_')}"
        pipeline = [
            { "$group": {
                "_id": "$country",
                name: { "$avg": f"${metric}" }
            }},
            { "$project": {
                "_id": 1,
                name: { "$round": [f"${name}", 2] }
            }}
        ]
        for doc in db.yearly_trends.aggregate(pipeline):
            country = doc["_id"]
            if country not in results:
                results[country] = {}
            results[country][name] = doc[name]

    # Update each country document with its data_profile
    for country_name, profile in results.items():
        country_id = country_id_map[country_name]
        db.countries.update_one(
            { "_id": country_id },
            { "$set": { "data_profile": profile } }
        )

def update_metric_strengths():
    metrics = ['real_gdp', 'gdp_growth', 'labor_productivity', 'labor_contributions.quantity',
               'labor_contributions.quality', 'capital_contributions.ict', 'capital_contributions.non_ict',
               'capital_contributions.total', 'tfp_growth']
    strengths = {}

    for metric in metrics:
        name = f"avg_{metric.replace('.', '_')}"
        pipeline = [
            {"$group": {
                "_id": "$country",
                name: {"$avg": f"${metric}"}
            }},
            {"$sort": { name: -1 }},
            {"$limit": 1}
        ]

        # update each country document with its strengths list
        for doc in db.yearly_trends.aggregate(pipeline):
            country = doc["_id"]
            if country not in strengths:
                strengths[country] = []
            strengths[country].append(metric)

    # Update each country document with its data_profile
    for country_name, strength_list in strengths.items():
        country_id = country_id_map[country_name]
        db.countries.update_one(
            {"_id": country_id},
            {"$push": {"strengths": {"$each": strength_list}}}
        )

def plot_metrics(countries, metrics, start_year, end_year):
    """
    Plot normalized metrics for multiple countries over a year range.

    Args:
        countries (list): List of country names e.g. ['Japan', 'Canada']
        metrics (list): List of metric fields e.g. ['real_gdp', 'tfp_growth']
        start_year (int): Start year e.g. 1995
        end_year (int): End year e.g. 2020

    Example:
        plot_metrics(['Japan', 'Canada'], ['real_gdp', 'tfp_growth'], 1995, 2020)
    """

    fig, ax = plt.subplots(figsize=(10, 6))

    for metric in metrics:
        all_values = []
        country_data = {}

        # get countries
        for country in countries:
            records = list(yearly_trends.find(
                {'country': country, 'year': {'$gte': start_year, '$lte': end_year}},
                {'_id': 0, 'year': 1, metric: 1}
            ).sort('year', 1))

            if records:
                years = [r['year'] for r in records]
                values = [r.get(metric, 0) for r in records]
                country_data[country] = (years, values)
                all_values.extend(values)

        min_val = min(all_values)
        max_val = max(all_values)

        # plot for years
        for country, (years, values) in country_data.items():
            if max_val != min_val:
                normalized = [(v - min_val) / (max_val - min_val) for v in values]
            else:
                normalized = [0.5] * len(values)
            ax.plot(years, normalized, marker='o', label=f'{country} - {metric}')

    ax.set_title(f'Normalized Metrics ({start_year}–{end_year})')
    ax.set_xlabel('Year')
    ax.set_ylabel('Normalized Value (0-1)')
    ax.legend()
    ax.grid(True)

    plt.tight_layout()
    plt.show()


# import_csv('ted_data.csv')
# print('Profile for China:')
# print(db.countries.find_one({'_id': 'CHN'}))
# print()

# update_country_profile()
# print('Updated Profile for China with Data:')
# print(db.countries.find_one({'_id': 'CHN'}))
# print()

# update_metric_strengths()
# print('Updated Profile for China with Strengths:')
# print(db.countries.find_one({'_id': 'CHN'}))
# print()

plot_metrics(
    countries=['United States'],
    metrics=['real_gdp', 'tfp_growth', 'labor_productivity'],
    start_year=2019,
    end_year=2021
)
