import re
import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# load in values from .env file
client = MongoClient(os.getenv('MONGO_URI'))
db = client[os.getenv('DB_NAME')]
yearly_trends = db[os.getenv('COLLECTION1')]
countries = db.countries

#initialize csv data into database
def import_csv(csv):
    yearly_trends.drop()
    df = pd.read_csv(csv)
    print(df.columns.tolist())
    df.columns = df.columns.str.strip()
    df['country'] = df['country'].str.strip()
    df['region'] = df['region'].str.strip()
    yearly_trends.insert_many(df.to_dict(orient='records'))

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

