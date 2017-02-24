# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os

from ddf_utils.str import to_concept_id, format_float_sigfig
from ddf_utils.index import get_datapackage


# configuration of file path
source_dir = '../source'
out_dir = '../../'


def extract_concepts_continuous(all_data):
    all_concepts = []

    for df in all_data:
        all_concepts.append(df[['Variable Name', 'Variable Id']].copy())

    all_concepts = pd.concat(all_concepts)
    all_concepts = all_concepts.reset_index(drop=True).drop_duplicates()
    all_concepts['concept'] = all_concepts['Variable Name'].map(to_concept_id)

    all_concepts.columns = ['name', 'variable_id', 'concept']
    all_concepts['concept_type'] = 'measure'

    return all_concepts[['concept', 'concept_type', 'name', 'variable_id']]


def extract_concepts_discrete():
    """manually create a descrete concepts dataframe."""
    disc = pd.DataFrame([['name', 'Name', 'string'],
                         ['year', 'Year', 'time'],
                         ['area', 'Area', 'entity_domain'],
                         ['area_id', 'Area Id', 'string'],
                         ['variable_id', 'Variable Id', 'string']
                         ], columns=['concept', 'name', 'concept_type'])
    return disc


def extract_entities_area(all_data):
    all_area = []

    for df in all_data:
        all_area.append(df[['Area', 'Area Id']].copy())

    all_area = pd.concat(all_area)
    all_area = all_area.drop_duplicates()
    # all Area in the source file have "ISO|name" format.
    # so we seperate those into 2 columns.
    all_area['area'] = all_area['Area'].map(lambda x: x.split('|')[0])
    all_area['name'] = all_area['Area'].map(lambda x: x.split('|')[1])
    # and drop the origin one.
    all_area = all_area.drop('Area', axis=1)
    all_area['area'] = all_area['area'].map(to_concept_id)
    all_area.columns = ['area_id', 'area', 'name']

    return all_area[['area', 'name', 'area_id']]


def extract_datapoints(all_data):
    for df in all_data:
        for g, ids in df.groupby('Variable Name').groups.items():
            df_concept = df.ix[ids].copy()
            concept = to_concept_id(g)

            df_concept['area'] = df['Area'].map(lambda x: to_concept_id(x.split('|')[0]))
            df_concept = df_concept.rename(columns={'Value': concept, 'Year': 'year'})
            df_yield = df_concept[['area', 'year', concept]].copy()

            yield concept, df_yield


if __name__ == '__main__':
    print('reading data files...')
    all_data = []
    for f in os.listdir(source_dir):
        if 'csv' in f:
            path = os.path.join(source_dir, f)
            all_data.append(pd.read_csv(path,
                                        skiprows=2,
                                        skip_footer=8,
                                        index_col=False,
                                        engine='python'))

    print('creating concepts files...')
    continuous_concept = extract_concepts_continuous(all_data)
    path = os.path.join(out_dir, 'ddf--concepts--continuous.csv')
    continuous_concept.to_csv(path, index=False)

    discrete_concept = extract_concepts_discrete()
    path = os.path.join(out_dir, 'ddf--concepts--discrete.csv')
    discrete_concept.to_csv(path, index=False)

    print('creating entities files...')
    area = extract_entities_area(all_data)
    path = os.path.join(out_dir, 'ddf--entities--area.csv')
    area.to_csv(path, index=False)

    print('creating datapoint files...')
    for k, df in extract_datapoints(all_data):
        path = os.path.join(out_dir, 'ddf--datapoints--{}--by--area--year.csv'.format(k))
        df[k] = df[k].map(format_float_sigfig)
        df.to_csv(path, index=False)

    print('creating index file...')
    get_datapackage(out_dir, use_existing=True, to_disk=True)

    print('Done.')
