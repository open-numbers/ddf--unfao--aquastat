# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os

from ddf_utils.str import to_concept_id, format_float_sigfig


# configuration of file path
source_file = '../source/bulk_eng(in).csv'
out_dir = '../../'


def extract_concepts_continuous(data):
    """Extract continuous concepts from the single dataframe."""
    concepts = data[['Variable Name', 'Variable Id', 'Unit']].copy()
    concepts = concepts.drop_duplicates().reset_index(drop=True)
    concepts['concept'] = concepts['Variable Name'].map(to_concept_id)

    concepts.columns = ['name', 'variable_id', 'unit', 'concept']
    concepts['concept_type'] = 'measure'

    return (concepts[['concept', 'concept_type', 'name', 'variable_id', 'unit']]
            .drop_duplicates(subset='concept'))


def extract_concepts_discrete():
    """manually create a descrete concepts dataframe."""
    disc = pd.DataFrame([['name', 'Name', 'string'],
                         ['year', 'Year', 'time'],
                         ['area', 'Area', 'entity_domain'],
                         ['area_id', 'Area Id', 'string'],
                         ['unit', 'Unit', 'string'],
                         ['variable_id', 'Variable Id', 'string']
                         ], columns=['concept', 'name', 'concept_type'])
    return disc


def extract_entities_area(data):
    """Extract area entities from the single dataframe."""
    area_data = data[['Area', 'Area Id']].copy()
    area_data = area_data.drop_duplicates()

    # create the area and name column.
    area_data['name'] = area_data['Area']
    area_data['area'] = area_data['name'].map(to_concept_id)

    # and drop the origin one.
    area_data = area_data.drop('Area', axis=1)
    area_data.columns = ['area_id', 'name', 'area']

    return area_data[['area', 'name', 'area_id']]


def extract_datapoints(data):
    """Extract datapoints from the single dataframe."""
    for g, ids in data.groupby('Variable Name').groups.items():
        df_concept = data.loc[ids].copy()
        concept = to_concept_id(g)

        df_concept['area'] = df_concept['Area'].map(to_concept_id)
        df_concept = df_concept.rename(columns={'Value': concept, 'Year': 'year'})
        df_yield = df_concept[['area', 'year', concept]].copy()

        yield concept, df_yield.drop_duplicates()


def extract_unit_from_variable_name(variable_name):
    """Extract unit from variable name format: {variable_name} [{unit}]"""
    if pd.isna(variable_name):
        return variable_name, None

    variable_name = str(variable_name)
    if '[' in variable_name and ']' in variable_name:
        # Find the last occurrence of brackets to handle nested brackets
        last_bracket_start = variable_name.rfind('[')
        last_bracket_end = variable_name.rfind(']')

        if last_bracket_start < last_bracket_end:
            unit = variable_name[last_bracket_start+1:last_bracket_end]
            name = variable_name[:last_bracket_start].strip()
            return name, unit

    return variable_name, None


if __name__ == '__main__':
    print('reading data file...')
    data = pd.read_csv(source_file, index_col=False, encoding='latin1')

    print('preprocessing data...')
    # Handle column renaming and dropping
    columns = list(data.columns)

    # Rename columns using pandas' automatic duplicate handling
    rename_dict = {
        'aquastatElement': 'Variable Id',
        'aquastatElement.1': 'Variable Name',
        'REF_AREA': 'Area Id',
        'AREA': 'Area',
        'timePointYears': 'Year'
    }

    data = data.rename(columns=rename_dict)

    # Drop the duplicate timePointYears column
    if 'timePointYears.1' in data.columns:
        data = data.drop('timePointYears.1', axis=1)

    # Extract units from Variable Name (only once per unique item)
    print('extracting units from variable names...')
    unique_variables = data['Variable Name'].drop_duplicates()
    unit_mapping = unique_variables.apply(
        lambda x: pd.Series(extract_unit_from_variable_name(x))
    )
    unit_mapping.columns = ['Variable Name Clean', 'Unit']
    unit_mapping.index = unique_variables.values

    # Map the extracted units back to the original data
    data = data.merge(unit_mapping, left_on='Variable Name', right_index=True, how='left')
    data['Variable Name'] = data['Variable Name Clean']
    data = data.drop('Variable Name Clean', axis=1)

    print('creating concepts files...')
    continuous_concept = extract_concepts_continuous(data)
    path = os.path.join(out_dir, 'ddf--concepts--continuous.csv')
    continuous_concept.to_csv(path, index=False)

    discrete_concept = extract_concepts_discrete()
    path = os.path.join(out_dir, 'ddf--concepts--discrete.csv')
    discrete_concept.to_csv(path, index=False)

    print('creating entities files...')
    area = extract_entities_area(data)
    path = os.path.join(out_dir, 'ddf--entities--area.csv')
    area.to_csv(path, index=False)

    print('creating datapoint files...')
    for k, df in extract_datapoints(data):
        df_ = df.copy()
        path = os.path.join(out_dir, 'ddf--datapoints--{}--by--area--year.csv'.format(k))
        df_[k] = df_[k].map(format_float_sigfig)
        df_.to_csv(path, index=False)

    print('Done.')
