import logging
import csv

import pandas as pd
import streamlit as st

LOGGING_LEVEL = logging.INFO
WORKS_CSV = 'ao3_official_dump_210321/works-20210226.csv'
TAGS_CSV = 'ao3_official_dump_210321/tags-20210226.csv'
WORKS_TAGS_CSV = 'preprocessed_works_tags.csv'
WORKS_TAGS_CSV_DTYPES = {
    'work_id': 'int',
    'creation date': 'string',
    'language': 'str',
    'complete': 'boolean',
    'word_count': 'float',
    'tag_id': 'int',
    'type_final': 'str',
    'name_final': 'str',
    'canonical_final': 'boolean'
}
TOP_50_FANDOMS_CSV = 'top_50_fandoms.csv'

logger = logging.getLogger('LOG')
logger.setLevel(LOGGING_LEVEL)
ch = logging.StreamHandler()
ch.setLevel(LOGGING_LEVEL)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'
)
ch.setFormatter(formatter)
logger.addHandler(ch)


@st.cache
def load_data(
        works_csv_location=WORKS_CSV, tags_csv_location=TAGS_CSV,
        works_tags_csv_location=WORKS_TAGS_CSV, top_n_fandom_csv_location=TOP_50_FANDOMS_CSV,
        flag_preprocess=False, works_tags_nrows=None
):
    """
    If flag_preprocess=True, loads raw works and tags dat and preprocesses
    If flag_preprocess=False, loads previously saved preprocessed data
    :param works_csv_location: Path to the CSV containing raw works data
    :param tags_csv_location: Path to the CSV containing raw tags data
    :param works_tags_csv_location: Path to the CSV containing preprocessed works tags data
    :param top_n_fandom_csv_location: Path to the CSV containing preprocess top 50 fandoms list
    :param flag_preprocess: If True, loads raw data and preprocesses it.
                            If False, loads preprocessed data
    :param works_tags_nrows: Number of rows to read from the works_tags_df when flag_preprocess=False
                            Ignored if flag_preprocess=True
    :return: DataFrame containing one row per tag per work
            List containing top 50 fandoms by works tagged
    """
    if flag_preprocess:
        # Retrieve data
        logger.info('Preprocessing data')
        logger.info('Retrieving works_df')
        works_df = pd.read_csv(works_csv_location)
        logger.info('Retrieving tags_df')
        tags_df = pd.read_csv(tags_csv_location, index_col='id')
        # Notes: Takes 19 minutes to process entire dataset
        works_tags_df = preprocess(works_df, tags_df)
        logger.info(f'Saving preprocessed works tags data to {works_tags_csv_location}')
        works_tags_df.to_csv(works_tags_csv_location, index=False)
        top_50_fandoms = list(tags_df.query(
            'type == "Fandom"'
        ).sort_values(
            by='cached_count', ascending=False
        ).head(50)['name'])
        top_50_fandoms = [[s] for s in top_50_fandoms]
        logger.info(f'Saving list of top 50 fandoms to {top_n_fandom_csv_location}')
        with open(top_n_fandom_csv_location, 'w') as f:
            write = csv.writer(f)
            write.writerows(top_50_fandoms)
    else:
        logger.info('Loading previously preprocessed data')
        # Test replacing this with the other IO library
        # Currently takes 3 minutes
        top_50_fandoms = [s.strip() for s in open(top_n_fandom_csv_location).readlines()]
        works_tags_df = pd.read_csv(works_tags_csv_location, dtype=WORKS_TAGS_CSV_DTYPES, nrows=works_tags_nrows)
        works_tags_df['creation date'] = pd.to_datetime(
            works_tags_df['creation date'], format='%Y-%m-%d'
        )
        logger.info('Finished loading data')
    return works_tags_df, top_50_fandoms


def preprocess(works_df, tags_df):
    """
    Explodes works_df to one row per tag per work, retrieves the names
    and types of the tags, and standardizes non-canonical tags
    :param works_df: A DataFrame with work info, one row per work
    :param tags_df: A DataFrame with tag info, one row per tag
    :return: A DataFrame with one row per tag per work
    """
    # Standardize non-canonical tags
    logger.info('Standardizing non-canonical tags')
    cols_to_coalesce = ['type', 'name', 'canonical']
    tags_df_merger = standardize_tags(tags_df, cols_to_coalesce)
    # Retrieve work tags
    logger.debug('Splitting tags into a list')
    works_df['tags_list'] = works_df['tags'].str.strip().str.split('+')
    logger.info('Exploding works')
    works_tags_df = works_df.drop(
        labels=['tags', 'Unnamed: 6', 'restricted'], axis=1
    ).explode(
        'tags_list'
    ).reset_index(
    ).rename(
        columns={'tags_list': 'tag_id', 'index': 'work_id'}
    )
    works_tags_df['tag_id'] = works_tags_df['tag_id'].fillna(-999).astype(int)
    works_tags_df = works_tags_df.merge(
        tags_df_merger, how='left', left_on='tag_id', right_index=True
    )
    return works_tags_df


def standardize_tags(tags_df, cols_to_coalesce):
    """
    Standardizes tags by retrieving canonical tag information for non-canonical tags
        that have a canonical equivalent
    :param tags_df: A DataFrame with tag info, one row per tag
    :param cols_to_coalesce: A list of columns in tags_df for which to retrieve
        canonical information if it exists
    :return: A DataFrame with standardized fields listed in cols_to_coalesce
    """
    tags_df_std = tags_df.merge(
        tags_df,
        how='left',
        left_on='merger_id',
        right_index=True,
        suffixes=['_orig', '_merg'],
        validate='many_to_one'
    )
    cols_final = []
    for col in cols_to_coalesce:
        tags_df_std[f'{col}_final'] = tags_df_std[
            f'{col}_merg'
        ].combine_first(tags_df_std[f'{col}_orig'])
        cols_final.append(f'{col}_final')

    tags_df_std = tags_df_std[cols_final].copy()

    return tags_df_std


def format_thousand(number, precision=0):
    if precision == 0:
        return f'{int(number / 1000)}K'
    else:
        return f'{round(number / 1000, precision)}K'
