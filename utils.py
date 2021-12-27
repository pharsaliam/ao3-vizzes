import logging

import pandas as pd
import streamlit as st

LOGGING_LEVEL = logging.INFO
WORKS_CSV = 'ao3_official_dump_210321/works-20210226.csv'
TAGS_CSV = 'ao3_official_dump_210321/tags-20210226.csv'
WORKS_TAGS_PARQUET = 'not_added_to_git/preprocessed_works_tags.parquet.gzip'
TAGS_AGGREGATED_PARQUET = 'data/non_fandoms_tags_aggregated.parquet.gzip'
WORKS_WITH_FANDOM_PARQUET = 'data/works_with_fandom.parquet.gzip'
FANDOM_WORKS_COUNT_PARQUET = 'data/fandom_works_count.parquet.gzip'
TAG_TYPES_TO_KEEP = ['Relationship', 'Freeform', 'ArchiveWarnings', 'Rating', 'Fandom']
MINIMUM_WORK_COUNT = 100
TAG_GROUPBY_LIST = ['fandom_name', 'name_final', 'type_final']
TAG_GROUPBY_AGG = {
    'work_id': 'count',
    'word_count': 'mean'
}
TO_PARQUET_CONFIG = {'index': 'False', 'compression': 'gzip'}

# TODO Figure out why duplicate logs
logger = logging.getLogger('LOG')
logger.setLevel(LOGGING_LEVEL)
ch = logging.StreamHandler()
ch.setLevel(LOGGING_LEVEL)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'
)
ch.setFormatter(formatter)
logger.addHandler(ch)


@st.cache(hash_funcs={pd._libs.parsers.TextReader: id})
def retrieve_preprocessed_data(
        tags_aggregated_location=TAGS_AGGREGATED_PARQUET,
        works_with_fandom_location=WORKS_WITH_FANDOM_PARQUET,
        fandom_count_location=FANDOM_WORKS_COUNT_PARQUET,
):
    """
    Loads previously saved preprocessed and aggregated data
    :param tags_aggregated_location: Location of the aggregated non-fandom tags data
    :type tags_aggregated_location: str
    :param works_with_fandom_location: Location of the works with fandom data
    :type works_with_fandom_location: str
    :param fandom_count_location: Location of the fandom count data
    :type fandom_count_location: str
    :return:
        - One row per fandom per non-fandom tag with count of works
        - One row per work per fandom
        - One row per fandom with count of works
    :rtype:
        - pandas DataFrame
        - pandas DataFrame
        - pandas DataFrame
    """
    logger.info('Loading previously preprocessed data')
    non_fandom_tags_agg = pd.read_parquet(tags_aggregated_location)
    works_with_fandom = pd.read_parquet(works_with_fandom_location)
    fandom_works_count = pd.read_parquet(fandom_count_location)
    logger.info('Finished loading data')
    return non_fandom_tags_agg, works_with_fandom, fandom_works_count


def preprocess_data(
        works_csv_location=WORKS_CSV, tags_csv_location=TAGS_CSV, works_tags_location=WORKS_TAGS_PARQUET,
        tags_aggregated_location=TAGS_AGGREGATED_PARQUET, works_with_fandom_location=WORKS_WITH_FANDOM_PARQUET,
        fandom_count_location=FANDOM_WORKS_COUNT_PARQUET, minimum_work_count=MINIMUM_WORK_COUNT, flag_save_data=True
):
    """
    Preprocesses raw AO3 data dump. If flag_save_data=True, will save data as parquet files
    :param works_csv_location: Location of the AO3 data dump works CSV
    :type works_csv_location: str
    :param tags_csv_location: Location of AO3 data dump tags CSV
    :type tags_csv_location: str
    :param works_tags_location: Location of file with one row per work per tag
    :type works_tags_location: str
    :param tags_aggregated_location: Location of file with aggregated non-fandom tag data
    :type tags_aggregated_location: str
    :param works_with_fandom_location: Location of file with one row per work per fandom
                                        after filtering out rare fandoms
    :type works_with_fandom_location: str
    :param fandom_count_location: Location of file with aggregated fandom work count data
    :type fandom_count_location: str
    :param minimum_work_count: Minimum number of works a fandom must have to be included in analysis
    :type minimum_work_count: int
    :param flag_save_data: Whether or not to save the preprocessed data
    :type flag_save_data: bool
    :return: None
    :rtype: None
    """
    # Retrieve data
    logger.info('Preprocessing data')
    logger.info('Retrieving works_df')
    works_df = pd.read_csv(works_csv_location)
    logger.info('Retrieving tags_df')
    tags_df = pd.read_csv(tags_csv_location, index_col='id')
    # Notes: Takes 19 minutes to process entire dataset
    logger.info('Generating works_tags_df')
    works_tags_df = generate_works_tags_df(works_df, tags_df)
    logger.info('Aggregating works_tags_df')
    non_fandom_tags_agg, works_with_fandom, fandom_works_count = aggregate_works_tags_df(
        works_tags_df, minimum_work_count
    )
    if flag_save_data:
        logger.info(f'Saving preprocessed works tags data to {works_tags_location}')
        works_tags_df.to_parquet(works_tags_location, **TO_PARQUET_CONFIG)
        logger.info(f'Saving aggregated non fandom tags data to {tags_aggregated_location}')
        non_fandom_tags_agg.to_parquet(tags_aggregated_location, **TO_PARQUET_CONFIG)
        logger.info(f'Saving works with fandoms data to {works_with_fandom_location}')
        works_with_fandom.to_parquet(works_with_fandom_location, **TO_PARQUET_CONFIG)
        logger.info(f'Saving fandom work counts data to {fandom_count_location}')
        fandom_works_count.to_parquet(fandom_count_location, **TO_PARQUET_CONFIG)
    return None


def generate_works_tags_df(works_df, tags_df):
    """
    Explodes works_df to one row per tag per work, retrieves the names
    and types of the tags, and standardizes non-canonical tags
    :param works_df: A DataFrame with work info, one row per work
    :type works_df: pandas Dataframe
    :param tags_df: A DataFrame with tag info, one row per tag
    : type tags_df: pandas DataFrame
    :return: A DataFrame with one row per tag per work
    :rtype: pandas DataFrame
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
        labels=['tags', 'Unnamed: 6', 'restricted', 'complete', 'language'], axis=1
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
    works_tags_df = works_tags_df.loc[works_tags_df['type_final'].isin(TAG_TYPES_TO_KEEP)]
    works_tags_df = works_tags_df.query('name_final != "Redacted"').copy()
    return works_tags_df


def aggregate_works_tags_df(works_tags_df, minimum_work_count):
    """
    Attaches fandom to work-level data, tag-level data aggregation
    In future iterations, this could really be done in SQL
    :param works_tags_df: A DataFrame with one row per tag per work
    :type works_tags_df: pandas DataFrame
    :param minimum_work_count: Minimum number of works a fandom must have to be included in analysis
    :type minimum_work_count: int
    :return:
        - One row per fandom per non-fandom tag with count of works
        - One row per work per fandom
        - One row per fandom with count of works
    :rtype:
        - pandas DataFrame
        - pandas DataFrame
        - pandas DataFrame
    """
    works_with_fandom = works_tags_df.query(
        'type_final == "Fandom"'
    )[['work_id', 'name_final', 'word_count', 'creation date']]
    works_with_fandom = works_with_fandom.rename(columns={'name_final': 'fandom_name'})
    logger.info(f"Works before filtering rare fandoms: {len(works_with_fandom['work_id'].unique())}")
    fandom_works_count = works_with_fandom.groupby(by='fandom_name').count()['work_id'].reset_index()
    fandom_works_count.rename(columns={'work_id': 'works_num'}, inplace=True)
    fandom_works_count = fandom_works_count.query(f'work_id < {minimum_work_count}')
    works_with_fandom = works_with_fandom.loc[
        ~works_with_fandom['fandom_name'].isin(fandom_works_count['fandom_name'])
    ]
    logger.info(f"Works after filtering rare fandoms: {len(works_with_fandom['work_id'].unique())}")
    works_tags_df_no_fandom = works_tags_df.query('type_final != "Fandom"')
    works_tags_df_no_fandom = works_tags_df_no_fandom.merge(
        works_with_fandom[['work_id', 'fandom_name']],
        how='inner',
        on='work_id'
    )
    non_fandom_tags_agg = works_tags_df_no_fandom.groupby(
        by=TAG_GROUPBY_LIST
    ).agg(TAG_GROUPBY_AGG).reset_index()
    non_fandom_tags_agg.rename(
        columns={
            'work_id': 'works_num',
            'word_count': 'word_count_mean'
        },
        inplace=True
    )
    return non_fandom_tags_agg, works_with_fandom, fandom_works_count


def standardize_tags(tags_df, cols_to_coalesce):
    """
    Standardizes tags by retrieving canonical tag information for non-canonical tags
        that have a canonical equivalent
    :param tags_df: A DataFrame with tag info, one row per tag
    :type tags_df: pandas DataFrame
    :param cols_to_coalesce: A list of columns in tags_df for which to retrieve
        canonical information if it exists
    :type cols_to_coalesce: list
    :return: A DataFrame with standardized fields listed in cols_to_coalesce
    :rtype: pandas DataFrame
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


def format_number(number):
    """
    Formats number to human readable string
    :param number: Number
    :type number: int
    :return: Formatted number
    :rtype: str
    """
    num = float('{:.3g}'.format(number))
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    return '{}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), ['', 'K', 'M', 'B', 'T'][magnitude])
