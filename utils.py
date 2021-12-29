import logging

import pandas as pd
import streamlit as st

LOGGING_LEVEL = logging.INFO
WORKS_CSV = 'data/ao3_official_dump_210321/works-20210226.csv'
TAGS_CSV = 'data/ao3_official_dump_210321/tags-20210226.csv'
WORKS_TAGS_PARQUET = 'not_added_to_git/preprocessed_works_tags.parquet.gzip'
WWF_CHUNK_NUM = 2
WORKS_WITH_FANDOM_LOCATIONS = [f'data/works_with_fandom_{i}.parquet.gzip' for i in range(WWF_CHUNK_NUM)]
TA_CHUNK_NUM = 3
TAGS_AGGREGATED_LOCATIONS = [f'data/non_fandoms_tags_aggregated_{i}.parquet.gzip' for i in range(TA_CHUNK_NUM)]
FANDOM_WORKS_COUNT_PARQUET = 'data/fandom_works_count.parquet.gzip'
TAG_TYPES_TO_KEEP = ['Relationship', 'Freeform', 'ArchiveWarnings', 'Rating', 'Fandom']
MINIMUM_WORK_COUNT = 100
TAG_GROUPBY_LIST = ['fandom_name', 'name_final', 'type_final']
TAG_GROUPBY_AGG = {
    'work_id': 'count',
    'word_count': 'mean'
}
TO_PARQUET_CONFIG = {'index': 'False', 'compression': 'gzip'}

logger = logging.getLogger('LOG')
logger.setLevel(LOGGING_LEVEL)
ch = logging.StreamHandler()
ch.setLevel(LOGGING_LEVEL)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'
)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.propagate = False


@st.experimental_memo()
def retrieve_preprocessed_data(
        tags_aggregated_locations=TAGS_AGGREGATED_LOCATIONS,
        works_with_fandom_locations=WORKS_WITH_FANDOM_LOCATIONS,
        fandom_count_location=FANDOM_WORKS_COUNT_PARQUET,
):
    """
    Loads previously saved preprocessed and aggregated data
    :param tags_aggregated_locations: Location of the aggregated non-fandom tags data
    :type tags_aggregated_locations: str
    :param works_with_fandom_locations: Location of the works with fandom data
    :type works_with_fandom_locations: str
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
    non_fandom_tags_agg = pd.DataFrame()
    non_fandom_tags_agg = concat_data(tags_aggregated_locations, non_fandom_tags_agg)
    works_with_fandom = pd.DataFrame()
    works_with_fandom = concat_data(works_with_fandom_locations, works_with_fandom)
    fandom_works_count = pd.read_parquet(fandom_count_location)
    # TODO Remove this once the data files get refreshed
    works_with_fandom = works_with_fandom.set_index(['fandom_name', 'work_id']).sort_index()
    non_fandom_tags_agg = non_fandom_tags_agg.set_index(['fandom_name', 'type_final']).sort_index()
    fandom_works_count = fandom_works_count.set_index('fandom_name').sort_index()
    logger.info('Finished loading data')
    return non_fandom_tags_agg, works_with_fandom, fandom_works_count


def concat_data(file_locations, final_df):
    """
    Reads multiple parquet files and concatenates into one DataFrame
    :param file_locations: List of parquet file locations
    :type file_locations: list
    :param final_df: empty DataFrame
    :type final_df: pandas DataFrame
    :return: DataFrame with data from the parquet files
    :rtype: pandas DataFrame
    """
    for file in file_locations:
        df = pd.read_parquet(file)
        final_df = pd.concat([final_df, df])
    return final_df


# TODO Replace this with millify or numerize?
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
