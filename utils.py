import logging

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
