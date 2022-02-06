import pandas as pd

from utils import (
    logger,
    WORKS_CSV,
    TAGS_CSV,
    WORKS_TAGS_PARQUET,
    NON_FANDOM_TAGS_AGG_LOC,
    WORKS_WITH_FANDOM_LOC,
    FANDOM_WORKS_COUNT_LOC,
    DATA_DIRECTORY,
    MINIMUM_WORK_COUNT,
    TAG_TYPES_TO_KEEP,
    TO_PARQUET_CONFIG,
    TAG_GROUPBY_AGG,
    TAG_GROUPBY_LIST,
)


def preprocess_data(
    works_csv_location=WORKS_CSV,
    tags_csv_location=TAGS_CSV,
    minimum_work_count=MINIMUM_WORK_COUNT,
    flag_save_works_tags_df=True,
):
    """
    Preprocesses raw AO3 data dump. If flag_save_data=True, will save data as
        parquet files
    :param works_csv_location: Location of the AO3 data dump works CSV
    :type works_csv_location: str
    :param tags_csv_location: Location of AO3 data dump tags CSV
    :type tags_csv_location: str
    :param works_tags_location: Location of file with one row per work per tag
    :type works_tags_location: str
    :param tags_aggregated_locations: Location of file with aggregated
        non-fandom tag data
    :type tags_aggregated_locations: str
    :param works_with_fandom_locations: Location of file with one row per work
        per fandom after filtering out rare fandoms
    :type works_with_fandom_locations: str
    :param fandom_count_location: Location of file with aggregated fandom work
        count data
    :type fandom_count_location: str
    :param minimum_work_count: Minimum number of works a fandom must have to
        be included in analysis
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
    (
        non_fandom_tags_agg,
        works_with_fandom,
        fandom_works_count,
    ) = aggregate_works_tags_df(works_tags_df, minimum_work_count)
    save_data_to_parquet(
        non_fandom_tags_agg, NON_FANDOM_TAGS_AGG_LOC
    )
    save_data_to_parquet(
        works_with_fandom, WORKS_WITH_FANDOM_LOC
    )
    save_data_to_parquet(
        fandom_works_count, FANDOM_WORKS_COUNT_LOC
    )
    if flag_save_works_tags_df:
        save_data_to_parquet(
            works_tags_df, WORKS_TAGS_PARQUET
        )
    return None


def save_data_to_parquet(df, file_location):
    df.to_parquet(file_location, **TO_PARQUET_CONFIG)
    logger.info(f'Data saved to {file_location}')
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
    works_tags_df = (
        works_df.drop(
            labels=[
                'tags',
                'Unnamed: 6',
                'restricted',
                'complete',
                'language',
            ],
            axis=1,
        )
        .explode('tags_list')
        .reset_index()
        .rename(columns={'tags_list': 'tag_id', 'index': 'work_id'})
    )
    works_tags_df['tag_id'] = works_tags_df['tag_id'].fillna(-999).astype(int)
    works_tags_df = works_tags_df.merge(
        tags_df_merger, how='left', left_on='tag_id', right_index=True
    )
    works_tags_df = works_tags_df.loc[
        works_tags_df['type_final'].isin(TAG_TYPES_TO_KEEP)
    ]
    works_tags_df = works_tags_df.query('name_final != "Redacted"').copy()
    return works_tags_df


def aggregate_works_tags_df(works_tags_df, minimum_work_count):
    """
    Attaches fandom to work-level data, tag-level data aggregation
    In future iterations, this could really be done in SQL
    :param works_tags_df: A DataFrame with one row per tag per work
    :type works_tags_df: pandas DataFrame
    :param minimum_work_count: Minimum number of works a fandom must have to be
     included in analysis
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
    works_with_fandom = works_tags_df.query('type_final == "Fandom"')[
        ['work_id', 'name_final', 'word_count', 'creation date']
    ]
    works_with_fandom = works_with_fandom.rename(
        columns={'name_final': 'fandom_name'}
    ).drop_duplicates()
    fandom_works_count = (
        works_with_fandom.groupby(by='fandom_name')
        .count()['work_id']
        .reset_index()
    )
    fandom_works_count.rename(columns={'work_id': 'works_num'}, inplace=True)
    fandom_works_count = fandom_works_count.query(
        f'works_num > {minimum_work_count}'
    )
    works_with_fandom = works_with_fandom.loc[
        works_with_fandom['fandom_name'].isin(
            fandom_works_count['fandom_name']
        )
    ]
    works_tags_df_no_fandom = works_tags_df.query(
        'type_final != "Fandom"'
    ).drop(columns='tag_id')
    works_tags_df_no_fandom = works_tags_df_no_fandom.drop_duplicates()
    works_tags_df_no_fandom = works_tags_df_no_fandom.merge(
        works_with_fandom[['work_id', 'fandom_name']],
        how='inner',
        on='work_id',
    )
    non_fandom_tags_agg = (
        works_tags_df_no_fandom.groupby(by=TAG_GROUPBY_LIST)
        .agg(TAG_GROUPBY_AGG)
        .reset_index()
    )
    non_fandom_tags_agg.rename(
        columns={'work_id': 'works_num', 'word_count': 'word_count_mean'},
        inplace=True,
    )
    works_with_fandom = works_with_fandom.set_index(
        ['fandom_name', 'work_id']
    ).sort_index()
    non_fandom_tags_agg = non_fandom_tags_agg.set_index(
        ['fandom_name', 'type_final']
    ).sort_index()
    fandom_works_count = fandom_works_count.set_index(
        'fandom_name'
    ).sort_index()
    non_fandom_tags_agg = use_efficient_dtypes(non_fandom_tags_agg)
    works_with_fandom = use_efficient_dtypes(works_with_fandom)
    fandom_works_count = use_efficient_dtypes(fandom_works_count)
    return non_fandom_tags_agg, works_with_fandom, fandom_works_count


def use_efficient_dtypes(df_u):
    df = df_u.copy()
    for col in df.columns:
        if pd.api.types.is_integer_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], downcast='integer')
        elif pd.api.types.is_float_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], downcast='float')
        elif (
                (pd.api.types.is_object_dtype(df[col]))
                & ('date' in col)
        ):
            df[col] = pd.to_datetime(df[col], infer_datetime_format=True)
        elif pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].astype('string[pyarrow]')
    return df


def standardize_tags(tags_df, cols_to_coalesce):
    """
    Standardizes tags by retrieving canonical tag information for non-canonical
        tags that have a canonical equivalent
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
        validate='many_to_one',
    )
    cols_final = []
    for col in cols_to_coalesce:
        tags_df_std[f'{col}_final'] = tags_df_std[f'{col}_merg'].combine_first(
            tags_df_std[f'{col}_orig']
        )
        cols_final.append(f'{col}_final')

    tags_df_std = tags_df_std[cols_final].copy()

    return tags_df_std


if __name__ == '__main__':
    # This took 40 minutes to run....
    # Mostly because exploding works took 30 minutes
    preprocess_data()
