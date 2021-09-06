from functools import partial
import logging

# import numpy as np
import pandas as pd

from utils import create_tag_dict, parse_tags_by_type

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

if __name__ == '__main__':
    # Retrieve data
    logging.info('Retrieving works_df')
    works_df = pd.read_csv('ao3_official_dump_210321/works-20210226.csv')
    works_df = works_df.drop(['Unnamed: 6'], axis=1)
    logging.info('Retrieving tags_df')
    tags_df = pd.read_csv('ao3_official_dump_210321/tags-20210226.csv', index_col='id')
    # TEMP -- TEST ON LIMITED DATA
    # Notes: 1M rows processed in ~18 minutes...
    works_df = works_df.head(100).copy()
    # Process tags into list and tag dictionary
    logging.debug('Splitting tags into a list')
    works_df['tags_list'] = works_df['tags'].str.strip().str.split('+')
    create_tag_dict_partial = partial(create_tag_dict, tag_lookup_table=tags_df)
    logging.info('Creating tag dictionary')
    works_df['tags_dict'] = works_df['tags_list'].apply(create_tag_dict_partial)
    # Retrieves list of tag names
    tag_types = list(tags_df.type.unique())
    logging.info('Parsing tags by tag type')
    for tag_type in tag_types:
        parse_tags_by_type_partial = partial(parse_tags_by_type, tag_type=tag_type, tag_lookup_table=tags_df)
        works_df[tag_type] = works_df['tags_dict'].apply(parse_tags_by_type_partial)
    works_df_export_name = 'works_df_with_parsed_tags_test.csv'
    works_df.to_csv(works_df_export_name, index=False)
    logging.info(f'Processed data exported to {works_df_export_name}')
