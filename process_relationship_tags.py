import pandas as pd

from utils import (
    preprocess,
    logger, WORKS_CSV, TAGS_CSV, FANDOM
)

if __name__ == '__main__':
    # Retrieve data
    logger.info('Retrieving works_df')
    works_df = pd.read_csv(WORKS_CSV)
    logger.info('Retrieving tags_df')
    tags_df = pd.read_csv(TAGS_CSV, index_col='id')
    # Notes: Takes 19 minutes to process entire dataset
    works_tags_df = preprocess(works_df, tags_df)
    # Filter for fandom
    logger.debug(f'Filtering for fandom: {FANDOM}')
    works_in_fandom = works_tags_df.query(
        f'type_final == "Fandom" and name_final == \"{FANDOM}\"'
    )[['work_id']].drop_duplicates()
    relationship_tags_in_fandom = works_tags_df.query(
        'type_final == "Relationship"'
    ).merge(
        works_in_fandom, how='inner', on='work_id'
    )
    logger.info(f'Done-- There are {len(relationship_tags_in_fandom)} relationship tags')
