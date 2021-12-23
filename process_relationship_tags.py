import csv

import pandas as pd

from utils import (
    preprocess, logger,
    WORKS_CSV, TAGS_CSV, WORKS_TAGS_CSV, WORKS_TAGS_CSV_DTYPES,
    TOP_50_FANDOMS_CSV
)
from fandom import Fandom

# These variables should be passed through in the CLI
FANDOM = '陈情令 | The Untamed (TV)'
FLAG_PREPROCESS = False

if __name__ == '__main__':
    if FLAG_PREPROCESS:
        # Retrieve data
        logger.info('Preprocessing data')
        logger.info('Retrieving works_df')
        works_df = pd.read_csv(WORKS_CSV)
        logger.info('Retrieving tags_df')
        tags_df = pd.read_csv(TAGS_CSV, index_col='id')
        # Notes: Takes 19 minutes to process entire dataset
        works_tags_df = preprocess(works_df, tags_df)
        logger.info(f'Saving preprocessed works tags data to {WORKS_TAGS_CSV}')
        works_tags_df.to_csv(WORKS_TAGS_CSV, index=False)
        top_50_fandoms = list(tags_df.query(
            'type == "Fandom"'
        ).sort_values(
            by='cached_count', ascending=False
        ).head(50)['name'])
        top_50_fandoms = [[s] for s in top_50_fandoms]
        logger.info(f'Saving list of top 50 fandoms to {TOP_50_FANDOMS_CSV}')
        with open(TOP_50_FANDOMS_CSV, 'w') as f:
            write = csv.writer(f)
            write.writerows(top_50_fandoms)
    else:
        logger.info('Loading previously preprocessed data')
        # Test replacing this with the other IO library
        # Currently takes 3 minutes
        top_50_fandoms = [s.strip() for s in open(TOP_50_FANDOMS_CSV).readlines()]
        works_tags_df = pd.read_csv(WORKS_TAGS_CSV, dtype=WORKS_TAGS_CSV_DTYPES)
        works_tags_df['creation date'] = pd.to_datetime(
            works_tags_df['creation date'], format='%Y-%m-%d'
        )
    # Test new class
    logger.info(f'Initializing fandom class for {FANDOM}')
    fandom = Fandom(FANDOM, works_tags_df)
    fandom.generate_relationship_chord_chart()
    logger.info(f'Done. Chord chart saved')
