from utils import (
    logger, load_data
)
from fandom import Fandom

# These variables should be passed through in the CLI
FANDOM = '陈情令 | The Untamed (TV)'
FLAG_PREPROCESS = False

if __name__ == '__main__':
    works_tags_df, top_50_fandoms = load_data(flag_preprocess=FLAG_PREPROCESS)
    # Test new class
    logger.info(f'Initializing fandom class for {FANDOM}')
    fandom = Fandom(FANDOM, works_tags_df)
    fandom.generate_relationship_chord_chart()
    logger.info(f'Done. Chord chart saved')
