import itertools

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mpl_chord_diagram import chord_diagram

plt.rcParams["figure.figsize"] = (20, 10)

RELATIONSHIP_SEPARATOR_LU = {
    'romantic': '/',
    'platonic': '&'
}
CHARACTER_COLUMN_NAMES = ['char_1', 'char_2']


class Fandom:
    def __init__(self, name, works_tags_df):
        self.name = name
        self.works = works_tags_df.query(
            f'type_final == "Fandom" and name_final == \"{name}\"'
        )[['work_id', 'creation date', 'language', 'complete', 'word_count']].drop_duplicates()
        self.relationships = self.retrieve_tags_by_type(works_tags_df, 'Relationship')
        relationship_conditions = [
            self.relationships['relationship_name'].str.contains(split)
            for split in RELATIONSHIP_SEPARATOR_LU.values()
        ]
        self.relationships ['relationship_type'] = np.select(
            relationship_conditions,
            [rel_type for rel_type in RELATIONSHIP_SEPARATOR_LU]
        )
        self.freeform_tags = self.retrieve_tags_by_type(works_tags_df, 'Freeform')

    def retrieve_tags_by_type(self, works_tags_df, tag_type):
        df = works_tags_df.query(
            f'type_final == "{tag_type}"'
        ).merge(
            self.works['work_id'], how='inner', on='work_id'
        ).rename(columns={
            'name_final': f'{tag_type.lower()}_name',
            'canonical_final': 'is_tag_canonical'
            }
        ).drop(
            columns=['tag_id', 'type_final']
        )
        return df

    def generate_relationship_chord_chart(self, relationship_type='romantic', top_n=50):
        rel_df = self.parse_relationships_to_characters(relationship_type)
        rel_count = rel_df.groupby(CHARACTER_COLUMN_NAMES).agg({'work_id': 'count'}).reset_index()
        rel_count = rel_count.sort_values(by='work_id', ascending=False).head(top_n)
        # Issue is right now 'A/B' is counted differently from 'B/A'.
        # In some cases, 'B/A' won't even exist, but it needs to for the chord chart to work
        matrix = rel_count.pivot(
            index=CHARACTER_COLUMN_NAMES[0], columns=CHARACTER_COLUMN_NAMES[1], values='work_id'
        )
        # Getting total list of unique characters
        characters = np.unique(rel_count[CHARACTER_COLUMN_NAMES].values)
        # Reindexing so both rows and columns contain all characters
        matrix = matrix.reindex(index=characters, columns=characters)
        # Make it so the value of 'A/B' equals the value of 'B/A'
        matrix = matrix.add(matrix.T, fill_value=0)
        matrix.fillna(0, inplace=True)
        chord_diagram(
            matrix,
            names=matrix.columns,
            rotate_names=[True for c in matrix.columns],
            use_gradient=True,
            cmap='tab20'
        )
        plt.ioff()
        plt.savefig(f'{self.name.replace(" ", "_")}_chord_chart.png')
        return None

    def parse_relationships_to_characters(self, relationship_type):
        assert relationship_type in ('romantic', 'platonic')
        # Filtering by relationship type
        relationship_split = RELATIONSHIP_SEPARATOR_LU[relationship_type]
        rel_df = self.relationships.query(f'relationship_type == "{relationship_type}"').copy()
        rel_df['relationship_chars'] = rel_df.relationship_name.str.split(relationship_split)
        # Assess the number of characters in the ship to filter poly ships
        # At this point, we are going with the route where we split poly ships to their pairs
        rel_df['relationship_chars_combo'] = rel_df['relationship_chars'].apply(
            lambda x: list(itertools.combinations(x, 2)))
        rel_df = rel_df.explode('relationship_chars_combo')
        character_column_names = CHARACTER_COLUMN_NAMES
        rel_df[character_column_names] = pd.DataFrame(rel_df['relationship_chars_combo'].tolist(), index=rel_df.index)
        for col in character_column_names:
            # Clean up character names so ' Gamora (Marvel)' becomes 'Gamora'
            rel_df[col] = rel_df[col].str.strip()
            rel_df[col] = rel_df[col].str.replace(' \(.+\)', '', regex=True)

        return rel_df
