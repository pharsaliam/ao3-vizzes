import itertools

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from mpl_chord_diagram import chord_diagram

from utils import format_thousand

PLT_RC_PARAMS = {
    'figure.figsize': (20, 10),
    'axes.labelsize': 24,
    'xtick.labelsize': 15,
    'ytick.labelsize': 15,
}
plt.style.use('ggplot')
for k,v in PLT_RC_PARAMS.items():
    plt.rcParams[k] = v

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
        # self.freeform_tags = self.retrieve_tags_by_type(works_tags_df, 'Freeform')

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

    def word_count_distribution(
            self, low_wc_upper_boundary=5000, low_wc_step=1000,
            high_wc_upper_boundary=100000, high_wc_step=5000
    ):
        wc_bins, wc_bin_labels = self.generate_word_count_bins(
            low_wc_upper_boundary, low_wc_step, high_wc_upper_boundary, high_wc_step
        )
        works_df = self.works.copy()
        mean_word_count = int(works_df['word_count'].mean())
        median_word_count = int(works_df['word_count'].median())
        works_df['word_count_bin'] = (
            pd.cut(works_df['word_count'],
                   wc_bins, labels=wc_bin_labels, include_lowest=True)
        )
        ax = works_df.groupby(by='word_count_bin')['work_id'].count().plot(kind='bar')
        plt.xlabel('Word Count')
        plt.ylabel('Number of Works')
        plt.xticks(rotation=45, ha='right')
        return ax, mean_word_count, median_word_count

    @staticmethod
    def generate_word_count_bins(
            low_wc_upper_boundary=5000, low_wc_step=1000,
            high_wc_upper_boundary=100000, high_wc_step=5000
    ):
        low_wc_bins = list(np.arange(0, low_wc_upper_boundary, low_wc_step))
        high_wc_bins = list(np.arange(low_wc_upper_boundary, high_wc_upper_boundary, high_wc_step))
        wc_bins = low_wc_bins + high_wc_bins
        wc_bins.append(np.inf)
        wc_bins_labels = [
            f'{format_thousand(wc_bins[i])} to {format_thousand(wc_bins[i + 1])}'
            for i in np.arange(1, len(wc_bins)-2)
        ]
        wc_bins_labels.append('>' + format_thousand(high_wc_upper_boundary))
        wc_bins_labels.insert(0, '<' + format_thousand(low_wc_step))
        return wc_bins, wc_bins_labels
