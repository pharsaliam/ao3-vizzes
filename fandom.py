import itertools

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.colors import LinearSegmentedColormap
from mpl_chord_diagram import chord_diagram
import plotly.express as px

from utils import (
    format_number, TAG_TYPES_TO_KEEP, PX_TEMPLATE,
    PX_FONT_SIZE_AXES, PX_FONT_SIZE_TICKS
)

PLT_RC_PARAMS = {
    'figure.figsize': (20, 10),
    'axes.labelsize': 24,
    'xtick.labelsize': 15,
    'ytick.labelsize': 15,
}
plt.style.use('ggplot')
for k, v in PLT_RC_PARAMS.items():
    plt.rcParams[k] = v
colors = [
    'lightseagreen',
    'tomato',
    'gold',
    'cornflowerblue',
    'violet',
    'lawngreen',
    'lightskyblue',
    'darkorange',
]
custom_cmap = LinearSegmentedColormap.from_list('mycmap', colors)

RELATIONSHIP_SEPARATOR_LU = {'romantic': '/', 'platonic': '&'}
CHARACTER_COLUMN_NAMES = ['char_1', 'char_2']


class Fandom:
    def __init__(self, name, works_with_fandom, non_fandom_tags_agg):
        self.name = name
        non_fandom_tags_agg_for_fandom = non_fandom_tags_agg.loc[name, :].compute().droplevel('fandom_name')
        self.works = works_with_fandom.loc[self.name].compute()
        self.relationships = self.retrieve_tags_by_type(
            non_fandom_tags_agg_for_fandom, 'Relationship'
        )
        relationship_conditions = [
            np.array(self.relationships['relationship_name'].str.contains(split), dtype=bool)
            for split in RELATIONSHIP_SEPARATOR_LU.values()
        ]
        self.relationships['relationship_type'] = np.select(
            relationship_conditions,
            [rel_type for rel_type in RELATIONSHIP_SEPARATOR_LU],
        )
        self.freeform_tags = self.retrieve_tags_by_type(
            non_fandom_tags_agg_for_fandom, 'Freeform'
        )

    @staticmethod
    def retrieve_tags_by_type(non_fandom_tags_agg, tag_type):
        """
        Retrieve works_tags_df by tag type
        :param non_fandom_tags_agg: DataFrame containing aggregated non-fandom
            tag data
        :param tag_type: Type of non-Fandom tag (see TAG_TYPES_TO_KEEP in utils
            for full list)
        :return: Subset of works_tags_df that only contains the specified tag
            type
        """
        assert tag_type in [s for s in TAG_TYPES_TO_KEEP if s != 'Fandom']
        df = (
            non_fandom_tags_agg.loc[tag_type]
            .rename(columns={'name_final': f'{tag_type.lower()}_name'})
            .reset_index(drop=True)
        )
        return df

    def generate_relationship_chord_chart(
        self, relationship_type='romantic', top_n=50, ax=None, save_fig=False
    ):
        """
        Generates chord chart for relationships
        :param relationship_type: 'romantic' or 'platonic'
        :param top_n: Top N relationships by number of fics to display
        :param ax: Matplotlib axis where the plot should be drawn.
        :param save_fig: Boolean indicating whether or not to save the chord
            chart as image
        :return: None
        """
        assert relationship_type in ('romantic', 'platonic')
        rel_df = self.parse_relationships_to_characters(relationship_type)
        rel_df.drop(columns='word_count_mean', inplace=True)
        # Need to group again because of how we parse poly pairings
        rel_count = (
            rel_df.groupby(by=CHARACTER_COLUMN_NAMES)
            .sum()[['works_num']]
            .reset_index()
        )
        rel_count = rel_count.sort_values(
            by='works_num', ascending=False
        ).head(top_n)
        # Issue is right now 'A/B' is counted differently from 'B/A'.
        # In some cases, 'B/A' won't even exist, so we force it
        matrix = rel_count.pivot(
            index=CHARACTER_COLUMN_NAMES[0],
            columns=CHARACTER_COLUMN_NAMES[1],
            values='works_num',
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
            cmap=custom_cmap,
            ax=ax,
        )
        if save_fig:
            plt.savefig(
                f'not_added_to_git/images/{self.name.replace(" ", "_")}'
                f'_chord_chart.png'
            )
        return None

    def parse_relationships_to_characters(self, relationship_type):
        """
        Parses the characters in relationships
        Note that currently poly relationships are split to pairs
            (e.g., A/B/C -> A/B, A/C, B/C)
        :param relationship_type: 'romantic' or 'platonic'
        :return: relationship dataframe with two new columns indicating the
            characters in that ship
        """
        # Filtering by relationship type
        relationship_split = RELATIONSHIP_SEPARATOR_LU[relationship_type]
        rel_df = self.relationships.query(
            f'relationship_type == "{relationship_type}"'
        ).copy()
        rel_df['relationship_chars'] = rel_df.relationship_name.str.split(
            relationship_split
        )
        # Assess the number of characters in the ship to filter poly ships
        rel_df['relationship_chars_combo'] = rel_df[
            'relationship_chars'
        ].apply(lambda x: list(itertools.combinations(x, 2)))
        rel_df = rel_df.explode('relationship_chars_combo')
        character_column_names = CHARACTER_COLUMN_NAMES
        rel_df[character_column_names] = pd.DataFrame(
            rel_df['relationship_chars_combo'].tolist(), index=rel_df.index
        )
        for col in character_column_names:
            # Clean up character names so ' Gamora (Marvel)' becomes 'Gamora'
            rel_df[col] = rel_df[col].str.strip()
            rel_df[col] = rel_df[col].str.replace(' \(.+\)', '', regex=True)

        return rel_df

    def word_count_distribution(
        self,
        low_wc_upper_boundary=5000,
        low_wc_step=1000,
        high_wc_upper_boundary=100000,
        high_wc_step=5000,
    ):
        """
        Provides visualizations, statistics on binned word count distribution
        :param low_wc_upper_boundary: Upper boundary for fic to be considered
            "low" word count
        :param low_wc_step: Bin steps for low word count bins
        :param high_wc_upper_boundary: Upper boundary for bins
            (e.g., words counts greater than this will all be lumped together
            into the last bin)
        :param high_wc_step: Bin steps for high word count bins
        :return: Plotly figure containing the binned word count distribution
            Mean word count
            Median word count
        """
        wc_bins, wc_bin_labels = self.generate_word_count_bins(
            low_wc_upper_boundary,
            low_wc_step,
            high_wc_upper_boundary,
            high_wc_step,
        )
        works_df = self.works.copy()
        mean_word_count = int(works_df['word_count'].mean())
        median_word_count = int(works_df['word_count'].median())
        works_df['word_count_bin'] = pd.cut(
            works_df['word_count'],
            wc_bins,
            labels=wc_bin_labels,
            include_lowest=True,
        )
        works_grouped_wc = works_df.groupby(
            by=['word_count_bin']
        )[['word_count']].count()
        fig = px.bar(
            works_grouped_wc,
            labels={
                'value': 'Number of Works',
                'word_count_bin': 'Number of Words'
            },
            hover_data={'variable': False},
            template=PX_TEMPLATE,
        )
        fig.update_traces(
            marker_color='green',
            hovertemplate="Number of Words: %{x}<extra></extra><br>"
                          + "Number of Works: %{y:.3s}",
        )
        fig.update_layout(
            yaxis=dict(tickformat="~s"),
            font=dict(
                size=PX_FONT_SIZE_TICKS,
            ),
            showlegend=False
        )
        fig.update_xaxes(
            tickangle=315,
            rangeselector_font_size=PX_FONT_SIZE_AXES,
        )
        return fig, mean_word_count, median_word_count

    @staticmethod
    def generate_word_count_bins(
        low_wc_upper_boundary=5000,
        low_wc_step=1000,
        high_wc_upper_boundary=100000,
        high_wc_step=5000,
    ):
        """
        Generates word count bins and bin labels
        :param low_wc_upper_boundary: Upper boundary for fic to be considered
            "low" word count
        :param low_wc_step: Bin steps for low word count bins
        :param high_wc_upper_boundary: Upper boundary for bins
            (e.g., words counts greater than this will all be lumped together
            into the last bin)
        :param high_wc_step: Bin steps for high word count bins
        :return: Word count bins and bin labels
        """
        low_wc_bins = list(np.arange(0, low_wc_upper_boundary, low_wc_step))
        high_wc_bins = list(
            np.arange(
                low_wc_upper_boundary, high_wc_upper_boundary, high_wc_step
            )
        )
        wc_bins = low_wc_bins + high_wc_bins
        wc_bins.append(np.inf)
        wc_bins_labels = [
            f'{format_number(wc_bins[i])} to {format_number(wc_bins[i + 1])}'
            for i in np.arange(1, len(wc_bins) - 2)
        ]
        wc_bins_labels.append('>' + format_number(high_wc_upper_boundary))
        wc_bins_labels.insert(0, '<' + format_number(low_wc_step))
        return wc_bins, wc_bins_labels

    def year_month_distribution(self):
        self.works['creation_month'] = self.works['creation date'] + pd.offsets.MonthBegin(-1)
        works_grouped_ym = self.works.groupby(
            by=['creation_month']
        )[['word_count']].count()
        fig = px.bar(
            works_grouped_ym,
            labels={
                'value': 'Number of Works',
                'creation_month': 'Month Created'
            },
            hover_data={'variable': False},
            template=PX_TEMPLATE
        )
        fig.update_layout(
            yaxis=dict(tickformat="~s"),
            font=dict(
                size=PX_FONT_SIZE_TICKS,
            ),
            showlegend=False
        )
        fig.update_traces(
            marker_color='#6eaf28',
        )
        return fig
