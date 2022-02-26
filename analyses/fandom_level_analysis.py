import matplotlib.pyplot as plt
import streamlit as st

from utils import logger, format_number
from fandom import Fandom

FANDOM_ORDER_LU = {
    'Popularity by Work Count': {'by': 'works_num', 'ascending': False},
    'Alphabetically': {'by': 'fandom_name', 'ascending': True},
}


class FandomLevelAnalysis:
    def __init__(
        self, non_fandom_tags_agg, works_with_fandom, fandom_works_count
    ):
        fandom_works_count = fandom_works_count.compute().reset_index()
        col1, col2 = st.columns([1, 2])
        fandom_order = col1.radio(
            'View fandom list ordered by', FANDOM_ORDER_LU
        )
        fandom_select_list = fandom_works_count.sort_values(
            **FANDOM_ORDER_LU[fandom_order]
        )
        fandom_selection = col2.selectbox(
            'Choose fandom (can type to search)', fandom_select_list
        )
        logger.info(f'Initializing fandom class for {fandom_selection}')
        fandom = Fandom(
            fandom_selection, works_with_fandom, non_fandom_tags_agg
        )
        logger.info(f'{fandom_selection} initialized')
        st.markdown(
            f'We found __{format_number(len(fandom.works))}__ '
            f'works to analyze.'
        )
        st.markdown('***')
        st.subheader('Relationship Chord Chart')
        relationship_type = st.radio(
            'Choose relationship type', ['romantic', 'platonic']
        )
        fig_c, ax_c = plt.subplots()
        fandom.generate_relationship_chord_chart(
            relationship_type=relationship_type, ax=ax_c
        )
        st.pyplot(fig_c)
        st.markdown(
            '''
            Inspiration for chart came from the visualizations of 
            [futurephotons](https://www.futurephotons.io/ao3stats/).  
        '''
        )
        with st.expander('Methodology notes'):
            st.markdown(
                '''
                    - Only the most popular 50 relationships are displayed.
                    - Relationships with 2+ characters were counted in their 
                    respective pairs.
                        - For example, a relationship between A/B/C is counted 
                        separately as A/B, A/C, and B/C.  
                '''
            )
        st.markdown('***')
        st.subheader('Word Count Distribution')
        (
            fig_wc,
            mean_word_count,
            median_word_count,
        ) = fandom.word_count_distribution()
        col1, col2 = st.columns(2)
        col1.metric('Mean', mean_word_count)
        col2.metric('Median', median_word_count)
        st.plotly_chart(fig_wc, use_container_width=True)
        st.subheader('Works Over Time')
        fig_ym = fandom.year_month_distribution()
        st.plotly_chart(fig_ym, use_container_width=True)
