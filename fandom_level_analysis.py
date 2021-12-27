import matplotlib.pyplot as plt
import streamlit as st

from utils import (
    logger, retrieve_preprocessed_data, format_number
)
from fandom import Fandom

FANDOM_ORDER_LU = {
    'Popularity by Work Count': {'by': 'works_num', 'ascending': False},
    'Alphabetically': {'by': 'fandom_name', 'ascending': True}
}


non_fandom_tags_agg, works_with_fandom, fandom_works_count = retrieve_preprocessed_data()
fandom_order = st.radio('View fandom list ordered by', [s for s in FANDOM_ORDER_LU.keys()])
fandom_select_list = fandom_works_count.sort_values(**FANDOM_ORDER_LU[fandom_order])
FANDOM = st.selectbox('Choose fandom', fandom_select_list)
logger.info(f'Initializing fandom class for {FANDOM}')
fandom = Fandom(FANDOM, works_with_fandom, non_fandom_tags_agg)
logger.info(f'{FANDOM} initialized')
st.text(f'We found {format_number(len(fandom.works))} fics to analyze.')
st.subheader('Relationship Chord Chart')
fig_c, ax_c = plt.subplots()
fandom.generate_relationship_chord_chart(ax=ax_c)
st.pyplot(fig_c)
st.subheader('Word Count Distribution')
fig, ax = plt.subplots()
ax, mean_word_count, median_word_count = fandom.word_count_distribution()
st.text(f'''
The mean word count for {FANDOM} fics is {mean_word_count}.
The median word count for {FANDOM} fics is {median_word_count}
''')
st.pyplot(fig)


