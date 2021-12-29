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

st.title('AO3 Data Visualizations')
st.markdown('''
This page displays some summary charts of the works on AO3 for a selected fandom, as of Feb. 26th, 2021. 
 The data used was provided by AO3 in their [March 2021 data dump](https://archiveofourown.org/admin_posts/18804).
 Methodology notes are contained at the bottom of the page. 
''')
non_fandom_tags_agg, works_with_fandom, fandom_works_count = retrieve_preprocessed_data()
fandom_works_count = fandom_works_count.reset_index()
col1, col2 = st.columns([1, 2])
fandom_order = col1.radio('View fandom list ordered by', [s for s in FANDOM_ORDER_LU.keys()])
fandom_select_list = fandom_works_count.sort_values(**FANDOM_ORDER_LU[fandom_order])
FANDOM = col2.selectbox('Choose fandom', fandom_select_list)
logger.info(f'Initializing fandom class for {FANDOM}')
fandom = Fandom(FANDOM, works_with_fandom, non_fandom_tags_agg)
logger.info(f'{FANDOM} initialized')
st.markdown(f'We found __{format_number(len(fandom.works))}__ fics to analyze.')
st.subheader('Relationship Chord Chart')
relationship_type = st.radio('Choose relationship type', ['romantic', 'platonic'])
fig_c, ax_c = plt.subplots()
fandom.generate_relationship_chord_chart(relationship_type=relationship_type, ax=ax_c)
st.pyplot(fig_c)
st.markdown('''
Inspiration for chart came from the visualizations of [futurephotons](https://www.futurephotons.io/ao3stats/).  
''')
st.subheader('Word Count Distribution')
fig, ax = plt.subplots()
ax, mean_word_count, median_word_count = fandom.word_count_distribution()
col1, col2 = st.columns(2)
col1.metric('Mean', mean_word_count)
col2.metric('Median', median_word_count)
st.pyplot(fig)
with st.expander('Methodology notes'):
    st.markdown('''
            __General__\n
            - Only fandoms with at least 100 works at the time of data collection are included in the analysis.
            - When selecting a fandom, please note that works with equivalent tags are included, but not works with
            subtags. For example, in the "DCU" tag, "DCU (Animated)" is an equivalent tag, but "Birds of Prey (TV)"
            is a subtag. This is a little different from the AO3 website, which seems to include all works with
            equivalent tags and subtags for a fandom. While I would have liked to replicate that, I am not able
            to link parent and child tags with the tag information provided in the data dump. 
            - Some works seem to be tagged with tags with redacted names. In those cases, I tried to match the 
            tag with any equivalent tags. However, if one could not be found, I dropped the tag entirely.
            __Relationship chord chart__\n
            - Only the most popular 50 relationships are displayed.
            - Relationships with 2+ characters were counted in their respective pairs.
                - For example, a relationship between A/B/C is counted separately as A/B, A/C, and B/C.  
        ''')

