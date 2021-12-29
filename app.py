import streamlit as st

from utils import retrieve_preprocessed_data
from analyses.fandom_level_analysis import FandomLevelAnalysis
from analyses.inter_fandom_analysis import InterFandomAnalysis

ANALYSIS_TYPES = {
    'Fandom Level': FandomLevelAnalysis,
    'Inter-fandom': InterFandomAnalysis,
}


def run():
    st.set_page_config(
        initial_sidebar_state='expanded',
        page_icon=':goblin:'
    )
    non_fandom_tags_agg, works_with_fandom, fandom_works_count = \
        retrieve_preprocessed_data()
    st.title('AO3 Data Visualizations')
    st.markdown('''
        This page displays some charts examining the works on AO3 as of Feb 
        26th, 2021. Please use the sidebar menu to select a type of analysis. 
        Source and general methodology are described at the bottom of the page, 
        while chart-specific notes are found below each chart. 
    ''')
    st.markdown('***')
    analysis_type = st.sidebar.radio('Choose an analysis', ANALYSIS_TYPES)
    # Initializes the class with the analysis
    ANALYSIS_TYPES[analysis_type](
        non_fandom_tags_agg, works_with_fandom, fandom_works_count
    )
    st.markdown('***')
    with st.expander('General methodology notes'):
        st.markdown('''
                __General__\n
                - Only fandoms with at least 100 works at the time of data 
                collection are included in the analysis.
                - When selecting a fandom, please note that works with 
                equivalent tags are included, but not works with subtags. 
                For example, in the "DCU" tag, "DCU (Animated)" is an 
                equivalent tag, but "Birds of Prey (TV)" is a subtag. 
                This is a little different from the AO3 website, 
                which seems to include all works with equivalent tags and 
                subtags for a fandom. While I would have liked to replicate 
                that, I am not able to link parent and child tags with the 
                tag information provided in the data dump. 
                - Some works seem to be tagged with tags with redacted names. 
                In those cases, I tried to match the tag with any equivalent 
                tags. However, if one could not be found, I dropped the tag 
                entirely.
        ''')
    with st.expander('Source'):
        st.markdown('''
        The source data was provided by 
        AO3 in their [March 2021 data dump]
        (https://archiveofourown.org/admin_posts/18804).
        ''')


if __name__ == '__main__':
    run()
