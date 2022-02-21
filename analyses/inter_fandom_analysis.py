import streamlit as st
import pandas as pd
import plotly.express as px


class InterFandomAnalysis:
    def __init__(
        self, non_fandom_tags_agg, works_with_fandom, fandom_works_count
    ):
        idx = pd.IndexSlice
        non_fandom_tags_agg = non_fandom_tags_agg.compute()
        fandom_works_count = fandom_works_count.compute()
        rel_tags = non_fandom_tags_agg.loc[idx[:, ['Relationship']], :]
        rel_tags = rel_tags.droplevel('type_final', axis=0).drop(
            columns='word_count_mean'
        )
        rel_tags['rn'] = rel_tags.groupby('fandom_name')['works_num'].rank(
            method='first', ascending=False
        )
        most_popular = (
            rel_tags.loc[rel_tags['rn'] == 1].drop(columns='rn').reset_index()
        )
        most_popular = most_popular.merge(
            fandom_works_count,
            how='left',
            on='fandom_name',
            suffixes=('', '_fandom_total'),
        )
        most_popular['pct_of_fandom'] = (
            most_popular['works_num'] / most_popular['works_num_fandom_total']
        )
        most_popular.sort_values(
            by='works_num_fandom_total',
            ascending=False,
            inplace=True,
        )
        st.subheader(
            '''
            How popular is the most popular pairing in each fandom? 
        '''
        )
        st.markdown('#### And how popular is it?')
        fig = px.scatter(
            most_popular.head(100),
            x='pct_of_fandom',
            y='works_num_fandom_total',
            custom_data=['fandom_name', 'name_final'],
            labels={
                'pct_of_fandom': 'Percent of Total Works in Fandom',
                'works_num_fandom_total': 'Number of Total Works in Fandom',
            },
            opacity=0.8,
        )
        fig.update_traces(
            marker=dict(
                size=8,
                color='lightgreen',
                line=dict(width=1, color='darkslategrey'),
            ),
            hovertemplate="<b>%{customdata[0]}</b><br><br>"
            + "Most Popular Relationship: %{customdata[1]}<br>"
            + "Percent of Total Works in Fandom: %{x:.0%}<br>"
            + "Number of Total Works in Fandom: %{y:.3s}<br>",
        )
        fig.update_layout(
            xaxis=dict(
                tickformat='.0%',
            ),
            yaxis=dict(tickformat="~s"),
            font=dict(
                size=15,
            ),
            plot_bgcolor='#e0e0e0',
        )
        st.plotly_chart(fig, use_container_width=True)
        st.markdown(
            '''
            The chart below plots the popularity of the most popular pairing 
            for the top 100 fandoms by number of works. 
            Each dot represents one fandom. Zoom in and hover over each dot 
            for details on which fandom and pairing each dot represents.
            The x axis represents the percent of total works within the fandom 
            that are tagged with the most popular pairing.
            The y axis represents the total number of works within the fandom 
            as of the time of data collection.    
        '''
        )
