# diamonds.py
from shiny import App, ui, render, reactive
import plotnine as gg
from plotnine.data import diamonds
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
from shinywidgets import output_widget, render_widget, register_widget

app_df = pd.read_csv('data/app_df.csv')


app_ui  = ui.page_fluid(
    ui.layout_sidebar(
         
    ui.panel_sidebar(
        
     
        ui.input_select(
            id='races',
            label='Name of the Race', 
            choices= list(app_df.race.unique()),
            multiple=True,
            selectize=True
        ),
          ui.input_select(
            id='type_race',
            label='What type of race is it?', 
            choices= list(app_df.type_race.unique()),
            multiple=True,
            selectize=True
        ),
        
        
        ), 
    
    ui.panel_main( output_widget("brands_barplot")
                 
   
    ))
)


def server(input, output, session):
    
    
    
    @reactive.Calc
    def grouped_by_brand():
        df = app_df.copy()
        races = list(input.races())
        df_race_filtered = df[df['race'].isin(races)]
        brand_groups = df_race_filtered.groupby(['race', 'bike_brand']).size().reset_index(name='count')
        return brand_groups
    
    
    
    
    @output
    @render.table
    def summary():
        filt_df = filtered_df()
        return filt_df
    
    
    @output 
    @render_widget
    def brands_barplot():
        plot_df = grouped_by_brand()
        bar_plot = px.bar(plot_df.sort_values(by='count', ascending = True),
                          x='count',
                    
                          y='bike_brand',
                          facet_col='race')
        bar_plot.update_yaxes(matches=None)
        return bar_plot

app = App(app_ui, server)
