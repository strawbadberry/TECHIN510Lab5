import streamlit as st
import pandas as pd
import altair as alt
import folium
from streamlit_folium import st_folium
from db import conn_str

def load_data(query, conn_str):
    return pd.read_sql_query(query, conn_str)

def create_bar_chart(data, x_axis, y_axis, title):
    chart = alt.Chart(data).mark_bar().encode(
        x=x_axis, 
        y=y_axis
    ).properties(
        title=title
    ).interactive()
    return chart

def prepare_data(df):
    df['date'] = pd.to_datetime(df['date'])
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year
    df['day_of_week'] = df['date'].dt.day_name()
    df['day_of_week_num'] = df['day_of_week'].map(
        {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6}
    )

def create_map(df, location, zoom_start):
    m = folium.Map(location=location, zoom_start=zoom_start)
    for idx, row in df.iterrows():
        if pd.notnull(row['geolocation']):
            try:
                lat, lon = map(float, row['geolocation'].strip("{}").split(','))
                folium.Marker(
                    location=[lat, lon],
                    popup=f"{row['title']} - {row['date'].strftime('%Y-%m-%d')}",
                ).add_to(m)
            except ValueError:
                st.error(f"Error parsing geolocation for row {idx}: {row['geolocation']}")
    return m

def main():
    st.title("Seattle Events")
    df = load_data("SELECT * FROM events", conn_str)
    prepare_data(df)

    st.subheader('What category of events are most common in Seattle?')
    category_counts = df['category'].value_counts().reset_index()
    category_counts.columns = ['category', 'count']
    st.altair_chart(create_bar_chart(category_counts, "count:Q", "category:N", "Event Category Counts"), use_container_width=True)

    st.subheader('What month has the most number of events?')
    monthly_events = df.groupby(['year', 'month']).size().reset_index(name='counts')
    monthly_events['month_year'] = monthly_events['month'].astype(str) + '/' + monthly_events['year'].astype(str)
    st.altair_chart(create_bar_chart(monthly_events, "month_year:N", "counts:Q", "Monthly Event Counts"), use_container_width=True)

    st.subheader('What day of the week has the most number of events?')
    weekly_events = df.groupby(['day_of_week', 'day_of_week_num'], as_index=False).size()
    weekly_events = weekly_events.sort_values('day_of_week_num')
    st.altair_chart(create_bar_chart(weekly_events, "day_of_week:N", "size:Q", "Weekly Event Counts"), use_container_width=True)

    category = st.selectbox("Select a category to filter", ['All'] + list(df['category'].unique()))
    
    date_range = st.date_input("Select date range", [])
    
    location = st.selectbox("Select a location to filter", ['All'] + list(df['location'].unique()))
    
    weather = st.selectbox("Select a weather condition to filter", ['All'] + list(df['weathercondition'].unique()))

    if category != 'All':
        df = df[df['category'] == category]
    if date_range:
        df = df[(df['date'].dt.date >= date_range[0]) & (df['date'].dt.date <= date_range[1])]
    if location != 'All':
        df = df[df['location'] == location]
    if weather != 'All':
        df = df[df['weathercondition'] == weather]

    st.write(df)


    st.subheader('Event Locations on Map')
    st_folium(create_map(df, [47.6504529, -122.3499861], 12), width=800, height=600)


if __name__ == "__main__":
    main()