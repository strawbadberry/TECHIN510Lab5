import requests
import re
import json
import html
import datetime
from zoneinfo import ZoneInfo
import psycopg2
import os

if not os.path.exists('./data'):
    os.makedirs('./data')

from db import get_db_conn

URL = 'https://visitseattle.org/events/page/'
URL_LIST_FILE = './data/links.json'
URL_DETAIL_FILE = './data/data.json'

def list_links():
    res = requests.get(URL + '1/', headers={'User-Agent': 'Mozilla/5.0'})
    last_page_link = re.findall(r'bpn-last-page-link"><a href="(https://visitseattle.org/events/page/(\d+)/)?', res.text)
    
    if last_page_link:
        last_page_url, last_page_no = last_page_link[0]
        print(f'--- Initiating link scraping, final page number is "{last_page_no}" ---')
        event_links = []
        for page_no in range(1, int(last_page_no) + 1):
            print(f'Gathering data from page {page_no} out of {last_page_no}...')
            res = requests.get(URL + str(page_no) + '/', headers={'User-Agent': 'Mozilla/5.0'})
            event_links.extend(re.findall(r'<h3 class="event-title"><a href="(https://visitseattle.org/events/.+?/)" title=".+?">.+?</a></h3>', res.text))
        print('--- Event link collection completed ---')

        with open(URL_LIST_FILE, 'w') as file:
            json.dump(event_links, file)
    else:
        print('Failed to locate the final page link')
    
def get_lat_lon(location):
    if '/' in location:
        location = location.split(' / ')[0].strip()
    location_query = f'{location}, Seattle, WA'
    base_url = "https://nominatim.openstreetmap.org/search"
    query_params = {
        "q": location_query,
        "format": "jsonv2"
    }
    response = requests.get(base_url, params=query_params)
    data = response.json()

    if data and isinstance(data, list) and len(data) > 0:
        return data[0].get('lat'), data[0].get('lon')
    else:
        return None, None 

def get_gridpoint(url):
    weather_details = {'MaxTemp': 'No data', 'MinTemp': 'No data', 'WindChill': 'No data'}
    try:
        gridPoint_res = requests.get(url)
        gridPoint_data = gridPoint_res.json()

        if gridPoint_res.status_code == 200 and 'properties' in gridPoint_data:
            maxTemp = gridPoint_data['properties']['maxTemperature']['values'][0]['value']
            minTemp = gridPoint_data['properties']['minTemperature']['values'][0]['value']
            windChill = gridPoint_data['properties']['windChill']['values'][0]['value']
            weather_details = {
                'MaxTemp': maxTemp,
                'MinTemp': minTemp,
                'WindChill': windChill
            }
        else:
            print(f"No weather info available for {url}")
    except Exception as e:
        print(f"Failed to retrieve weather info: {e}")
    return weather_details

def get_weather_data(lat, lon):
    weather_overview = {'ShortForecast': 'No data', 'GridPoint': 'No data'}

    if lat is None or lon is None:
        return weather_overview

    point_url = f"https://api.weather.gov/points/{lat},{lon}"
    try:
        point_res = requests.get(point_url)
        if point_res.status_code == 200:
            point_data = point_res.json()
            forecast_url = point_data['properties'].get('forecast')
            grid_point_url = point_data['properties'].get('forecastGridData')

            forecast_res = requests.get(forecast_url)
            if forecast_res.status_code == 200:
                forecast_data = forecast_res.json()
                if 'properties' in forecast_data and 'periods' in forecast_data['properties']:
                    for period in forecast_data['properties']['periods']:
                        if period['isDaytime']:
                            weather_overview = {
                                'ShortForecast': period['shortForecast'],
                                'GridPoint': grid_point_url
                            }
                            break
        else:
            print(f"No weather data for {lat},{lon}")
    except Exception as e:
        print(f"Weather data retrieval error: {e}")

    return weather_overview

def get_detail_page():
    links = json.load(open(URL_LIST_FILE, 'r'))
    details = []
    for link in links:
        try:
            event_info = {}
            res = requests.get(link)
            event_info['title'] = html.unescape(re.findall(r'<h1 class="page-title" itemprop="headline">(.+?)</h1>', res.text)[0])
            datetime_venue = re.findall(r'<h4><span>.*?(\d{1,2}/\d{1,2}/\d{4})</span> \| <span>(.+?)</span></h4>', res.text)[0]
            event_info['date'] = datetime.datetime.strptime(datetime_venue[0], '%m/%d/%Y').replace(tzinfo=ZoneInfo('America/Los_Angeles')).isoformat()
            event_info['venue'] = datetime_venue[1].strip()
            categories = re.findall(r'<a href=".+?" class="button big medium black category">(.+?)</a>', res.text)
            event_info['category'] = html.unescape(categories[0])
            event_info['location'] = categories[1]
            lat, lon = get_lat_lon(event_info['location'])
            event_info['geolocation'] = lat, lon
            weather = get_weather_data(lat, lon)
            event_info['weather_condition'] = weather['ShortForecast']
            grid_point_weather = get_gridpoint(weather['GridPoint'])
            event_info['weather_minTemp'] = grid_point_weather['MinTemp']
            event_info['weather_maxTemp'] = grid_point_weather['MaxTemp']
            event_info['weather_windChill'] = grid_point_weather['WindChill']

            details.append(event_info)

            print('Current data snapshot:')
            print()
            print(details)
            print('----------------------')
            print()

        except IndexError as e:
            print(f'Error encountered: {e}')
            print(f'Problematic link: {link}')
    json.dump(details, open(URL_DETAIL_FILE, 'w'))

def insert_to_pg():
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS events (
        url TEXT PRIMARY KEY,
        title TEXT,
        date TIMESTAMP WITH TIME ZONE,
        venue TEXT,
        category TEXT,
        location TEXT,
        geolocation TEXT,
        weathercondition TEXT,
        weathermintemp FLOAT,
        weathermaxtemp FLOAT,
        weatherwindchill FLOAT
    );
    '''
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(create_table_query)
    
    urls = json.load(open(URL_LIST_FILE, 'r'))
    events_data = json.load(open(URL_DETAIL_FILE, 'r'))
    for url, event in zip(urls, events_data):
        insert_query = '''
        INSERT INTO events (url, title, date, venue, category, location, geolocation, weathercondition, weathermintemp, weathermaxtemp, weatherwindchill)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (url) DO NOTHING;
        '''
        cur.execute(insert_query, (url, event['title'], event['date'], event['venue'], event['category'], event['location'], event['geolocation'], event['weather_condition'], event['weather_minTemp'], event['weather_maxTemp'], event['weather_windChill']))

def scrape_events_data():
    list_links()
    get_detail_page()
    insert_to_pg()

if __name__ == '__main__':
    scrape_events_data()