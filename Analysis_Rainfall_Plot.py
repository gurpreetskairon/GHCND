#################################################################################################
## Analysis Q4e: Find an elegant way to plot the average rainfall for each country using Python,#
##               R, or any other programming language you know well. There are many ways to do ##
##               this in Python and R specifically, such as using a choropleth to color a map  ##
##               according to average rainfall.                                                ##
#################################################################################################

import pandas as pd
import numpy as np
import glob
import os
from pycountry_convert import country_name_to_country_alpha2
import folium
from folium.plugins import MarkerCluster
from IPython.display import display

 

path = r'C:\Gurpreet\UC\DATA420-Scalable Data Science\Assignments\Assignment1\average_rain_per_country'

all_files = glob.glob(os.path.join(path, "*.csv"))     # advisable to use os.path.join as this makes concatenation OS independent


li = []


for filename in all_files:

    df = pd.read_csv(filename)

    li.append(df)

 

total_df = pd.concat(li, axis=0, ignore_index=True)


average_rainfall_df = total_df.groupby(['NAME']).mean().reset_index()

 

#function to convert to alpah2 country codes and continents

def get_continent(col):

    try:

        print(col)

        cn_a2_code =  country_name_to_country_alpha2(col)

    except:

        cn_a2_code = 'Unknown'

       

    return cn_a2_code

 

average_rainfall_df['COUNTRY'] = average_rainfall_df['NAME'].apply(get_continent)

 

average_rainfall_df = average_rainfall_df[average_rainfall_df['COUNTRY'] != 'Unknown']

 

#function to get longitude and latitude data from country name

from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent='myapplication')

def latitude(country):

    try:

        loc = geolocator.geocode(country)

        return loc.latitude

    except:

        return np.nan

 

def longitude(country):

    try:

        loc = geolocator.geocode(country)

        return loc.longitude

    except:

        return np.nan

      

average_rainfall_df['LATITUDE'] = average_rainfall_df['COUNTRY_NAME'].apply(latitude)

average_rainfall_df['LONGITUDE'] = average_rainfall_df['COUNTRY_NAME'].apply(longitude)

 

#empty map

world_map= folium.Map(tiles="cartodbpositron")

marker_cluster = MarkerCluster().add_to(world_map)

 

#for each coordinate, create circlemarker of user percent

for i in range(len(average_rainfall_df)):

        lat = average_rainfall_df.iloc[i]['LATITUDE']

        long = average_rainfall_df.iloc[i]['LONGITUDE']

        radius=10

        popup_text = """Country : {}<br>

                    Average Rain Fall : {}<br>"""

        popup_text = popup_text.format(average_rainfall_df.iloc[i]['COUNTRY_NAME'],

                                   average_rainfall_df.iloc[i]['AVG_RAINFALL']

                                  )

        folium.CircleMarker(location = [lat, long], radius=radius, popup= popup_text, fill =True).add_to(marker_cluster)

       

display(world_map)

 

world_map.save("average_rainfall.html")