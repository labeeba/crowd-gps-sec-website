from flask import Flask
from flask import render_template
import pandas as pd
import numpy as np
import gmplot
import urllib.request
import configparser
import time
from datetime import datetime, timedelta
from pathlib import Path
import requests
import hashlib
import re
import tempfile
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Iterable, Iterator, Optional, Tuple, Union
import paramiko


class ImpalaWrapper(object):

    #SQL query 
    basic_request= "select time,icao24,lat,lon,GROUP_CONCAT(CAST(serials.item AS STRING),',') FROM state_vectors_data4, state_vectors_data4.serials WHERE time in (select max(time) from state_vectors_data4,state_vectors_data4.serials where hour > unix_timestamp(now()) - 6*3600) and hour > unix_timestamp(now()) - 6*3600 GROUP BY time,icao24,lat,lon,velocity,heading,vertrate,callsign,onground,alert,spi,squawk,baroaltitude,geoaltitude,lastposupdate,lastcontact ORDER BY time asc;"

    def __init__(self, username: str, password: str) -> None:

        self.username = username
        self.password = password
        self.connected = False

        self.cache_dir = Path(tempfile.gettempdir()) / "cache_opensky"
        if not self.cache_dir.exists():
            self.cache_dir.mkdir(parents=True)
    
    #connecting to OpenSky Network
    def _connect(self) -> None:
        if self.username == "" or self.password == "":
            raise RuntimeError("This method requires authentication.")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect("data.opensky-network.org", port=2230,
                       username=self.username, password=self.password)
        self.c = client.invoke_shell()
        self.connected = True
        total = ""
        while len(total) == 0 or total[-19:] != "[hadoop-1:21000] > ":
            b = self.c.recv(256)
            total += b.decode()
            
    #saving data and parsing to store in a .csv file
    def _impala(self, request: str) -> Optional[pd.DataFrame]:

        digest = hashlib.md5(request.encode('utf8')).hexdigest()
        print(digest)
        cachename = self.cache_dir / digest
        print(cachename)

        if not cachename.exists():
            if not self.connected:
                self._connect()
            self.c.send(request + ";\n")
            total = ""
            while len(total) == 0 or total[-19:] != "[hadoop-1:21000] > ":
                b = self.c.recv(256)
                total += b.decode()
            with cachename.open('w') as fh:
                fh.write(total)

        with cachename.open('r') as fh:
            s = StringIO()
            count = 0
            for line in fh.readlines():
                if re.match("\|.*\|", line):
                    count += 1
                    s.write(re.sub(" *\| *", ",", line)[1:-2])
                    s.write("\n")
            if count > 0:
                s.seek(0)
                df = pd.read_csv(s, names = list(range(0,50)))
                return df

        return None

    #sending the query to OpenSky and storing results as a pandas dataframe
    def history(self) -> Optional[pd.DataFrame]:

        df = self._impala(self.basic_request)
        cumul=[]
        cumul.append(df)

        if len(cumul) > 0:
            return pd.concat(cumul)

        return None


#initializing and logging into OpenSky Network and saving results to a .csv
def opensky_data(output_file):

    username = input('Please enter username:')
    password = input('Please enter password:')
    opensky = ImpalaWrapper(username, password)

    data = opensky.history()
    data.to_csv(output_file)


opensky_data("data.csv") #the .csv file where results are stored


raw_data = pd.read_csv(r"data.csv", skiprows=1) 


#rawdata['Unnamed: 8'] # selects the column with the 4th serial receiver id.

filter_data = raw_data.dropna(subset=['lat']) #dropping messages with null latitude/longitude values
filter_data1 = filter_data.dropna(subset=['Unnamed: 8']) #dropping messages which were not received by 4 or more receivers

#latitude and longitude values for scatter map- test 1
lat = filter_data1['lat'] 
lon = filter_data1['lon']

#latitude and longitude values for heatmap - test 2
lat2 = filter_data['lat'] 
lon2 = filter_data['lon']

lat = pd.to_numeric(lat, errors='coerce')
lon = pd.to_numeric(lon, errors='coerce')

lat2 = pd.to_numeric(lat2, errors='coerce')
lon2 = pd.to_numeric(lon2, errors='coerce')



#gmap= gmplot.GoogleMapPlotter(0,0,2)

gmaps.configure(api_key= "AIzaSyCzknoU12SeX7kfauakSviJ9NZU96yDFo4")


locations= list(zip(lat,lon))
locations2= list(zip(lat2,lon2))
locations2 = [x for x in locations2 if str(x[0]) != 'nan']

red_layer = gmaps.symbol_layer(
    locations, fill_color='rgba(200, 0, 0, 0.4)',
    stroke_color='rgba(200, 0, 0, 0.4)', scale=2
)


fig = gmaps.figure()
fig.add_layer(red_layer)
fig.add_layer(gmaps.heatmap_layer(locations2))
embed_minimal_html('test1_test2.html', views=[fig]) #html file that contains both scatter and heatmap

fig1 = gmaps.figure()
fig1.add_layer(red_layer)
embed_minimal_html('scatter_test1.html', views=[fig1]) #html file that contains only the scatter plot

fig2= gmaps.figure()
fig2.add_layer(gmaps.heatmap_layer(locations2))
embed_minimal_html('heatmap_test2.html', views=[fig2]) #html file that contains only the heatmap 

'''gmap= gmplot.GoogleMapPlotter(0,0,2)


gmap.heatmap(lat, lon)
gmap.scatter(lat, lon, '#FF0000',size = 40, marker = False)
gmap.draw(r"./templates/demo_heat.html")

app = Flask(__name__)
 
@app.route("/")
def index():
    return render_template("demo_heat.html")
 
if __name__ == "__main__":
    app.run()'''

