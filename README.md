# crowd-gps-sec

## Motivation

Crowd-GPS-Sec is a solution proposed to detect and localize GPS spoofing attacks on moving airborne targets 
such as UAVs or commercial airliners. 

The GPS Spoofing detection module consists of two tests:
* Test 1 - Cross Checks with MLAT 
    Condition: MLAT requires atleast 4 sensors on the ground to receive ADS-B messages.
* Test 2 - Multiple Aircraft Comparison
    Condition: Aircraft Comparison requires atleast 2 aircrafts to receive the spoofed signal.

The purpose of this app is to:
1) find the "blind-spots" where MLAT cannot be computed by plotting a scatter map of the 
positions on the world map where atleast 4 sensors received the message from the aircraft.
2) visualize aircraft density in order to determine which parts of the world Test 2 can be
used in.


## Overview 

app.py: 
1) connects to the OpenSky Network 
2) sends an SQL query to the Impala shell to retrieve most recent aircraft messages + serial ids of sensors
3)

