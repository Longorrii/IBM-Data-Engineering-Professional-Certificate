#!/bin/bash

today=$(date +%Y%m%d)
weather_report=raw_data_$today

city=Casablanca
curl wttr.in/$city --output $weather_report

echo "There are    too    many spaces in this    sentence." | tr -s " "

echo " Never start or end a sentence with a space. " | xargs 

echo ".sdrawkcab saw ecnetnes sihT" | rev

# print the last field of the string
echo "three two one" | rev | cut -d " " -f 1 | rev

# Unfortunately, this prints the last field of the string, which is empty:
echo "three two one " | rev | cut -d " " -f 1 | rev
# But if you trim the trailing space first, you get the expected result:
echo "three two one " | xargs | rev | cut -d " " -f 1 | rev

grep °C $weather_report > temperatures.txt

obs_tmp=$(head -1 temperatures.txt | tr -s " " | xargs | rev | cut -d " " -f2 | rev)

fc_temp=$(head -3 temperatures.txt | tail -1 | tr -s " " | xargs | cut -d "C" -f2 | rev | cut -d " " -f2 | rev)

TZ='Morocco/Casablanca'

hour=$(TZ='Morocco/Casablanca' date -u +%H) 
day=$(TZ='Morocco/Casablanca' date -u +%d) 
month=$(TZ='Morocco/Casablanca' date +%m)
year=$(TZ='Morocco/Casablanca' date +%Y)

record=$(echo -e "$year\t$month\t$day\t$hour\t$obs_tmp\t$fc_temp")
echo $record>>rx_poc.log