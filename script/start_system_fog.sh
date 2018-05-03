#!/usr/bin/env bash
#Forwarder Fog to Cloud
docker run -dit --name fog-to-cloud -e "BROKER_CLOUD=$BROKER_CLOUD" -e "BROKER_FOG=$BROKER_FOG" haiquan5396/forwarder_fog_to_cloud:1.0

#Filter
docker run -dit --name filter -e "BROKER_FOG=$BROKER_FOG" haiquan5396/filter:1.0

#HomeDriver
docker run -dit --name home-driver -e "MODE=$MODE" -v $HOME/homeassistant_driver/config:/app/config haiquan5396/homeassistant_driver:1.0
