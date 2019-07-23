# Event data connector

## Overview

This directory contains the three components of the event data connector 
that are 

1. Process initializer (`initializer.py`)
2. Waveform Extractor (`waveform_extractor.py`)
3. Database reconciliation (`database_reconciliator.py`)

## Process Initializer

This modules grabs the catalogue from the IMS system compares it to are 
register of event that have been sent for processing and send the event that
 have not yet been saved to the waveform extractor
 
## Waveform Extractor

This modules does the following
 
1. takes the description of the event provided by the 
event object (`microquake.core.event.Event`) that is received from the 
process initializer
2. Estimate the location and time of the event
3. Extract the waveforms
4. Categorize the event
5. Sends the events to the API connector module (using redis queue)
6. Sends the event to the automatic processing (using redis queue) 

## Database Reconciliation

The purpose of this module is to ensure that all the manually accepted 
events are found in both the IMS and Microquake databases. 