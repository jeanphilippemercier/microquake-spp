#!/bin/bash

# These are only needed if you want to work from local xml/mseed files:
#export DDIR='/Users/mth/mth/Data/OT_data/'
#export EVID='20180706112101'

#python xx_snr_picker.py -m $DDIR/$EVID.mseed  --LOGLEVEL WARN \
                       #-x $DDIR/$EVID.xml -o event.xml


# Read traces and input xml from web api: 
python xx_snr_picker.py -e smi:local/8f0f1cbd-2f81-4050-8c62-fd72241f6752 -o event.xml

python xx_locate.py -x event.xml -o event.xml

# Read traces from web api but use local xml:
python xx_measure_amplitudes.py -e smi:local/8f0f1cbd-2f81-4050-8c62-fd72241f6752 -x event.xml -o event.xml

# Don't need traces below:
python xx_focal_mechanism.py -x event.xml -o event.xml
python xx_moment_magnitude.py -x event.xml -o event.xml
exit 1

#python post_process.py -a -e smi:local/8f0f1cbd-2f81-4050-8c62-fd72241f6752 -o event.xml
#python xx_measure_amplitudes.py -m $DDIR/$EVID.mseed -x event.xml -o event.xml
#python xx_calc_focal_mechanism.py -m $DDIR/$EVID.mseed -x event.xml -o event.xml
#python xx_calc_moment_magnitude.py -m $DDIR/$EVID.mseed -x event.xml -o event.xml
