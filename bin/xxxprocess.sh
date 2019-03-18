#!/bin/bash

use_web_api="True"
use_web_api="False"

if [ $use_web_api == "True" ]
then
  echo "Use web api"
  input_flag="-e smi:local/8f0f1cbd-2f81-4050-8c62-fd72241f6752"
else
  echo "Dont Use web api"
# These are only needed if you want to work from local xml/mseed files:
  export DDIR='/Users/mth/mth/Data/OT_data'
  export EVID='20180706112101'
  #export DDIR='/Users/mth/Downloads'
  #export EVID='nlloc_output'

  #input_data="-m $DDIR/$EVID.mseed"
  input_data="-m $DDIR/$EVID.mseed"
  xml_in="$DDIR/$EVID.xml"
  xml_out="event.xml"
fi
echo "input=$input_data"


#python xx_snr_picker.py -m $DDIR/$EVID.mseed  --LOGLEVEL WARN \
                       #-x $DDIR/$EVID.xml -o event.xml

# Read traces and input xml from web api: 
#python xx_snr_picker.py -e smi:local/8f0f1cbd-2f81-4050-8c62-fd72241f6752 -o event.xml
# Read traces from web api but use local xml:
#python xx_measure_amplitudes.py -e smi:local/8f0f1cbd-2f81-4050-8c62-fd72241f6752 -x event.xml -o event.xml

python xx_snr_picker.py $input_data -x $xml_in -o $xml_out
python xx_locate.py -x $xml_out -o $xml_out
python xx_measure_amplitudes.py $input_data  -x $xml_out -o $xml_out
python xx_moment_magnitude.py -x $xml_out -o $xml_out
exit 1
python xx_focal_mechanism.py -x $xml_out -o $xml_out
python xx_measure_smom.py $input_data  -x $xml_out -o $xml_out
python xx_moment_magnitude_f.py -x $xml_out -o $xml_out
exit 1

