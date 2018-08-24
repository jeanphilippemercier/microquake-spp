#!/bin/bash
export SPP='../..'
export SPP_CONFIG=${SPP}/config
export SPP_COMMON=${SPP}/common
export SPP_DATA=${SPP}/data
export PATH=${PATH}:/home/spadmin/projects/nlloc/bin

# Start continuous mongodb api - set to log to /var/log/spp/spp_api.log
python3 /home/spadmin/projects/seismic-processing-platform/scripts/spp_archive_api.py &

# Start interLoc server
export PYTHONPATH="${PYTHONPATH}:${HOME}/projects/xseis/pyinclude"
#nohup python3 /home/spadmin/projects/xseis/pyscripts/poc/interloc_kafka_engine.py >&log &
#python3 /home/spadmin/projects/xseis/pyscripts/poc/interloc_kafka_engine.py &
# Start interLoc server
#python3 ~/projects/xseis/tests/proc_kafka.py >& log &

# Start process_event server
python3 process_event.py >&log &
#exit

# Inject mseed from config/data_connector_config.yaml: data_source.location into kafka
#python3 /home/spadmin/projects/seismic-processing-platform/scripts/ims_connector.py >& log
python3 /home/spadmin/projects/seismic-processing-platform/scripts/ims_connector.py 

exit


