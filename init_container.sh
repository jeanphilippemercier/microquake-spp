#!/usr/bin/env bash

mkdir /app/common/velocities
curl -X GET https://api.microquake.org/api/v1/inventory/sites/OT.xml > /app/common/inventory.xml
poetry run seismic_platform velocities
poetry run seismic_platform prepare
poetry 'run rq worker --url redis://:$(SPP_REDIS_PASSWORD)\
        @spp-redis-master:6379/4\
         --log-format %(%(asctime)s $(SPP_PRE_PROCESSING_MESSAGE_QUEUE)'