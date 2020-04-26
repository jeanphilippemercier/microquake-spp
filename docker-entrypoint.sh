#!/bin/bash
set -e

. /ve/bin/activate

prepare() {
    poetry run seismic_platform velocities
    poetry run seismic_platform prepare
}

if [ "$1" == "prepare" ]; then
    prepare
    exec "$@"
fi

if [ "$1" == "modelupdate" ]; then
    seismic_client get-weight -i "${SPP_MODEL_WEIGHT_ID}"
    exit $?
fi


exec "$@"
