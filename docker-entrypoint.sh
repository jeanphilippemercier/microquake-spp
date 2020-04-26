#!/bin/bash
set -e

. /ve/bin/activate

prepare() {
    poetry run seismic_platform velocities
    poetry run seismic_platform prepare
}

if [ "$1" == "prepare" ]; then
    wait_for_init
    exec "$@"
fi

exec "$@"
