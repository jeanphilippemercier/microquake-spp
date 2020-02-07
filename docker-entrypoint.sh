#!/bin/bash
set -e

. /ve/bin/activate

poetry run seismic_platform velocities
poetry run seismic_platform prepare

exec "$@"


