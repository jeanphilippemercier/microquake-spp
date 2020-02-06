#!/bin/bash
set -e

. /ve/bin/activate
exec "$@"

poetry run seismic_platform velocities
poetry run seismic_platform prepare
