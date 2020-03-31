FROM microquake/python-seismology

ADD ./ /app
WORKDIR /app

ENV PATH="/ve/bin:${PATH}"
ENV SPP_COMMON="/app/common"

ARG PYPI_USER
ARG PYPI_PASS
RUN poetry config repositories.microquake https://pkg.microquake.org
RUN poetry config http-basic.microquake $PYPI_USER $PYPI_PASS
RUN bash -c ". /ve/bin/activate ; poetry install"
RUN rm -rf ~/.config
RUN curl -X GET "https://api.microquake.org/api/v1/inventory/sites/OT.xml" -H "accept: application/json" > /app/common/inventory.xml
#RUN poetry run seismic_platform velocities
#RUN poetry run seismic_platform prepare

ENTRYPOINT ["/app/docker-entrypoint.sh"]
