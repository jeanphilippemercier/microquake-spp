FROM microquake/python-seismology

ADD ./ /app
WORKDIR /app

ENV PATH="/ve/bin:${PATH}"
ENV SPP_COMMON="/app/common"

ARG PYPI_USER
ARG PYPI_PASS
RUN poetry run pip install pip>=41.6.0
RUN poetry config repositories.microquake https://pkg.microquake.org
RUN poetry config http-basic.microquake $PYPI_USER $PYPI_PASS
RUN bash -c ". /ve/bin/activate ; poetry install"
RUN rm -rf ~/.config

ENTRYPOINT ["/app/docker-entrypoint.sh"]
