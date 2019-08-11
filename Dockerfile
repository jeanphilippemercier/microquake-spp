FROM microquake/python-seismology

ADD ./ /app
WORKDIR /app

ENV PATH="/ve/bin:${PATH}"
ENV SPP_COMMON="/app/common"

RUN poetry config repositories.microquake https://pkg.microquake.org
RUN poetry config http-basic.microquake $PYPI_USER $PYPI_PASS
RUN bash -c ". /ve/bin/activate ; poetry install"
RUN rm -rf ~/.config

ENTRYPOINT ["./docker-entrypoint.sh"]
