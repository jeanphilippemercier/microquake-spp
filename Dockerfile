FROM microquake/python-seismology

ADD ./ /app
WORKDIR /app

ENV PATH="/ve/bin:${PATH}"
ENV SPP_COMMON="/app/common"

RUN bash -c ". /ve/bin/activate ; poetry install"

ENTRYPOINT ["./docker-entrypoint.sh"]
