FROM microquake/python-seismology

ADD ./ /app
WORKDIR /app

ENV PATH="/ve/bin:${PATH}"

RUN bash -c ". /ve/bin/activate ; cd libs/microquake; pip install -e ."
RUN curl -o xseis2-0.1.1-cp37-cp37m-linux_x86_64.whl -J -L 'https://git.microquake.org/api/v4/projects/13/jobs/3036/artifacts/dist/xseis2-0.1.1-cp37-cp37m-linux_x86_64.whl'
RUN bash -c ". /ve/bin/activate ; pip install xseis*whl ; rm *whl"
RUN bash -c ". /ve/bin/activate ; poetry install"
RUN bash -c ". /ve/bin/activate ; pip install confluent-kafka"
