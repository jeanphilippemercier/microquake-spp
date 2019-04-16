FROM microquake/python-seismology

ADD ./ /app
WORKDIR /app

ENV PATH="/ve/bin:${PATH}"

RUN cd libs/microquake; pip install -e .
RUN curl -o xseis2-0.1.1-cp37-cp37m-linux_x86_64.whl -J -L 'https://git.microquake.org/api/v4/projects/13/jobs/2274/artifacts/dist/xseis2-0.1.1-cp37-cp37m-linux_x86_64.whl?private_token=A9kAyWM7-Qa57QLpwt66'
RUN pip install xseis*whl ; rm *whl
RUN bash -c ". /ve/bin/activate ; poetry install --no-dev"
RUN pip install confluent-kafka
