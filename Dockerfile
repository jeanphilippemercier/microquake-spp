FROM microquake/python-seismology

ADD ./ /app
WORKDIR /app

ENV PATH="/ve/bin:${PATH}"
ENV SPP_COMMON="/app/common"

RUN bash -c ". /ve/bin/activate ; cd libs/microquake; pip install -e ."
# WARNING: private_token should not be in here
RUN curl -o xseis2-0.1.5-cp37-cp37m-linux_x86_64.whl -J -L "https://git.microquake.org/api/v4/projects/13/jobs/4951/artifacts/dist/xseis2-0.1.5-cp37-cp37m-linux_x86_64.whl?private_token=A9kAyWM7-Qa57QLpwt66"
RUN bash -c ". /ve/bin/activate ; pip install xseis*whl ; rm *whl"
RUN bash -c ". /ve/bin/activate ; poetry install"
