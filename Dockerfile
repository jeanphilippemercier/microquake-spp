from python:3.6

RUN apt-get update -qq \
 && apt-get install -y --no-install-recommends \
 gfortran \
 swig \
 libatlas-dev liblapack-dev \
 libhdf5-dev libfftw3-dev \
 libfreetype6 libfreetype6-dev \
 libxft-dev \
 graphviz libgraphviz-dev \
 pandoc \
 libxml2-dev libxslt-dev zlib1g-dev \
 libpng-dev \
 libxext-dev python-qt4 \
 qt4-dev-tools build-essential

RUN mkdir -p /app
COPY Pipfile* /app/
WORKDIR /app

# add credentials on build
ARG SSH_PRIVATE_KEY
RUN mkdir /root/.ssh/
RUN echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa

# make sure your domainccepted
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan git.microquake.org >> /root/.ssh/known_hosts
RUN chmod 0600 /root/.ssh/*

RUN git clone git@git.microquake.org:rio-tinto/nlloc.git
RUN cd nlloc && make \
    ; mv fmm2grid fpfit2hyp Grid2GMT Grid2Time GridCascadingDecimate hypoe2hyp interface2fmm Loc2ddct LocSum Makefile NLDiffLoc NLLoc oct2grid PhsAssoc scat2latlon Time2Angles Time2EQ Vel2Grid Vel2Grid3D /usr/bin \
    ; cd .. && rm -rf nlloc

RUN pip install pipenv

ENV CFLAGS "-I/usr/include/hdf5/serial"
RUN pipenv install --system --deploy

RUN apt-get --purge autoremove -y build-essential qt4-dev-tools python-qt4 libstdc++-6-dev \
  && apt-get install -y graphicsmagick \
  && apt-get clean -y

RUN rm /root/.ssh/id_rsa
RUN rm -rf /root/.cache
RUN rm -rf /usr/local/lib/python3.6/site-packages/microquake/examples

# CMD ["python", "app.py"]
