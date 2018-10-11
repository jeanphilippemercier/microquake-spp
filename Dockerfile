from python:3.6

RUN apt-get update -qq \
 && apt-get install -y --no-install-recommends \
 gfortran \
 swig \
 libatlas-dev liblapack-dev \
 libfreetype6 libfreetype6-dev \
 libxft-dev \
 graphviz libgraphviz-dev \
 pandoc \
 libxml2-dev libxslt-dev zlib1g-dev \
 libpng-dev \
 libxext-dev python-qt4 \
 qt4-dev-tools build-essential

RUN apt-get clean

RUN mkdir -p /app
COPY Pipfile* /app/
WORKDIR /app

# add credentials on build
ARG SSH_PRIVATE_KEY
RUN mkdir /root/.ssh/
RUN echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa

# make sure your domainccepted
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan seismic-gitlab.eastus.cloudapp.azure.com >> /root/.ssh/known_hosts
RUN chmod 0600 /root/.ssh/*

RUN pip install pipenv

RUN pipenv install --system --deploy

RUN rm /root/.ssh/id_rsa

# CMD ["python", "app.py"]
