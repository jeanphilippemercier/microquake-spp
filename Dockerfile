from registry.microquake.org/rio-tinto/seismic-processing-platform/base:latest

RUN mkdir -p /app
WORKDIR /app

# add credentials on build
ARG SSH_PRIVATE_KEY
RUN mkdir /root/.ssh/
RUN echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa

# make sure your domainccepted
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan git.microquake.org >> /root/.ssh/known_hosts
RUN chmod 0600 /root/.ssh/*

RUN rm /root/.ssh/id_rsa
RUN rm -rf /root/.cache
RUN rm -rf /usr/local/lib/python3.6/site-packages/microquake/examples

# CMD ["python", "app.py"]
