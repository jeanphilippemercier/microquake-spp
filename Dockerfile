FROM registry.microquake.org/rio-tinto/seismic-processing-platform/base:latest

RUN apt install vim -y

#COPY ./ /app

ADD ./ /app

#ADD $SPP_COMMON/ /app/common
#COPY $SPP_CONFIG /app/config

RUN pip install --no-deps -e libs/microquake
#RUN pip install -e libs/microquake/
RUN pip install --no-deps libs/xseis
RUN pip install ipython
RUN pip install -e .

RUN rm -rf /root/.cache
RUN rm -rf /usr/local/lib/python3.6/site-packages/microquake/examples

# CMD ["python", "app.py"]
