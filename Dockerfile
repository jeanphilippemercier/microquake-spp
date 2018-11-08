from registry.microquake.org/rio-tinto/seismic-processing-platform/base:latest

COPY ./ /app
RUN pip install libs/microquake
RUN pip install libs/xseis

RUN rm -rf /root/.cache
RUN rm -rf /usr/local/lib/python3.6/site-packages/microquake/examples

# CMD ["python", "app.py"]
