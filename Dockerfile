from registry.microquake.org/rio-tinto/seismic-processing-platform/base:latest

COPY ./ /app
RUN pip install --no-deps libs/microquake
RUN pip install --no-deps libs/xseis
RUN echo "helloworld"

RUN rm -rf /root/.cache
RUN rm -rf /usr/local/lib/python3.6/site-packages/microquake/examples

# CMD ["python", "app.py"]
