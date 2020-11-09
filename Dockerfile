FROM python:3.8
COPY . /src
RUN pip install -r /src/requirements.txt
RUN pip install -r /src/agent/modules/findus-collector/requirements.txt
ENV PYTHONPATH "${PYTHONPATH}/src"
RUN mkdir -p /src/log
