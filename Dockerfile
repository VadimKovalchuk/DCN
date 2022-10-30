FROM python:3.8

# Installing requirements
COPY ./requirements.txt /src/
RUN pip install -r /src/requirements.txt
COPY ./packages/edge/ /src/edge/
RUN pip install /src/edge/dist/findus_edge-0.0.1-py3-none-any.whl
RUN rm -rf /src/edge

COPY ./agent /src/agent
COPY ./client /src/client
COPY ./common /src/common
COPY ./dispatcher /src/dispatcher
RUN ls /src/*
WORKDIR /src
ENV PYTHONPATH "${PYTHONPATH}/src"
