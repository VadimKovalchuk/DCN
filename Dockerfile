FROM python:3.8

# Installing requirements
COPY ./requirements.txt /src/
COPY ./agent/modules/findus-collector/requirements.txt /src/requirements-collector.txt
RUN pip install -r /src/requirements.txt
RUN pip install -r /src/requirements-collector.txt

COPY . /src
WORKDIR /src
ENV PYTHONPATH "${PYTHONPATH}/src"
