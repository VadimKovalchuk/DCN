FROM python:3.8

# Installing requirements
COPY ./requirements.txt /src/
COPY ./agent/modules/findus-edge/requirements.txt /src/requirements-findus-edge.txt
RUN pip install -r /src/requirements.txt
RUN pip install -r /src/requirements-findus-edge.txt

COPY . /src
WORKDIR /src
ENV PYTHONPATH "${PYTHONPATH}/src"
