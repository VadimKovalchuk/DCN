FROM python:3.8-slim

# Installing DCN requirements
COPY ./requirements.txt /src/
RUN pip install -U pip wheel
RUN pip install -r /src/requirements.txt

# Installing Findus Edge module
RUN apt update && apt install -y git
RUN git clone https://github.com/VadimKovalchuk/findus-edge.git /tmp/edge
#RUN python /tmp/edge/setup.py build --build-base /tmp egg_info --egg-base /tmp bdist_wheel --dist-dir /tmp
WORKDIR /tmp/edge
RUN python setup.py build bdist_wheel
RUN ls **
RUN pip install dist/findus_edge-0.0.1-py3-none-any.whl
WORKDIR /
#RUN rm -rf /tmp/*

# Copy relevant components
COPY ./dcn /src/dcn
COPY ./service /src/service

# Setting environment parameters
WORKDIR /src
ENV PYTHONPATH "${PYTHONPATH}/src"

RUN pip list
