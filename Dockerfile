FROM python:3.8

# Installing DCN requirements
COPY ./requirements.txt /src/
RUN pip install -r /src/requirements.txt

# Installing Findus Edge module
RUN git clone https://github.com/VadimKovalchuk/findus-edge.git /tmp/edge
RUN python /tmp/edge/setup.py build --build-base /tmp egg_info --egg-base /tmp bdist_wheel --dist-dir /tmp
RUN pip install /tmp/findus_edge-0.0.1-py3-none-any.whl
RUN rm -rf /tmp/*

# Copy relevant components
COPY ./agent /src/agent
COPY ./client /src/client
COPY ./common /src/common
COPY ./dispatcher /src/dispatcher

# Setting environment parameters
WORKDIR /src
ENV PYTHONPATH "${PYTHONPATH}/src"
