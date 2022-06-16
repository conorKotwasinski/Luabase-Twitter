# [START cloudrun_lua_py_dockerfile]
# [START run_lua_py_dockerfile]
# Arg used to build either locally for developing or cloud for deployment
ARG local=cloud
# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.9 as base-reqs
COPY requirements.txt /luapy/
RUN pip3 --default-timeout=600 install -r /luapy/requirements.txt

FROM base-reqs as testbuild-reqs
COPY requirements-dev.txt /luapy/
RUN pip3 --default-timeout=600 install -r /luapy/requirements-dev.txt

FROM base-reqs as base
COPY luapy/ /luapy/

# testbuild includes dev dependencies and tests
FROM testbuild-reqs as testbuild-base
COPY requirements-dev.txt /luapy/
RUN pip3 --default-timeout=600 install -r /luapy/requirements-dev.txt
COPY luapy/ /luapy/
COPY tests/ /tests/

# not local, credentials should already be in the environment
FROM testbuild-base as testbuild-cloud
ENV RUNNING_LOCAL=0

# running tests locally needs credentials
FROM testbuild-base as testbuild-local
ENV RUNNING_LOCAL=1
ENV GOOGLE_APPLICATION_CREDENTIALS=/luapy/luabase-dev.json

# runs pytest
FROM testbuild-${local} as testbuild
RUN python -m pytest -v /tests/

# final image
FROM base as final

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True
# ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION cpp

# Copy local code to the container image.
# ENV APP_HOME /app
# WORKDIR $APP_HOME
# COPY . ./

# Install Postgres stuff
# remove this if postgres-binary is works
# RUN apt-get update -y
# RUN apt-get install -y libpq-dev 

# Install production dependencies.
# RUN pip install --no-cache-dir -r requirements.txt

# --no-cache-dir
# RUN --mount=type=cache,mode=0755,target=/root/.cache/pip pip3 --default-timeout=600 install -r /luapy/requirements.txt 

EXPOSE 22
EXPOSE 5000/tcp

ENV PORT 5000

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
# CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app
# 

# CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app

CMD exec gunicorn --timeout 0 --bind :$PORT --workers $WORKERS --threads $THREADS luapy.app:app

# [END run_lua_py_dockerfile]
# [END cloudrun_lua_py_dockerfile]
