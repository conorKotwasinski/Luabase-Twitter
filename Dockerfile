# [START cloudrun_lua_py_dockerfile]
# [START run_lua_py_dockerfile]

# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.9

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

COPY requirements.txt /app/
# --no-cache-dir
RUN --mount=type=cache,mode=0755,target=/root/.cache/pip pip3 --default-timeout=600 install -r /app/requirements.txt 

EXPOSE 22
EXPOSE 5000/tcp

ENV PORT 5000

COPY app.py /app/
COPY logger.py /app/

COPY el /app/el
COPY utils /app/utils



# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
# CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app
# 

# CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app

CMD cd app && exec gunicorn --timeout 0 --bind :$PORT --workers $WORKERS --threads $THREADS app:app

# [END run_lua_py_dockerfile]
# [END cloudrun_lua_py_dockerfile]
