# source .env
source ~/.zprofile

export DOCKER_TAG=luabase-py-test
export PORT=5000
export WORKERS=1
export THREADS=8
export RUNNING_LOCAL=1

echo "starting test build..."

cp $GOOGLE_APPLICATION_CREDENTIALS ./luapy/luabase-dev.json
DOCKER_BUILDKIT=1 docker build --build-arg local=local --target testbuild -t ${DOCKER_TAG} .
rm ./luapy/luabase-dev.json

# docker run -it \
#     -p ${PORT}:${PORT} \
#     -e RUNNING_LOCAL=${RUNNING_LOCAL} \
#     -e WORKERS=${WORKERS} \
#     -e THREADS=${THREADS} \
#     -e GOOGLE_APPLICATION_CREDENTIALS=/luapy/luabase-dev.json \
#     -v $GOOGLE_APPLICATION_CREDENTIALS:/luapy/luabase-dev.json:ro \
#     ${DOCKER_TAG}:latest
