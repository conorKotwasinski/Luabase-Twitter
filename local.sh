source .env
source ~/.zprofile

export DOCKER_TAG=luabase-py
export PORT=5000
export RUNNING_LOCAL=1

echo "starting build..."


# --env-file .env \

docker build -t ${DOCKER_TAG} .
docker run -it \
    -p ${PORT}:${PORT} \
    --env-file .env \
    -e RUNNING_LOCAL=${RUNNING_LOCAL} \
    -e GOOGLE_APPLICATION_CREDENTIALS=/app/luabase-dev.json \
    -v $GOOGLE_APPLICATION_CREDENTIALS:/app/luabase-dev.json:ro \
    ${DOCKER_TAG}:latest