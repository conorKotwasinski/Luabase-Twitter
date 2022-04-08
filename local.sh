source .env

export DOCKER_TAG=luabase-py
export PORT=5000

echo "starting build..."

docker build -t ${DOCKER_TAG} .
docker run -it -p ${PORT}:${PORT} --env-file .env ${DOCKER_TAG}:latest