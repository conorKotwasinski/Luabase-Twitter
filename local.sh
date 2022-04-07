export DOCKER_TAG=luabase-py

docker build -t ${DOCKER_TAG} . && PORT=5000 && \
docker run -it -p ${PORT}:${PORT} \
--env-file .env \
-e PORT=${PORT} \
-e WORKERS=1 \
${DOCKER_TAG}:latest