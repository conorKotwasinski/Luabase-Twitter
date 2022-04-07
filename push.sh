set -x

export DOCKER_TAG=luabase-py
export PROJECT_ID=luabase

git add .
git status
git commit -m "$GIT_COMMENT"
git push -u origin main

# gcloud config set project luabase
# gcloud config set compute/zone us-central1-a
# gcloud config set compute/region us-central1
# gcloud config set account mike@luabase.com


docker build --platform linux/amd64 -t ${DOCKER_TAG} .
# docker run ${DOCKER_TAG}:latest
docker tag ${DOCKER_TAG}:latest us.gcr.io/${PROJECT_ID}/${DOCKER_TAG}:latest
docker push us.gcr.io/${PROJECT_ID}/${DOCKER_TAG}:latest

gcloud beta run services update ${DOCKER_TAG} \
    --image us.gcr.io/${PROJECT_ID}/${DOCKER_TAG} \
    --platform managed \
    --memory 2Gi \
    --account mike@luabase.com \
    --project ${PROJECT_ID} \
    --region us-central1 
# gcloud run services replace cloudrun-service.yaml --account mike@wax.run --project "waxdotrun" --region "us-central1"

terminal-notifier -message "cloud run deployed" -title "Info"
say "cloud run deployed"