set -x
source .env

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
    --min-instances 1 \
    --memory 2Gi \
    --timeout 3600 \
    --account mike@luabase.com \
    --project ${PROJECT_ID} \
    --set-env-vars "SCRAPING_BEE_API_KEY=$SCRAPING_BEE_API_KEY" \
    --set-env-vars "CH_ADMIN_PASSWORD=$CH_ADMIN_PASSWORD" \
    --set-env-vars "SUPABASE_SQLALCHEMY_DATABASE_URI=$SUPABASE_SQLALCHEMY_DATABASE_URI" \
    --set-env-vars "WORKERS=$WORKERS" \
    --set-env-vars "THREADS=$THREADS" \
    --region us-central1 

terminal-notifier -message "cloud run python deployed" -title "Info"
say "cloud run deployed, what do now?"