#!/bin/sh

if [ -z "$1" ]; then
    echo "Usage: release.sh TAG"
    return
fi

swpt_creditors="epandurski/swpt_creditors:$1"
swpt_creditors_swagger_ui="epandurski/swpt_creditors_swagger_ui:$1"
docker build -t "$swpt_creditors" --target app-image .
docker build -t "$swpt_creditors_swagger_ui" --target swagger-ui-image .
git tag "v$1"
git push origin "v$1"
docker login
docker push "$swpt_creditors"
docker push "$swpt_creditors_swagger_ui"
