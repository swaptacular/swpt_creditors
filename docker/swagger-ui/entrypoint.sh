#!/bin/sh

envsubst '$API_BASE_URL $OAUTH2_AUTHORIZATION_URL $OAUTH2_TOKEN_URL $OAUTH2_REFRESH_URL' \
         < /openapi.template > /openapi.json

exec "$@"
