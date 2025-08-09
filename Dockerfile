FROM oryd/oathkeeper:v0.40.9 as oathkeeper-image

FROM python:3.11.11-alpine3.21 AS venv-image
WORKDIR /usr/src/app

ENV POETRY_VERSION="2.1.1"
RUN apk add --no-cache \
    file \
    make \
    build-base \
    curl \
    gcc \
    git \
    musl-dev \
    libffi-dev \
    python3-dev \
    postgresql-dev \
    openssl-dev \
    cargo \
  && curl -sSL https://install.python-poetry.org | python - \
  && ln -s "$HOME/.local/bin/poetry" "/usr/local/bin"

COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false --local \
  && python -m venv /opt/venv \
  && source /opt/venv/bin/activate \
  && poetry install --only main --no-interaction


# This is the final app image. Starting from a clean alpine image, it
# copies over the previously created virtual environment.
FROM python:3.11.11-alpine3.21 AS app-image
ARG FLASK_APP=swpt_creditors

ENV FLASK_APP=$FLASK_APP
ENV APP_ROOT_DIR=/usr/src/app
ENV APP_ASSOCIATED_LOGGERS="swpt_pythonlib.flask_signalbus.signalbus_cli swpt_pythonlib.multiproc_utils"
ENV PYTHONPATH="$APP_ROOT_DIR"
ENV PATH="/opt/venv/bin:$PATH"
ENV WEBSERVER_PORT=8080
ENV RESOURCE_SERVER=http://127.0.0.1:4499
ENV GUNICORN_LOGLEVEL=warning
ENV TOKEN_INTROSPECTION_CACHE_MAX_COST=10000000

RUN apk add --no-cache \
    libffi \
    postgresql-libs \
    postgresql-client \
    supervisor \
    gettext \
    && addgroup -S "$FLASK_APP" \
    && adduser -S -D -h "$APP_ROOT_DIR" "$FLASK_APP" "$FLASK_APP"

COPY --from=oathkeeper-image /usr/bin/oathkeeper /usr/bin/oathkeeper
COPY --from=venv-image /opt/venv /opt/venv

WORKDIR /usr/src/app

COPY docker/entrypoint.sh \
     docker/gunicorn.conf.py \
     docker/supervisord-webserver.conf \
     docker/supervisord-all.conf \
     docker/trigger_supervisor_process.py \
     wsgi.py \
     pytest.ini \
     ./
COPY docker/oathkeeper/ oathkeeper/
COPY migrations/ migrations/
COPY $FLASK_APP/ $FLASK_APP/
RUN python -m compileall -x '^\./(migrations|tests)/' . \
    && rm -f .env \
    && chown -R "$FLASK_APP:$FLASK_APP" .
RUN SQLALCHEMY_DATABASE_URI=sqlite:// SQLALCHEMY_ENGINE_OPTIONS={} \
    flask openapi write openapi.json

USER $FLASK_APP:$FLASK_APP
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
CMD ["all"]


# This is the swagger-ui image. Starting from the final app image, it
# copies the auto-generated OpenAPI spec file. The entrypoint
# substitutes the placeholders in the spec file with values from
# environment variables.
FROM swaggerapi/swagger-ui:v5.17.14 AS swagger-ui-image

ENV SWAGGER_JSON=/openapi.json

COPY --from=app-image /usr/src/app/openapi.json /openapi.template
COPY docker/swagger-ui/envsubst-on-openapi.sh /docker-entrypoint.d/25-envsubst-on-openapi.sh
