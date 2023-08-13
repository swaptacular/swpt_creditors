Swaptacular "Creditors Agent" reference implementation
======================================================

This project implements a [Swaptacular] "Creditors Agent" node. The
deliverables are two [docker images]: the *app-image*, and the
*swagger-ui-image*. Both images are generated from the project's
[Dockerfile](../master/Dockerfile).

* The `app-image` provides all the necessary services. The most
  important service is the [Payments Web API]. This is a server Web
  API, which allows creditors to create accounts with debtors, and
  make and receive payments to/from other creditors. Normally, in
  order to "talk" to the creditors agent, currency holders will use a
  ["wallet" client application].

* The `swagger-ui-image` is a simple [Swagger UI] cleint for the
  server Web API, mainly useful for testing.

**Note:** This implementation uses [JSON Serialization for the
Swaptacular Messaging Protocol].


Dependencies
------------

Containers started from the *app-image* must have access to the
following servers:

1. [PostgreSQL] server instance, which stores creditors' data.

2. [RabbitMQ] server instance, which acts as broker for [Swaptacular
   Messaging Protocol] (SMP) messages.

   A [RabbitMQ queue] must be configured on the broker instance, so
   that all incoming SMP messages for the creditors stored on the
   PostgreSQL server instance, are routed to this queue.

   Also, a [RabbitMQ exchange] named **`creditors_out`** must be configured
   on the broker instance. This exchange is for messages that must be sent
   to accounting authorities. The routing key will represent the debtor ID
   as hexadecimal (lowercase). For example, for debtor ID equal to 10, the
   routing key will be "00.00.00.00.00.00.00.0a".

   **Note:** If you execute the "configure" command (see below), with
   the environment variable `SETUP_RABBITMQ_BINDINGS` set to `yes`, an
   attempt will be made to automatically setup all the required
   RabbitMQ queues, exchanges, and the bindings between them.

3. [Redis] server instance, which stores Denial of Service attack
   protection statistics for the creditors stored on the PostgreSQL
   server instance. For this kind of information, the tolerance for
   data loss is high, but nevertheless, the Redis server instance must
   be configured as persistent (on-disk) database.

4. [OAuth 2.0] authorization server, which authorizes clients'
   requests to the [Payments Web API]. There is a plethora of popular
   Oauth 2.0 server implementations. Normally, they maintain their own
   user database, and go together with UI for user registration,
   login, and authorization consent.

To increase security and performance, it is highly recommended that
you configure HTTP reverse-proxy server(s) (like [nginx]) between your
clients and your "Payments Web API". In addition, this approach allows
different creditors to be located on different database servers
(sharding).


Configuration
-------------

The behavior of the running container can be tuned with environment
variables. Here are the most important settings with some random
example values:

```shell
# The creditors agent will be responsible only for creditor IDs
# between "$MIN_CREDITOR_ID" and "$MAX_CREDITOR_ID". This can be
# passed as a decimal number (like "4294967296"), or a
# hexadecimal number (like "0x100000000"). Numbers between
# 0x8000000000000000 and 0xffffffffffffffff will be automatically
# converted to their corresponding two's complement negative
# numbers.
MIN_CREDITOR_ID=4294967296
MAX_CREDITOR_ID=8589934591

# To improve the security of their accounts, creditors may configure
# PINs (Personal Identification Numbers). The PINs will be stored in
# the database encrypted. This variable determines the encryption key
# (default ""). If a malicious attacker knows the encryption key, and
# has a copy of the database, he/she will be able to decrypt all the
# PINs. If you lose this encryption key, the users' PINs will not be
# verified correctly, until each user configures a new PIN.
PIN_PROTECTION_SECRET=some-long-string-that-must-be-kept-secret

# The specified number of processes ("$WEBSERVER_PROCESSES") will be
# spawned to handle "Payments Web API" requests (default 1),
# each process will run "$WEBSERVER_THREADS" threads in parallel
# (default 3). The container will listen for "Payments Web API"
# requests on port "$WEBSERVER_PORT" (default 8080).
WEBSERVER_PROCESSES=2
WEBSERVER_THREADS=10
WEBSERVER_PORT=8003

# Requests to the "Payments Web API" are protected by an OAuth
# 2.0 authorization server. With every request, the client (a Web
# browser, for example) presents a token, and to verify the validity
# of the token, internally, a request is made to the OAuth 2.0
# authorization server. This is called "token introspection". This
# variable sets the URL at which internal token introspection requests
# will be sent.
#
# NOTE: The response to the "token introspection" request will contain
# a "username" field. The OAuth 2.0 authorization server must be
# configured to return usernames that match one of the following
# regular expressions: ^creditors-superuser$, ^creditors-supervisor$,
# ^creditors:([0-9]+)$. The "creditors-superuser" account will be
# allowed to do everything; the "creditors-supervisor" account will be
# allowed to view creditors' data, and to create new creditors; the
# "creditors:<CREDITOR_ID>" accounts will only be allowed access to
# the creditor with the specified <CREDITOR_ID> (an unsigned 64-bit
# integer).
OAUTH2_INTROSPECT_URL=http://localhost:4445/oauth2/introspect

# Connection string for a PostgreSQL database server to connect to.
POSTGRES_URL=postgresql://swpt_creditors:swpt_creditors@localhost:5435/test

# Connection string for a Redis database server to connect to.
REDIS_URL=redis://localhost:6380/0?health_check_interval=30

# Parameters for the communication with the RabbitMQ server which is
# responsible for brokering SMP messages. The container will connect
# to "$PROTOCOL_BROKER_URL" (default
# "amqp://guest:guest@localhost:5672"), will consume messages from the
# queue named "$PROTOCOL_BROKER_QUEUE" (default "swpt_creditors"),
# prefetching at most "$PROTOCOL_BROKER_PREFETCH_COUNT" messages at
# once (default 1). The specified number of processes
# ("$PROTOCOL_BROKER_PROCESSES") will be spawned to consume and
# process messages (default 1), each process will run
# "$PROTOCOL_BROKER_THREADS" threads in parallel (default 1). Note
# that PROTOCOL_BROKER_PROCESSES can be set to 0, in which case, the
# container will not consume any messages from the queue.
PROTOCOL_BROKER_URL=amqp://guest:guest@localhost:5672
PROTOCOL_BROKER_QUEUE=swpt_creditors
PROTOCOL_BROKER_PROCESSES=1
PROTOCOL_BROKER_THREADS=3
PROTOCOL_BROKER_PREFETCH_COUNT=10

# The binding key with which the "$PROTOCOL_BROKER_QUEUE"
# RabbitMQ queue is bound to the "creditors_in" RabbitMQ topic
# exchange (default "#"). The binding key must consist of zero or
# more 0s or 1s, separated by dots, ending with a hash symbol.
# For example: "0.1.#", "1.#", or "#".
PROTOCOL_BROKER_QUEUE_ROUTING_KEY=#

# All outgoing Swaptacular Messaging Protocol messages are first
# recorded in the PostgreSQL database, and then are "fulshed" to
# the RabbitMQ message broker. The specified number of
# processes ("$FLUSH_PROCESSES") will be spawned to flush
# messages (default 1). Note that FLUSH_PROCESSES can be set to
# 0, in which case, the container will not flush any messages.
# The "$FLUSH_ALL_WAIT" value specifies the number of seconds to
# wait between two sequential collective flushes (default 2).
FLUSH_PROCESSES=2
FLUSH_ALL_WAIT=1.5

# The processing of incoming events consists of several stages. The
# following configuration variables control the number of worker
# threads that will be involved on each respective stage (default
# 1). You must set this to a reasonable value, and increase it when
# you start experiencing problems with performance.
PROCESS_LOG_ADDITIONS_THREADS=10
PROCESS_LEDGER_UPDATES_THREADS=10

# Set this to "true" after splitting a parent database shard into
# two children shards. You may set this back to "false", once all
# left-over records from the parent have been deleted from the
# child shard.
DELETE_PARENT_SHARD_RECORDS=false

# Set the minimum level of severity for log messages ("info",
# "warning", or "error"). The default is "warning".
APP_LOG_LEVEL=info

# Set format for log messages ("text" or "json"). The default is
# "text".
APP_LOG_FORMAT=text
```

For more configuration options, check the
[development.env](../master/development.env) file.


Available commands
------------------

The [entrypoint](../master/docker/entrypoint.sh) of the docker
container allows you to execute the following *documented commands*:

* `all`

  Starts all the necessary services in the container. Also, this is
  the command that will be executed if no arguments are passed to the
  entrypoint.

  **IMPORTANT NOTE: For each database instance, you must start exactly
  one container with this command.**

* `configure`

  Initializes a new empty PostgreSQL database.

  **IMPORTANT NOTE: This command has to be run only once (at the
  beginning), but running it multiple times should not do any harm.**

* `webserver`

  Starts only the "Payments Web API" server. This command allows you
  to start as many additional dedicated web servers as necessary, to
  handle the incoming load.

* `consume_messages`

  Starts only the processes that consume SMP messages. This command
  allows you to start as many additional dedicated SMP message
  processors as necessary, to handle the incoming load.

* `flush_all`

  Starts processes that sends all types of outgoing messages to the RabbitMQ
  broker, and remove them from the PostgreSQL database.

  The "FLUSH_PROCESSES" environment variable specifies the number of started
  worker processes. The "FLUSH_ALL_WAIT" environment variable control the
  number of seconds to wait between two sequential collective flushes
  (default 2).

* `flush_configure_accounts`, `flush_prepare_transfers`,
  `flush_finalize_transfers`

  Starts processes that sends outgoing messages to the RabbitMQ broker, and
  remove them from the PostgreSQL database. These commands allow you to
  start additional processes, dedicated to the flushing of particular type
  of messages, to handle the load.

  The "FLUSH_PROCESSES" environment variables specifies the number of
  started worker processes. The "FLUSH_*message-type*_WAIT" environment
  variables control the number of seconds to wait between two sequential
  flushes (default 2). For example, configuring
  "FLUSH_PREPARE_TRANSFERS_WAIT=0.5" will try to flush the outgoing
  "PrapareTransfer" messages every 0.5 seconds.

This [docker-compose example](../master/docker-compose-all.yml) shows
how to use the generated docker images, along with the PostgerSQL
server, the RabbitMQ server, the Redis server, the OAuth 2.0
authorization server, and the HTTP reverse-proxy server.


How to run the tests
--------------------

1.  Install [Docker Engine] and [Docker Compose].

2.  To create an *.env* file with reasonable defalut values, run this
    command:

        $ cp development.env .env

3.  To run the unit tests, use the following commands:

        $ docker-compose build
        $ docker-compose run tests-config test


How to setup a development environment
--------------------------------------

1.  Install [Poetry](https://poetry.eustace.io/docs/).

2.  Create a new [Python](https://docs.python.org/) virtual
    environment and activate it.

3.  To install dependencies, run this command:

        $ poetry install

4.  To run the minimal set of services needed for development, use
    this command:

        $ docker-compose up --build

    This will start its own PostgreSQL and Redis server instances in
    docker containers, but will rely on being able to connect to a
    RabbitMQ server instance at
    "amqp://guest:guest@localhost:5672". The OAuth 2.0 authorization
    will be bypassed.

    Note that because the RabbitMQ "guest" user [can only connect from
    localhost], you should either explicitly allow the "guest" user to
    connect from anywhere, or create a new RabbitMQ user, and change
    the RabbitMQ connection URLs accordingly (`PROTOCOL_BROKER_URL` in
    the *.env* file).

5.  You can use `flask run -p 5000` to run a local web server, and
    `pytest --cov=swpt_creditors --cov-report=html` to run the tests
    and generate a test coverage report.


How to run all services (production-like)
-----------------------------------------

To start the "Creditors Agent" server, along with creditors UI webapp,
Swagger UI client, STOMP server, STOMP client, PostgerSQL server, RabbitMQ
server, Redis server, OAuth 2.0 authorization server, and HTTP reverse-proxy
server, use this command:

    $ docker-compose -f docker-compose-all.yml up --build

Then, you can open a browser window at
https://localhost:44301/creditors-webapp/ to use the creditors UI webapp, or
if you want to experiment with the Swagger UI client, go to
https://localhost:44301/creditors-swagger-ui/ and use client ID
`swagger-ui`, and client secret `swagger-ui` to authorize Swagger UI
to use the server API. In this testing environment, user registration
emails will be sent to a fake email server, whose messages can be read
at http://localhost:8025/

Note that the `docker/nodedata` directory contains an already populated
[root-CA database](https://github.com/swaptacular/swpt_ca_scripts), which
can be used for end-to-end testing.


[Swaptacular]: https://swaptacular.github.io/overview
[docker images]: https://www.geeksforgeeks.org/what-is-docker-images/
[Payments Web API]: https://swaptacular.github.io/public/docs/swpt_creditors/redoc.html
["wallet" client application]: https://github.com/swaptacular/swpt_creditors_ui
[Swagger UI]: https://swagger.io/tools/swagger-ui/
[JSON Serialization for the Swaptacular Messaging Protocol]: https://github.com/swaptacular/swpt_accounts/blob/master/protocol-json.rst
[PostgreSQL]: https://www.postgresql.org/
[Swaptacular Messaging Protocol]: https://github.com/swaptacular/swpt_accounts/blob/master/protocol.rst
[RabbitMQ]: https://www.rabbitmq.com/
[RabbitMQ queue]: https://www.cloudamqp.com/blog/part1-rabbitmq-for-beginners-what-is-rabbitmq.html
[RabbitMQ exchange]: https://www.cloudamqp.com/blog/part4-rabbitmq-for-beginners-exchanges-routing-keys-bindings.html
[Redis]: https://redis.io/
[OAuth 2.0]: https://oauth.net/2/
[nginx]: https://en.wikipedia.org/wiki/Nginx
[Docker Engine]: https://docs.docker.com/engine/
[Docker Compose]: https://docs.docker.com/compose/
[Poetry]: https://poetry.eustace.io/docs/
[Python]: https://docs.python.org/
[can only connect from localhost]: https://www.rabbitmq.com/access-control.html#loopback-users
