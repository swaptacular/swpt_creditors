import logging
import sys
import click
from os import environ
from datetime import timedelta
from multiprocessing.dummy import Pool as ThreadPool
from flask import current_app
from flask.cli import with_appcontext
from swpt_creditors.models import MIN_INT64, MAX_INT64
from swpt_creditors import procedures
from .extensions import db
from .table_scanners import AccountScanner


@click.group('swpt_creditors')
def swpt_creditors():
    """Perform swpt_creditors specific operations."""


@swpt_creditors.command()
@with_appcontext
@click.argument('queue_name')
def subscribe(queue_name):  # pragma: no cover
    """Subscribe a queue for the observed events and messages.

    QUEUE_NAME specifies the name of the queue.

    """

    from .extensions import broker, MAIN_EXCHANGE_NAME
    from . import actors  # noqa

    channel = broker.channel
    channel.exchange_declare(MAIN_EXCHANGE_NAME)
    click.echo(f'Declared "{MAIN_EXCHANGE_NAME}" direct exchange.')

    if environ.get('APP_USE_LOAD_BALANCING_EXCHANGE', '') not in ['', 'False']:
        bind = channel.exchange_bind
        unbind = channel.exchange_unbind
    else:
        bind = channel.queue_bind
        unbind = channel.queue_unbind
    bind(queue_name, MAIN_EXCHANGE_NAME, queue_name)
    click.echo(f'Subscribed "{queue_name}" to "{MAIN_EXCHANGE_NAME}.{queue_name}".')

    for actor in [broker.get_actor(actor_name) for actor_name in broker.get_declared_actors()]:
        if 'event_subscription' in actor.options:
            routing_key = f'events.{actor.actor_name}'
            if actor.options['event_subscription']:
                bind(queue_name, MAIN_EXCHANGE_NAME, routing_key)
                click.echo(f'Subscribed "{queue_name}" to "{MAIN_EXCHANGE_NAME}.{routing_key}".')
            else:
                unbind(queue_name, MAIN_EXCHANGE_NAME, routing_key)
                click.echo(f'Unsubscribed "{queue_name}" from "{MAIN_EXCHANGE_NAME}.{routing_key}".')


@swpt_creditors.command('configure_interval')
@with_appcontext
@click.argument('min_id', type=int)
@click.argument('max_id', type=int)
def configure_interval(min_id, max_id):
    """Configures the server to manage creditor IDs between MIN_ID and MAX_ID.

    The passed creditor IDs must be between -9223372036854775808 and
    9223372036854775807. Use "--" to pass negative integers. For
    example:

    $ flask swpt_creditors configure_interval -- -16 0

    """

    def validate(value):
        if not MIN_INT64 <= value <= MAX_INT64:
            click.echo(f'Error: {value} is not a valid creditor ID.')
            sys.exit(1)

    validate(min_id)
    validate(max_id)
    if min_id > max_id:
        click.echo('Error: an invalid interval has been specified.')
        sys.exit(1)

    procedures.configure_agent(min_creditor_id=min_id, max_creditor_id=max_id)


@swpt_creditors.command('process_log_entries')
@with_appcontext
@click.option('-t', '--threads', type=int, help='The number of worker threads.')
def process_log_entries(threads):
    """Process all pending log entries."""

    threads = threads or int(environ.get('APP_PROCESS_LOG_ENTRIES_THREADS', '1'))
    app = current_app._get_current_object()

    def push_app_context():
        ctx = app.app_context()
        ctx.push()

    def log_error(e):  # pragma: no cover
        try:
            raise e
        except Exception:
            logger = logging.getLogger(__name__)
            logger.exception('Caught error while processing log entries.')

    pool = ThreadPool(threads, initializer=push_app_context)
    for creditor_id in procedures.get_creditors_with_pending_log_entries():
        pool.apply_async(procedures.process_pending_log_entries, (creditor_id,), error_callback=log_error)
    pool.close()
    pool.join()


@swpt_creditors.command('process_ledger_updates')
@with_appcontext
@click.option('-t', '--threads', type=int, help='The number of worker threads.')
@click.option('-b', '--burst', type=int, help='The number of transfers to process in a single database transaction.')
def process_ledger_updates(threads, burst):
    """Process all pending ledger updates."""

    threads = threads or int(environ.get('APP_PROCESS_LEDGER_UPDATES_THREADS', '1'))
    burst = burst or int(environ.get('APP_PROCESS_LEDGER_UPDATES_BURST', '1000'))
    max_delay = timedelta(days=float(current_app.config['APP_MAX_TRANSFER_DELAY_DAYS']))
    app = current_app._get_current_object()

    def push_app_context():
        ctx = app.app_context()
        ctx.push()

    def log_error(e):  # pragma: no cover
        try:
            raise e
        except Exception:
            logger = logging.getLogger(__name__)
            logger.exception('Caught error while processing ledger updates.')

    def process_ledger_update(creditor_id, debtor_id):
        while not procedures.process_pending_ledger_update(
                creditor_id, debtor_id, max_count=burst, max_delay=max_delay):
            pass

    pool = ThreadPool(threads, initializer=push_app_context)
    for account_pk in procedures.get_pending_ledger_updates():
        pool.apply_async(process_ledger_update, account_pk, error_callback=log_error)
    pool.close()
    pool.join()


@swpt_creditors.command('scan_accounts')
@with_appcontext
@click.option('-h', '--hours', type=float, help='The number of hours.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def scan_accounts(hours, quit_early):
    """Start a process that executes accounts maintenance operations.

    The specified number of hours determines the intended duration of
    a single pass through the accounts table. If the number of hours
    is not specified, the default number of hours is 8.

    """

    click.echo('Scanning accounts...')
    hours = hours or 8
    assert hours > 0.0
    scanner = AccountScanner()
    scanner.run(db.engine, timedelta(hours=hours), quit_early=quit_early)
