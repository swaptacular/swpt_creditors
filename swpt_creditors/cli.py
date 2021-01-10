import logging
import sys
import click
from os import environ
from datetime import timedelta
from multiprocessing.dummy import Pool as ThreadPool
from flask import current_app
from flask.cli import with_appcontext
from swpt_creditors.models import MIN_INT64, MAX_INT64, ROOT_CREDITOR_ID
from swpt_creditors import procedures
from .extensions import db
from .table_scanners import CreditorScanner, AccountScanner, LogEntryScanner, LedgerEntryScanner, \
    CommittedTransferScanner


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

    from .extensions import protocol_broker, MAIN_EXCHANGE_NAME
    from . import actors  # noqa

    channel = protocol_broker.channel
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

    for actor in [protocol_broker.get_actor(actor_name) for actor_name in protocol_broker.get_declared_actors()]:
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
    if min_id <= ROOT_CREDITOR_ID <= max_id:
        click.echo(f'Error: the specified interval contains {ROOT_CREDITOR_ID}.')
        sys.exit(1)

    procedures.configure_agent(min_creditor_id=min_id, max_creditor_id=max_id)


@swpt_creditors.command('process_log_entries')
@with_appcontext
@click.option('-t', '--threads', type=int, help='The number of worker threads.')
def process_log_entries(threads):
    """Process all pending log entries.

    If --threads is not specified, the value of the configuration
    variable APP_PROCESS_LOG_ENTRIES_THREADS is taken (the default is
    1).

    """

    threads = threads or current_app.config['APP_PROCESS_LOG_ENTRIES_THREADS']
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
    """Process all pending ledger updates.

    If --threads is not specified, the value of the configuration
    variable APP_PROCESS_LEDGER_UPDATES_THREADS is taken (the default
    is 1).

    If --burst is not specified, the value of the configuration
    variable APP_PROCESS_LEDGER_UPDATES_BURST is taken (the default is
    1000).

    """

    threads = threads or current_app.config['APP_PROCESS_LEDGER_UPDATES_THREADS']
    burst = burst or current_app.config['APP_PROCESS_LEDGER_UPDATES_BURST']
    max_delay = timedelta(days=current_app.config['APP_MAX_TRANSFER_DELAY_DAYS'])
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


@swpt_creditors.command('scan_creditors')
@with_appcontext
@click.option('-d', '--days', type=float, help='The number of days.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def scan_creditors(days, quit_early):
    """Start a process that garbage-collects inactive creditors.

    The specified number of days determines the intended duration of a
    single pass through the creditors table. If the number of days is
    not specified, the value of the configuration variable
    APP_CREDITORS_SCAN_DAYS is taken. If it is not set, the default
    number of days is 7.

    """

    click.echo('Scanning creditors...')
    days = days or current_app.config['APP_CREDITORS_SCAN_DAYS']
    assert days > 0.0
    scanner = CreditorScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_creditors.command('scan_accounts')
@with_appcontext
@click.option('-h', '--hours', type=float, help='The number of hours.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def scan_accounts(hours, quit_early):
    """Start a process that executes accounts maintenance operations.

    The specified number of hours determines the intended duration of
    a single pass through the accounts table. If the number of hours
    is not specified, the value of the configuration variable
    APP_ACCOUNTS_SCAN_HOURS is taken. If it is not set, the default
    number of hours is 8.

    """

    click.echo('Scanning accounts...')
    hours = hours or current_app.config['APP_ACCOUNTS_SCAN_HOURS']
    assert hours > 0.0
    scanner = AccountScanner()
    scanner.run(db.engine, timedelta(hours=hours), quit_early=quit_early)


@swpt_creditors.command('scan_log_entries')
@with_appcontext
@click.option('-d', '--days', type=float, help='The number of days.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def scan_log_entries(days, quit_early):
    """Start a process that garbage-collects staled log entries.

    The specified number of days determines the intended duration of a
    single pass through the log entries table. If the number of days
    is not specified, the value of the configuration variable
    APP_LOG_ENTRIES_SCAN_DAYS is taken. If it is not set, the default
    number of days is 7.

    """

    click.echo('Scanning log entries...')
    days = days or current_app.config['APP_LOG_ENTRIES_SCAN_DAYS']
    assert days > 0.0
    scanner = LogEntryScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_creditors.command('scan_ledger_entries')
@with_appcontext
@click.option('-d', '--days', type=float, help='The number of days.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def scan_ledger_entries(days, quit_early):
    """Start a process that garbage-collects staled ledger entries.

    The specified number of days determines the intended duration of a
    single pass through the ledger entries table. If the number of
    days is not specified, the value of the configuration variable
    APP_LEDGER_ENTRIES_SCAN_DAYS is taken. If it is not set, the
    default number of days is 7.

    """

    click.echo('Scanning ledger entries...')
    days = days or current_app.config['APP_LEDGER_ENTRIES_SCAN_DAYS']
    assert days > 0.0
    scanner = LedgerEntryScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_creditors.command('scan_committed_transfers')
@with_appcontext
@click.option('-d', '--days', type=float, help='The number of days.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def scan_committed_transfers(days, quit_early):
    """Start a process that garbage-collects staled committed transfers.

    The specified number of days determines the intended duration of a
    single pass through the committed transfers table. If the number
    of days is not specified, the value of the configuration variable
    APP_COMMITTED_TRANSFERS_SCAN_DAYS is taken. If it is not set, the
    default number of days is 7.

    """

    click.echo('Scanning committed transfers...')
    days = days or current_app.config['APP_COMMITTED_TRANSFERS_SCAN_DAYS']
    assert days > 0.0
    scanner = CommittedTransferScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)
