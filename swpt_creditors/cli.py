import logging
import time
import sys
import click
import threading
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


class ThreadPoolProcessor:
    def __init__(self, threads, *, get_args_collection, process_func, wait_seconds):
        self.logger = logging.getLogger(__name__)
        self.threads = threads
        self.get_args_collection = get_args_collection
        self.process_func = process_func
        self.wait_seconds = wait_seconds
        self.all_done = threading.Condition()
        self.pending = 0

    def _wait_until_all_done(self):
        while self.pending > 0:
            self.all_done.wait()
        assert self.pending == 0

    def _mark_done(self, result=None):
        with self.all_done:
            self.pending -= 1
            if self.pending <= 0:
                self.all_done.notify()

    def _log_error(self, e):  # pragma: no cover
        self._mark_done()
        try:
            raise e
        except Exception:
            self.logger.exception('Caught error while processing objects.')

    def run(self, *, quit_early=False):
        app = current_app._get_current_object()

        def push_app_context():
            ctx = app.app_context()
            ctx.push()

        pool = ThreadPool(self.threads, initializer=push_app_context)
        iteration_counter = 0

        while not (quit_early and iteration_counter > 0):
            iteration_counter += 1
            started_at = time.time()
            args_collection = self.get_args_collection()

            with self.all_done:
                self.pending += len(args_collection)

            for args in args_collection:
                pool.apply_async(self.process_func, args, callback=self._mark_done, error_callback=self._log_error)

            with self.all_done:
                self._wait_until_all_done()

            time.sleep(max(0.0, self.wait_seconds + started_at - time.time()))

        pool.close()
        pool.join()


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

    logger = logging.getLogger(__name__)
    channel = protocol_broker.channel
    channel.exchange_declare(MAIN_EXCHANGE_NAME)
    logger.info(f'Declared "{MAIN_EXCHANGE_NAME}" direct exchange.')

    if environ.get('APP_USE_LOAD_BALANCING_EXCHANGE', '') not in ['', 'False']:
        bind = channel.exchange_bind
        unbind = channel.exchange_unbind
    else:
        bind = channel.queue_bind
        unbind = channel.queue_unbind
    bind(queue_name, MAIN_EXCHANGE_NAME, queue_name)
    logger.info(f'Subscribed "{queue_name}" to "{MAIN_EXCHANGE_NAME}.{queue_name}".')

    for actor in [protocol_broker.get_actor(actor_name) for actor_name in protocol_broker.get_declared_actors()]:
        if 'event_subscription' in actor.options:
            routing_key = f'events.{actor.actor_name}'
            if actor.options['event_subscription']:
                bind(queue_name, MAIN_EXCHANGE_NAME, routing_key)
                logger.info(f'Subscribed "{queue_name}" to "{MAIN_EXCHANGE_NAME}.{routing_key}".')
            else:
                unbind(queue_name, MAIN_EXCHANGE_NAME, routing_key)
                logger.info(f'Unsubscribed "{queue_name}" from "{MAIN_EXCHANGE_NAME}.{routing_key}".')


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

    logger = logging.getLogger(__name__)

    def validate(value):
        if not MIN_INT64 <= value <= MAX_INT64:
            logger.error(f'{value} is not a valid creditor ID.')
            sys.exit(1)

    validate(min_id)
    validate(max_id)
    if min_id > max_id:
        logger.error('An invalid interval has been specified.')
        sys.exit(1)
    if min_id <= ROOT_CREDITOR_ID <= max_id:
        logger.error(f'The specified interval contains {ROOT_CREDITOR_ID}.')
        sys.exit(1)

    procedures.configure_agent(min_creditor_id=min_id, max_creditor_id=max_id)


@swpt_creditors.command('process_log_additions')
@with_appcontext
@click.option('-t', '--threads', type=int, help='The number of worker threads.')
@click.option('-w', '--wait', type=float, help='The minimal number of seconds between'
              ' the queries to obtain pending log entries.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def process_log_additions(threads, wait, quit_early):
    """Process pending log additions.

    If --threads is not specified, the value of the configuration
    variable APP_PROCESS_LOG_ADDITIONS_THREADS is taken. If it is not
    set, the default number of threads is 1.

    If --wait is not specified, the value of the configuration
    variable APP_PROCESS_LOG_ADDITIONS_WAIT is taken. If it is not
    set, the default number of seconds is 5.

    """

    threads = threads or current_app.config['APP_PROCESS_LOG_ADDITIONS_THREADS']
    wait = wait if wait is not None else current_app.config['APP_PROCESS_LOG_ADDITIONS_WAIT']

    logger = logging.getLogger(__name__)
    logger.info('Started log additions processor.')

    def get_args_collection():
        return [(creditor_id,) for creditor_id in procedures.get_creditors_with_pending_log_entries()]

    ThreadPoolProcessor(
        threads,
        get_args_collection=get_args_collection,
        process_func=procedures.process_pending_log_entries,
        wait_seconds=wait,
    ).run(quit_early=quit_early)


@swpt_creditors.command('process_ledger_updates')
@with_appcontext
@click.option('-t', '--threads', type=int, help='The number of worker threads.')
@click.option('-b', '--burst', type=int, help='The maximal number of transfers to process in'
              ' a single database transaction.')
@click.option('-w', '--wait', type=float, help='The minimal number of seconds between'
              ' the queries to obtain pending ledger updates.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def process_ledger_updates(threads, burst, wait, quit_early):
    """Process all pending ledger updates.

    If --threads is not specified, the value of the configuration
    variable APP_PROCESS_LEDGER_UPDATES_THREADS is taken. If it is not
    set, the default number of threads is 1.

    If --burst is not specified, the value of the configuration
    variable APP_PROCESS_LEDGER_UPDATES_BURST is taken. If it is not
    set, the default is 1000.

    If --wait is not specified, the value of the configuration
    variable APP_PROCESS_LEDGER_UPDATES_WAIT is taken. If it is not
    set, the default number of seconds is 5.

    """

    threads = threads or current_app.config['APP_PROCESS_LEDGER_UPDATES_THREADS']
    burst = burst or current_app.config['APP_PROCESS_LEDGER_UPDATES_BURST']
    wait = wait if wait is not None else current_app.config['APP_PROCESS_LEDGER_UPDATES_WAIT']
    max_delay = timedelta(days=current_app.config['APP_MAX_TRANSFER_DELAY_DAYS'])

    def process_ledger_update(creditor_id, debtor_id):
        while not procedures.process_pending_ledger_update(
                creditor_id, debtor_id, max_count=burst, max_delay=max_delay):
            pass

    logger = logging.getLogger(__name__)
    logger.info('Started ledger updates processor.')

    ThreadPoolProcessor(
        threads,
        get_args_collection=procedures.get_pending_ledger_updates,
        process_func=process_ledger_update,
        wait_seconds=wait,
    ).run(quit_early=quit_early)


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

    logger = logging.getLogger(__name__)
    logger.info('Started creditors scanner.')
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

    logger = logging.getLogger(__name__)
    logger.info('Started accounts scanner.')
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

    logger = logging.getLogger(__name__)
    logger.info('Started log entries scanner.')
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

    logger = logging.getLogger(__name__)
    logger.info('Started ledger entries scanner.')
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

    logger = logging.getLogger(__name__)
    logger.info('Started committed transfers scanner.')
    days = days or current_app.config['APP_COMMITTED_TRANSFERS_SCAN_DAYS']
    assert days > 0.0
    scanner = CommittedTransferScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)
