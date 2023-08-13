import logging
import os
import time
import signal
import sys
import click
import pika
from typing import Optional, Any
from datetime import timedelta
from flask import current_app
from flask.cli import with_appcontext
from flask_sqlalchemy.model import Model
from swpt_creditors import procedures
from .extensions import db
from .table_scanners import CreditorScanner, AccountScanner, LogEntryScanner, LedgerEntryScanner, \
    CommittedTransferScanner
from swpt_pythonlib.multiproc_utils import ThreadPoolProcessor, spawn_worker_processes, \
    try_unblock_signals, HANDLED_SIGNALS
from swpt_pythonlib.flask_signalbus import SignalBus


@click.group('swpt_creditors')
def swpt_creditors():
    """Perform swpt_creditors specific operations."""


@swpt_creditors.command()
@with_appcontext
def subscribe():  # pragma: no cover
    """Declare a RabbitMQ queue, and subscribe it to receive incoming
    messages.

    The value of the PROTOCOL_BROKER_QUEUE_ROUTING_KEY configuration
    variable will be used as a binding key for the created queue. The
    default binding key is "#".

    This is mainly useful during development and testing.

    """

    from .extensions import CREDITORS_IN_EXCHANGE, CREDITORS_OUT_EXCHANGE

    logger = logging.getLogger(__name__)
    queue_name = current_app.config['PROTOCOL_BROKER_QUEUE']
    routing_key = current_app.config['PROTOCOL_BROKER_QUEUE_ROUTING_KEY']
    dead_letter_queue_name = queue_name + '.XQ'
    broker_url = current_app.config['PROTOCOL_BROKER_URL']
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()

    # declare exchanges
    channel.exchange_declare(CREDITORS_IN_EXCHANGE, exchange_type='topic', durable=True)
    channel.exchange_declare(CREDITORS_OUT_EXCHANGE, exchange_type='topic', durable=True)

    # declare a corresponding dead-letter queue
    channel.queue_declare(dead_letter_queue_name, durable=True)
    logger.info('Declared "%s" dead-letter queue.', dead_letter_queue_name)

    # declare the queue
    channel.queue_declare(queue_name, durable=True, arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": dead_letter_queue_name,
    })
    logger.info('Declared "%s" queue.', queue_name)

    # bind the queue
    channel.queue_bind(exchange=CREDITORS_IN_EXCHANGE, queue=queue_name, routing_key=routing_key)
    logger.info('Created a binding from "%s" to "%s" with routing key "%s".',
                CREDITORS_IN_EXCHANGE, queue_name, routing_key)


@swpt_creditors.command('process_log_additions')
@with_appcontext
@click.option('-t', '--threads', type=int, help='The number of worker threads.')
@click.option('-w', '--wait', type=float, help='The minimal number of seconds between'
              ' the queries to obtain pending log entries.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def process_log_additions(threads, wait, quit_early):
    """Process pending log additions.

    If --threads is not specified, the value of the configuration
    variable PROCESS_LOG_ADDITIONS_THREADS is taken. If it is not
    set, the default number of threads is 1.

    If --wait is not specified, the value of the configuration
    variable APP_PROCESS_LOG_ADDITIONS_WAIT is taken. If it is not
    set, the default number of seconds is 5.

    """

    # TODO: Consider allowing load-sharing between multiple processes
    #       or containers. This may also be true for the other
    #       "process_*" CLI commands. A possible way to do this is to
    #       separate the `args collection` in multiple buckets,
    #       assigning a dedicated process/container for each bucket.
    #       Note that this would makes sense only if the load is
    #       CPU-bound, which is unlikely, especially if we
    #       re-implement the logic in stored procedures.

    threads = threads or current_app.config['PROCESS_LOG_ADDITIONS_THREADS']
    wait = wait if wait is not None else current_app.config['APP_PROCESS_LOG_ADDITIONS_WAIT']
    max_count = current_app.config['APP_PROCESS_LOG_ADDITIONS_MAX_COUNT']

    def get_args_collection():
        return procedures.get_creditors_with_pending_log_entries(max_count=max_count)

    logger = logging.getLogger(__name__)
    logger.info('Started log additions processor.')

    ThreadPoolProcessor(
        threads,
        get_args_collection=get_args_collection,
        process_func=procedures.process_pending_log_entries,
        wait_seconds=wait,
        max_count=max_count,
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
    variable PROCESS_LEDGER_UPDATES_THREADS is taken. If it is not
    set, the default number of threads is 1.

    If --burst is not specified, the value of the configuration
    variable APP_PROCESS_LEDGER_UPDATES_BURST is taken. If it is not
    set, the default is 1000.

    If --wait is not specified, the value of the configuration
    variable APP_PROCESS_LEDGER_UPDATES_WAIT is taken. If it is not
    set, the default number of seconds is 5.

    """

    threads = threads or current_app.config['PROCESS_LEDGER_UPDATES_THREADS']
    burst = burst or current_app.config['APP_PROCESS_LEDGER_UPDATES_BURST']
    wait = wait if wait is not None else current_app.config['APP_PROCESS_LEDGER_UPDATES_WAIT']
    max_count = current_app.config['APP_PROCESS_LEDGER_UPDATES_MAX_COUNT']
    max_delay = timedelta(days=current_app.config['APP_MAX_TRANSFER_DELAY_DAYS'])

    def get_args_collection():
        return procedures.get_pending_ledger_updates(max_count=max_count)

    def process_ledger_update(creditor_id, debtor_id):
        while not procedures.process_pending_ledger_update(
                creditor_id, debtor_id, max_count=burst, max_delay=max_delay):
            pass

    logger = logging.getLogger(__name__)
    logger.info('Started ledger updates processor.')

    ThreadPoolProcessor(
        threads,
        get_args_collection=get_args_collection,
        process_func=process_ledger_update,
        wait_seconds=wait,
        max_count=max_count,
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


@swpt_creditors.command('consume_messages')
@with_appcontext
@click.option('-u', '--url', type=str, help='The RabbitMQ connection URL.')
@click.option('-q', '--queue', type=str, help='The name the queue to consume from.')
@click.option('-p', '--processes', type=int, help='The number of worker processes.')
@click.option('-t', '--threads', type=int, help='The number of threads running in each process.')
@click.option('-s', '--prefetch-size', type=int, help='The prefetch window size in bytes.')
@click.option('-c', '--prefetch-count', type=int, help='The prefetch window in terms of whole messages.')
def consume_messages(url, queue, processes, threads, prefetch_size, prefetch_count):
    """Consume and process incoming Swaptacular Messaging Protocol
    messages.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_creditors")

    * PROTOCOL_BROKER_PROCESSES (defalut 1)

    * PROTOCOL_BROKER_THREADS (defalut 1)

    * PROTOCOL_BROKER_PREFETCH_COUNT (default 1)

    * PROTOCOL_BROKER_PREFETCH_SIZE (default 0, meaning unlimited)

    """

    def _consume_messages(url, queue, threads, prefetch_size, prefetch_count):  # pragma: no cover
        """Consume messages in a subprocess."""

        from swpt_creditors.actors import SmpConsumer, TerminatedConsumtion
        from swpt_creditors import create_app

        consumer = SmpConsumer(
            app=create_app(),
            config_prefix='PROTOCOL_BROKER',
            url=url,
            queue=queue,
            threads=threads,
            prefetch_size=prefetch_size,
            prefetch_count=prefetch_count,
        )
        for sig in HANDLED_SIGNALS:
            signal.signal(sig, consumer.stop)
        try_unblock_signals()

        pid = os.getpid()
        logger = logging.getLogger(__name__)
        logger.info('Worker with PID %i started processing messages.', pid)

        try:
            consumer.start()
        except TerminatedConsumtion:
            pass

        logger.info('Worker with PID %i stopped processing messages.', pid)

    spawn_worker_processes(
        processes=processes or current_app.config['PROTOCOL_BROKER_PROCESSES'],
        target=_consume_messages,
        url=url,
        queue=queue,
        threads=threads,
        prefetch_size=prefetch_size,
        prefetch_count=prefetch_count,
    )
    sys.exit(1)


@swpt_creditors.command('flush_messages')
@with_appcontext
@click.option('-p', '--processes', type=int, help='Then umber of worker processes.'
              ' If not specified, the value of the FLUSH_PROCESSES environment'
              ' variable will be used, defaulting to 1 if empty.')
@click.option('-w', '--wait', type=float, default=2.0, help='Flush every FLOAT seconds (defalut 2).')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
@click.argument('message_types', nargs=-1)
def flush_messages(
        message_types: list[str],
        processes: int, wait:
        float, quit_early: bool,
) -> None:
    """Send pending messages to the message broker.

    If a list of MESSAGE_TYPES is given, flushes only these types of
    messages. If no MESSAGE_TYPES are specified, flushes all messages.

    """
    signalbus: SignalBus = current_app.extensions['signalbus']
    logger = logging.getLogger(__name__)

    def _get_models_to_flush(model_names: list[str]) -> list[type[Model]]:
        signal_names = set(model_names)
        wrong_names = set()
        models_to_flush = signalbus.get_signal_models()
        if signal_names:
            wrong_names = signal_names - {m.__name__ for m in models_to_flush}
            models_to_flush = [
                m for m in models_to_flush if m.__name__ in signal_names]

        for name in wrong_names:  # pragma: no cover
            logger.warning('A signal with name "%s" does not exist.', name)

        return models_to_flush

    models_to_flush = _get_models_to_flush(message_types)
    logger.info('Started flushing %s.',
                ', '.join(m.__name__ for m in models_to_flush))

    def _flush(
            models_to_flush: list[type[Model]],
            wait: Optional[float],
    ) -> None:  # pragma: no cover
        from swpt_creditors import create_app
        app = create_app()
        stopped = False

        def stop(signum: Any = None, frame: Any = None) -> None:
            nonlocal stopped
            stopped = True

        for sig in HANDLED_SIGNALS:
            signal.signal(sig, stop)
        try_unblock_signals()

        with app.app_context():
            signalbus: SignalBus = current_app.extensions['signalbus']
            while not stopped:
                started_at = time.time()
                try:
                    count = signalbus.flushmany(models_to_flush)
                except Exception:
                    logger.exception('Caught error while sending pending signals.')
                    sys.exit(1)

                if count > 0:
                    logger.info('%i signals have been successfully processed.', count)
                else:
                    logger.debug('0 signals have been processed.')

                if quit_early:
                    break
                time.sleep(max(0.0, wait + started_at - time.time()))

    spawn_worker_processes(
        processes=(processes if processes is not None
                   else current_app.config['FLUSH_PROCESSES']),
        target=_flush,
        models_to_flush=models_to_flush,
        wait=wait,
    )
    sys.exit(1)
