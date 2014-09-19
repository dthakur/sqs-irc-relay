import logging
import time
import json
import boto.sqs
import itertools

from format import *
from irc.bot import ServerSpec, SingleServerIRCBot
from functools import partial
from boto.sqs.message import RawMessage
from secrets import SQS_KEY, SQS_SECRET

QUEUE_NAME = 'sqslog'
IRC_SERVER = '127.0.0.1'
IRC_PORT = 6667
IRC_NICK = 'btxbot'

CHANNELS = ['all', 'login']


class IrcLogger(SingleServerIRCBot):
    def __init__(self, queue, server=IRC_SERVER, port=IRC_PORT, nick=IRC_NICK):
        SingleServerIRCBot.__init__(self, [(server, port)], nick, nick)
        self.queue = queue

    def on_welcome(self, connection, event):
        logging.info(
            'connected to irc server server=%s',
            connection.server_address)

        for c in CHANNELS:
            connection.join('#' + c)

        # race with on_join
        self.run_sqs_loop()

    def on_disconnect(self, connection, event):
        raise Exception('disconnected from irc')

    def on_join(self, connection, event):
        logging.info('joined irc channel=%s', event.target)

    def run_sqs_loop(self):
        while True:
            self.sqs_poll()
            time.sleep(1)

    def sqs_poll(self):
        rs = self.queue.get_messages(
            visibility_timeout=120,
            wait_time_seconds=10)

        for m in rs:
            body = json.loads(m.get_body())
            try:
                self.sqs_process(m, body)
            except Exception, e:
                raise e

    def sqs_process(self, message, body):
        logging.info('message: %s', body)
        name = body['name']
        d = body['data']

        items = ['%s=%s' % (k, bold(v if v is not None else 'None')) for (k, v) in d.iteritems()]
        d = ', '.join(items)

        self.connection.privmsg('#' + name, d)
        self.connection.privmsg('#all', d)
        self.queue.delete_message(message)


def run():
    logging.info('connecting to sqs queue=%s', QUEUE_NAME)
    conn = boto.sqs.connect_to_region(
        'us-east-1',
        aws_access_key_id=SQS_KEY,
        aws_secret_access_key=SQS_SECRET)

    q = conn.create_queue(QUEUE_NAME)
    q.set_message_class(RawMessage)

    client = IrcLogger(q)
    client.start()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s %(levelname)s - %(message)s')

    run()
