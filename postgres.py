#!/usr/bin/python3
import argparse
import configparser
from functools import cached_property

from services.factory import ArgsFactory


class Processor:
    def __init__(self):
        self.sys_args = self.get_sys_args()
        self.config = self.get_config()

    @cached_property
    def pg_kwargs(self):
        return {
            'user': self.config.get('postgresql', 'user'),
            'password': self.config.get('postgresql', 'password'),
            'host': self.config.get('postgresql', 'host'),
            'port': self.config.get('postgresql', 'port'),
            'db': self.config.get('postgresql', 'db')
        }

    @property
    def kwargs(self):
        default = {'config': self.config}

        services = {
            'list': default,
            'list_dbs': default,
            'backup': default,
            'restore': {
                **default,
                'date': self.sys_args.date,
            },
        }
        return services[self.sys_args.action]

    def get_config(self):
        config = configparser.ConfigParser()
        config.read(self.sys_args.configfile)
        return config

    def get_sys_args(self):
        args_parser = argparse.ArgumentParser(description='Postgres Database Management')
        args_parser.add_argument(
            '--action',
            metavar='action',
            choices=['list', 'list_dbs', 'restore', 'backup'],
            required=True,
        )
        args_parser.add_argument(
            '--date',
            metavar='YYYY-MM-dd',
            help='Date to use for restore (shown with --action list)',
        )
        args_parser.add_argument(
            '--dest-db',
            metavar='dest_db',
            default=None,
            help='Name of the new restored database',
        )
        args_parser.add_argument('--verbose', default=False, help='Verbose output')
        args_parser.add_argument(
            '--configfile', required=True, help='Database configuration file'
        )
        return args_parser.parse_args()

    def process(self):
        service = ArgsFactory.get_service(self.sys_args.action, **self.kwargs)
        service.process()


if __name__ == '__main__':
    Processor().process()
