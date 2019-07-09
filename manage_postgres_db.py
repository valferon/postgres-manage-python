#!/usr/bin/python3
import argparse
import logging
import subprocess
import os
import tempfile
from tempfile import mkstemp

import configparser
import gzip
import boto3
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Amazon S3 settings.
# AWS_ACCESS_KEY_ID  in ~/.aws/credentials
# AWS_SECRET_ACCESS_KEY in ~/.aws/credentials
import datetime

from shutil import move

AWS_BUCKET_NAME = 'backup.mydomain.com'
AWS_BUCKET_PATH = 'postgres/'
BACKUP_PATH = '/tmp/'


def upload_to_s3(file_full_path, dest_file):
    """
    Upload a file to an AWS S3 bucket.
    """
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_full_path, AWS_BUCKET_NAME, AWS_BUCKET_PATH + dest_file)
        os.remove(file_full_path)
    except boto3.exceptions.S3UploadFailedError as exc:
        print(exc)
        exit(1)


def download_from_s3(backup_s3_key, dest_file):
    """
    Upload a file to an AWS S3 bucket.
    """
    s3_client = boto3.resource('s3')
    try:
        s3_client.meta.client.download_file(AWS_BUCKET_NAME, backup_s3_key, dest_file)
    except Exception as e:
        print(e)
        exit(1)


def list_available_backup():
    key_list = []
    s3_client = boto3.client('s3')
    s3_objects = s3_client.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix=AWS_BUCKET_PATH)

    for key in s3_objects['Contents']:
        key_list.append(key['Key'])
    return key_list


def list_postgres_databases(host, database_name, port, user, password):
    try:
        process = subprocess.Popen(
            ['psql',
             '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, database_name),
             '--list'],
            stdout=subprocess.PIPE
        )
        output = process.communicate()[0]
        if int(process.returncode) != 0:
            print('Command failed. Return code : {}'.format(process.returncode))
            exit(1)
        return output
    except Exception as e:
        print(e)
        exit(1)


def backup_postgres_db(host, database_name, port, user, password, dest_file, verbose):
    """
    Backup postgres db to a file.
    """
    if verbose:
        try:
            process = subprocess.Popen(
                ['pg_dump',
                 '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, database_name),
                 '-Fc',
                 '-f', dest_file,
                 '-v'],
                stdout=subprocess.PIPE
            )
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print('Command failed. Return code : {}'.format(process.returncode))
                exit(1)
            return output
        except Exception as e:
            print(e)
            exit(1)
    else:

        try:
            process = subprocess.Popen(
                ['pg_dump',
                 '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, database_name),
                 '-f', dest_file],
                stdout=subprocess.PIPE
            )
            output = process.communicate()[0]
            if process.returncode != 0:
                print('Command failed. Return code : {}'.format(process.returncode))
                exit(1)
            return output
        except Exception as e:
            print(e)
            exit(1)


def compress_file(src_file):
    compressed_file = "{}.gz".format(str(src_file))
    with open(src_file, 'rb') as f_in:
        with gzip.open(compressed_file, 'wb') as f_out:
            for line in f_in:
                f_out.write(line)
    return compressed_file


def extract_file(src_file):
    extracted_file, extension = os.path.splitext(src_file)
    print(extracted_file)
    with gzip.open(src_file, 'rb') as f_in:
        with open(extracted_file, 'wb') as f_out:
            for line in f_in:
                f_out.write(line)
    return extracted_file

def remove_faulty_statement_from_dump(src_file):

    temp_file, _ = tempfile.mkstemp()

    try:
        with open(temp_file, 'w+') as dump_temp:
            process = subprocess.Popen(
                ['pg_restore',
                 '-l'
                 '-v',
                 src_file],
                stdout=subprocess.PIPE
            )
            output = subprocess.check_output(('grep','-v','"EXTENSION - plpgsql"'), stdin=process.stdout)
            process.wait()
            if int(process.returncode) != 0:
                print('Command failed. Return code : {}'.format(process.returncode))
                exit(1)

            os.remove(src_file)
            with open(src_file, 'w+') as cleaned_dump:
                subprocess.call(
                    ['pg_restore',
                     '-L'],
                    stdin=output,
                    stdout=cleaned_dump
                )

    except Exception as e:
        print("Issue when modifying dump : {}".format(e))


def change_user_from_dump(source_dump_path, old_user, new_user):
    fh, abs_path = mkstemp()
    with os.fdopen(fh, 'w') as new_file:
        with open(source_dump_path) as old_file:
            for line in old_file:
                new_file.write(line.replace(old_user, new_user))
    # Remove original file
    os.remove(source_dump_path)
    # Move new file
    move(abs_path, source_dump_path)


def restore_postgres_db(db_host, db, port, user, password, backup_file, verbose):
    """
    Restore postgres db from a file.
    """

    if verbose:
        try:
            print(user,password,db_host,port, db)
            process = subprocess.Popen(
                ['pg_restore',
                 '--no-owner',
                 '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user,
                                                               password,
                                                               db_host,
                                                               port, db),
                 '-v',
                 backup_file],
                stdout=subprocess.PIPE
            )
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print('Command failed. Return code : {}'.format(process.returncode))

            return output
        except Exception as e:
            print("Issue with the db restore : {}".format(e))
    else:
        try:
            process = subprocess.Popen(
                ['pg_restore',
                 '--no-owner',
                 '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user,
                                                                      password,
                                                                      db_host,
                                                                      port, db),
                 backup_file],
                stdout=subprocess.PIPE
            )
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print('Command failed. Return code : {}'.format(process.returncode))

            return output
        except Exception as e:
            print("Issue with the db restore : {}".format(e))


def create_db(db_host, database, db_port, user_name, user_password):
    try:
        con = psycopg2.connect(dbname='postgres', port=db_port,
                               user=user_name, host=db_host,
                               password=user_password)

    except Exception as e:
        print(e)
        exit(1)

    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    try:
        cur.execute("DROP DATABASE {} ;".format(database))
    except Exception as e:
        print('DB does not exist, nothing to drop')
    cur.execute("CREATE DATABASE {} ;".format(database))
    cur.execute("GRANT ALL PRIVILEGES ON DATABASE {} TO {} ;".format(database, user_name))
    return database


def swap_restore_active(db_host, restore_database, active_database, db_port, user_name, user_password):
    try:
        con = psycopg2.connect(dbname='postgres', port=db_port,
                               user=user_name, host=db_host,
                               password=user_password)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute("SELECT pg_terminate_backend( pid ) "
                    "FROM pg_stat_activity "
                    "WHERE pid <> pg_backend_pid( ) "
                    "AND datname = '{}'".format(active_database))
        cur.execute("DROP DATABASE {}".format(active_database))
        cur.execute('ALTER DATABASE "{}" RENAME TO "{}";'.format(restore_database, active_database))
    except Exception as e:
        print(e)
        exit(1)

def swap_restore_new(db_host, restore_database, new_database, db_port, user_name, user_password):
    try:
        con = psycopg2.connect(dbname='postgres', port=db_port,
                               user=user_name, host=db_host,
                               password=user_password)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute('ALTER DATABASE "{}" RENAME TO "{}";'.format(restore_database, new_database))
    except Exception as e:
        print(e)
        exit(1)


def main():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    args_parser = argparse.ArgumentParser(description='Postgres database management')
    args_parser.add_argument("--action",
                             metavar="action",
                             choices=['list', 'list_dbs', 'restore', 'backup'],
                             required=True)
    args_parser.add_argument("--date",
                             metavar="YYYY-MM-dd",
                             help="Date to use for restore (show with --action list)")
    args_parser.add_argument("--dest-db",
                             metavar="dest_db",
                             default=None,
                             help="Name of the new restored database")
    args_parser.add_argument("--verbose",
                             default=True,
                             help="verbose output")
    args_parser.add_argument("--configfile",
                             required=True,
                             help="Database configuration file")
    args = args_parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.configfile)

    postgres_host = config.get('postgresql', 'host')
    postgres_port = config.get('postgresql', 'port')
    postgres_db = config.get('postgresql', 'db')
    postgres_restore = "{}_restore".format(postgres_db)
    postgres_user = config.get('postgresql', 'user')
    postgres_password = config.get('postgresql', 'password')
    aws_bucket_name = config.get('S3', 'bucket_name')
    aws_bucket_path = config.get('S3', 'bucket_backup_path')
    timestr = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    filename = 'backup-{}-{}.dump'.format(timestr, postgres_db)
    filename_compressed = '{}.gz'.format(filename)
    restore_filename = '/tmp/restore.dump.gz'
    restore_uncompressed = '/tmp/restore.dump'
    local_file_path = '{}{}'.format(BACKUP_PATH, filename)

    # list task
    if args.action == "list":
        logger.info('Listing S3 bucket s3://{}/{} content :'.format(aws_bucket_name,
                                                                    aws_bucket_path))
        s3_backup_objects = list_available_backup()
        for key in s3_backup_objects:
            logger.info("Key : {}".format(key))
    # list databases task
    elif args.action == "list_dbs":
        result = list_postgres_databases(postgres_host,
                                         postgres_db,
                                         postgres_port,
                                         postgres_user,
                                         postgres_password)
        for line in result.splitlines():
            logger.info(line)
    # backup task
    elif args.action == "backup":
        logger.info('Backing up {} database to {}'.format(postgres_db, local_file_path))
        result = backup_postgres_db(postgres_host,
                                    postgres_db,
                                    postgres_port,
                                    postgres_user,
                                    postgres_password,
                                    local_file_path, args.verbose)
        for line in result.splitlines():
            logger.info(line)

        logger.info("Backup complete")
        logger.info("Compressing {}".format(local_file_path))
        comp_file = compress_file(local_file_path)
        logger.info('Uploading {} to Amazon S3...'.format(comp_file))
        upload_to_s3(comp_file, filename_compressed)
        logger.info("Uploaded to {}".format(filename_compressed))
    # restore task
    elif args.action == "restore":
        if not args.date:
            logger.warn('No date was chosen for restore. Run again with the "list" '
                        'action to see available restore dates')
        else:
            try:
                os.remove(restore_filename)
            except Exception as e:
                logger.info(e)
            all_backup_keys = list_available_backup()
            backup_match = [s for s in all_backup_keys if args.date in s]
            if backup_match:
                logger.info("Found the following backup : {}".format(backup_match))
            else:
                logger.error("No match found for backups with date : {}".format(args.date))
                logger.info("Available keys : {}".format([s for s in all_backup_keys]))
                exit(1)

            logger.info("Downloading {} from S3 into : {}".format(backup_match[0], restore_filename))
            download_from_s3(backup_match[0], restore_filename)
            logger.info("Download complete")
            logger.info("Extracting {}".format(restore_filename))
            ext_file = extract_file(restore_filename)
            # cleaned_ext_file = remove_faulty_statement_from_dump(ext_file)
            logger.info("Extracted to : {}".format(ext_file))
            logger.info("Creating temp database for restore : {}".format(postgres_restore))
            tmp_database = create_db(postgres_host,
                      postgres_restore,
                      postgres_port,
                      postgres_user,
                      postgres_password)
            logger.info("Created temp database for restore : {}".format(tmp_database))
            logger.info("Restore starting")
            result = restore_postgres_db(postgres_host,
                                         postgres_restore,
                                         postgres_port,
                                         postgres_user,
                                         postgres_password,
                                         restore_uncompressed,
                                         args.verbose)
            for line in result.splitlines():
                logger.info(line)
            logger.info("Restore complete")
            if args.dest_db is not None:
                restored_db_name = args.dest_db
                logger.info("Switching restored database with new one : {} > {}".format(
                    postgres_restore, restored_db_name
                ))
                swap_restore_new(postgres_host,
                                    postgres_restore,
                                    restored_db_name,
                                    postgres_port,
                                    postgres_user,
                                    postgres_password)
            else:
                restored_db_name = postgres_db
                logger.info("Switching restored database with active one : {} > {}".format(
                    postgres_restore, restored_db_name
                ))
                swap_restore_active(postgres_host,
                                    postgres_restore,
                                    restored_db_name,
                                    postgres_port,
                                    postgres_user,
                                    postgres_password)
            logger.info("Database restored and active.")
    else:
        logger.warn("No valid argument was given.")
        logger.warn(args)


if __name__ == '__main__':
    main()
