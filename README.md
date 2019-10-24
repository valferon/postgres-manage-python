# postgres_manage_python

Utility to backup, restore and list Postgresql databases from/to AWS S3 (or local storage) using python

## Getting Started

### Setup
* Create and activate virtualenv

      virtualenv -p python3 venv
      source venv/bin/activate

* Install dependencies

      pip3 install -r requirements.txt

* Create configuration file (ie. sample.config)

      [setup]
      # define if LOCAL or S3 storage will be used when storing/restoring the backup
      storage_engine='S3'

      [S3]
      bucket_name="db_backups.s3.my.domain.com"  # S3 bucket name (no need for s3:// prefix)
      bucket_backup_path="postgres/"  # PATH in the bucket to store your backups

      [local_storage]
      path=./backups/

      [postgresql]
      host=<your_psql_addr(probably 127.0.0.1)>
      port=<your_psql_port(probably 5432)>
      db=<your_db_name>
      user=<your_username>
      password=<your_password>

### Usage

* List databases on a postgresql server

      python3 manage_postgres_db.py --configfile sample.config --action list_dbs --verbose true

* Create database backup and store it (based on config file details)

      python3 manage_postgres_db.py --configfile sample.config --action backup --verbose true

* List previously created database backups available on storage engine

      python3 manage_postgres_db.py --configfile sample.config --action list --verbose true

* Restore previously created database backups available on storage engine (check available dates with *list* action)

      python3 manage_postgres_db.py --configfile sample.config --action restore --date "YYYY-MM-dd" --verbose true

* Restore previously created database backups into a new destination database

      python3 manage_postgres_db.py --configfile sample.config --action restore --date "YYYY-MM-dd" --dest-db new_DB_name

## Authors

* **Val Feron** - *Initial work* - [github](https://github.com/valferon)


## LicenseMIT License

Copyright (c) valferon

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.