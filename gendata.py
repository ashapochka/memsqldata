#!/usr/bin/env python3

import csv
import gzip
import io
import argparse
from datetime import datetime
import tempfile
import os
import shutil

import numpy as np


def gen_data(pathbase, files=1, cols=1, rows=1, gz=True):
    print(f'starting data generation for {files} files...')
    cc = list(range(cols))
    header = ['row'] + [f'col{i}' for i in cc]
    for fi in range(files):
        n1 = datetime.now()
        with tempfile.TemporaryDirectory() as tmpdir:
            ext = f'{fi}.csv.gz' if gz else f'{fi}.csv'
            fpath = f'{pathbase}.{ext}'
            tpath = os.path.join(tmpdir, ext)
            with gzip.open(tpath, "w") if gz else open(tpath, "w") as f:
                fo = io.TextIOWrapper(f) if gz else f
                w = csv.writer(fo)
                w.writerow(header)
                w.writerows([fi*rows + j] + cc for j in range(rows))
                if gz:
                    fo.flush()
            shutil.move(tpath, fpath)
        n2 = datetime.now()
        secs = (n2 - n1).total_seconds()
        print(f'{fpath} completed in {secs} secs')


def gen_data_np(pathbase, files=1, cols=1, rows=1, gz=True):
    header = 'row,' + ','.join(f'col{i}' for i in range(cols))
    cc = list(range(cols))
    header = ['row'] + [f'col{i}' for i in cc]
    with tempfile.TemporaryDirectory() as tmpdir:
        for fi in range(files):
            n1 = datetime.now()
            ext = f'{fi}.csv.gz' if gz else f'{fi}.csv'
            fpath = f'{pathbase}.{ext}'
            tpath = os.path.join(tmpdir, ext)
            index = np.arange(fi * rows, (fi + 1) * rows)
            data = np.random.randint(0, 10, size=(rows, cols))
            table = np.c_[index, data]
            with gzip.open(tpath, "w") if gz else open(tpath, "w") as f:
                fo = io.TextIOWrapper(f) if gz else f
                w = csv.writer(fo)
                w.writerow(header)
                # print(table.tolist()[:3])
                w.writerows(table.tolist())
                if gz:
                    fo.flush()
            shutil.move(tpath, fpath)
            # np.savetxt(tpath, table,
            #            delimiter=',', header=header,
            #            comments='', fmt='%d')
            # shutil.move(tpath, fpath)
            n2 = datetime.now()
            secs = (n2 - n1).total_seconds()
            print(f'{fpath} completed in {secs} secs')


# noinspection SqlDialectInspection
def gen_ddl(pathbase, table_name, cols=1, gz=True):
    cols_def = '\n'.join((f'col{i} INTEGER,' for i in range(cols)))
    cols_key = ', '.join((f'col{i}' for i in range(cols)))
    table_ddl = f"""CREATE TABLE {table_name} (
row INTEGER,
{cols_def}
KEY ({cols_key}) USING CLUSTERED COLUMNSTORE,
SHARD KEY (row)
);"""
    with open(f'{pathbase}.table.sql', 'w') as f:
        f.write(table_ddl)

    ext = 'csv.gz' if gz else 'csv'
    pipe_ddl = f"""CREATE PIPELINE pipe_{table_name}
AS LOAD DATA FS '{pathbase}.*.{ext}'
INTO TABLE {table_name}
FIELDS TERMINATED BY ',';

START PIPELINE pipe_{table_name};"""
    with open(f'{pathbase}.pipe.sql', 'w') as f:
        f.write(pipe_ddl)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("pathbase",
                        help='test data file path base e.g. data/test',
                        type=str)
    parser.add_argument("-f", "--files", help="number of files to generate",
                        type=int, default=1)
    parser.add_argument("-c", "--cols",
                        help="number of columns to generate in a file (in "
                             "addition to the 'row' column)",
                        type=int, default=1)
    parser.add_argument("-r", "--rows",
                        help="number of rows to generate in a file",
                        type=int, default=1)
    parser.add_argument("-z", "--gz", help="generate compressed csv files",
                        action="store_true")
    parser.add_argument("-t", "--table", help="target table name",
                        type=str)
    args = parser.parse_args()
    if args.table:
        gen_ddl(args.pathbase, args.table, cols=args.cols, gz=args.gz)

    # 100000000 ~ 236 MB gz ~ 1.4 GB csv
    gen_data(args.pathbase, files=args.files, cols=args.cols,
             rows=args.rows, gz=args.gz)
