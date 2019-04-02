#!/usr/bin/env python3

import csv
import gzip
import io
import argparse


def gen_data(pathbase, files=1, cols=1, rows=1, gz=True):
    for fi in range(files):
        with gzip.open(f'{pathbase}.{fi}.csv.gz', "w") if gz else open(f'{pathbase}.{fi}.csv', "w") as f:
            w = csv.writer(io.TextIOWrapper(f) if gz else f)
            cc = list(range(cols))
            w.writerow(['row'] + [f'col{i}' for i in cc])
            for j in range(rows):
                w.writerow([fi*rows + j] + cc)


# noinspection SqlDialectInspection
def gen_ddl(pathbase, table_name, cols=1, gz=True):
    cols_def = '\n'.join((f'col{i} INTEGER,' for i in range(cols)))[:-1]
    table_ddl = f"""CREATE TABLE {table_name} (
row INTEGER,
{cols_def}
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
        gen_ddl(args.pathbase, args.table, cols=args.cols)

    # 100000000 ~ 236 MB gz ~ 1.4 GB csv
    gen_data(args.pathbase, files=args.files, cols=args.cols,
             rows=args.rows, gz=args.gz)
