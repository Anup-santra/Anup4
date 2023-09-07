#!/usr/bin/env python3
#
# Brain Dump
#
# Requirements:
#   pip install aiofiles aiopg
#
# Usage:
#   python dump.py

import aiofiles
import aiopg
import argparse
import asyncio
import json
import sys
from datetime import datetime


class CrateJsonEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


async def fetch(dsn, schema, table):
    async with aiopg.connect(dsn) as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"SELECT * FROM {schema}.{table}")
            rs = await cur.fetchall()
            cols = [col.name for col in cur.description]

    async with aiofiles.open(f"{schema}.{table}.json", "w") as fp:
        for row in rs:
            await fp.write(json.dumps(dict(zip(cols, row)), cls=CrateJsonEncoder))
            await fp.write("\n")


async def dump(dsn, schema):
    async with aiopg.connect(dsn) as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema = %s
            """, (schema,))
            rs = await cur.fetchall()
            coros = [fetch(dsn, *row) for row in rs]
    await asyncio.gather(*coros)


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default="5432")
    parser.add_argument("--user", default="crate")
    parser.add_argument("--password", default=None)
    parser.add_argument("--dbname", default="core")
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    dsn = " ".join([f"{k}={v}" for k, v in args.__dict__.items() if v])
    loop = asyncio.get_event_loop()
    loop.run_until_complete(dump(dsn, args.dbname))


if __name__ == "__main__":
    main()