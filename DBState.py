#!/usr/bin/env python3

import time
import random
import logging
from argparse import ArgumentParser
import sys
import getopt
import time
import numpy as np
import csv

import psycopg2
from psycopg2.errors import SerializationFailure


def run_transaction(conn, op, max_retries=3):
    """
    Execute the operation *op(conn)* retrying serialization failure.

    If the database returns an error asking to retry the transaction, retry it
    *max_retries* times before giving up (and propagate it).
    """
    # leaving this block the transaction will commit or rollback
    # (if leaving with an exception)
    with conn:
        for retry in range(1, max_retries + 1):
            try:
                op(conn)

                # If we reach this point, we were able to commit, so we break
                # from the retry loop.
                return

            except SerializationFailure as e:
                # This is a retry error, so we roll back the current
                # transaction and sleep for a bit before retrying. The
                # sleep time increases for each failed transaction.
                logging.debug("got error: %s", e)
                conn.rollback()
                logging.debug("EXECUTE SERIALIZATION_FAILURE BRANCH")
                sleep_ms = (2 ** retry) * 0.1 * (random.random() + 0.5)
                logging.debug("Sleeping %s seconds", sleep_ms)
                time.sleep(sleep_ms)

            except psycopg2.Error as e:
                logging.debug("got error: %s", e)
                logging.debug("EXECUTE NON-SERIALIZATION_FAILURE BRANCH")
                raise e

        raise ValueError(
            f"Transaction did not succeed after {max_retries} retries")


def test_retry_loop(conn):
    """
    Cause a seralization error in the connection.

    This function can be used to test retry logic.
    """
    with conn.cursor() as cur:
        # The first statement in a transaction can be retried transparently on
        # the server, so we need to add a dummy statement so that our
        # force_retry() statement isn't the first one.
        cur.execute("SELECT now()")
        cur.execute("SELECT crdb_internal.force_retry('1s'::INTERVAL)")
    logging.debug("test_retry_loop(): status message: %s", cur.statusmessage)


def main():
    opt = parse_cmdline()
    exp_num = opt.exp_num
    log_name = 'DBState-Log.log'
    logging.basicConfig(filename=log_name, filemode='a',
                        format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    conn = psycopg2.connect(opt.dsn)

    with conn.cursor() as cur:
        cur.execute(
            "Select sum(w_ytd) from warehouse")
        warehouse = cur.fetchall()

        # print(warehouse)

        cur.execute(
            "Select sum(d_ytd), sum(d_next_o_id) from district")
        district = cur.fetchall()

        # print(district)

        cur.execute(
            "Select sum(c_balance), sum(c_ytd_payment), sum(c_payment_cnt), sum(c_delivery_cnt) from customer")
        customer = cur.fetchall()

        # print(customer)

        cur.execute(
            "Select max(o_id), sum(o_ol_cnt) from orders")
        orders = cur.fetchall()

        # print(orders)

        cur.execute(
            "Select sum(ol_amount), sum(ol_quantity) from orderline")
        orderline = cur.fetchall()

        # print(orderline)

        cur.execute(
            "Select sum(s_quantity), sum(s_ytd), sum(s_order_cnt), sum(s_remote_cnt) from stock")
        stock = cur.fetchall()

        # print(stock)

        conn.commit()

    output_dbstate(exp_num, warehouse[0], district[0],
                   customer[0], orders[0], orderline[0], stock[0])


def output_dbstate(exp_num, warehouse, district, customer, orders, orderline, stock):
    with open('dbstate.csv', 'a+', newline='') as file:
        d_ytd = district[0]
        d_next_o_id = district[1]

        c_balance = customer[0]
        c_ytd_payment = customer[1]
        c_payment_cnt = customer[2]
        c_delivery_cnt = customer[3]

        o_id = orders[0]
        o_ol_cnt = orders[1]

        ol_amount = orderline[0]
        ol_quantity = orderline[1]

        s_quantity = stock[0]
        s_ytd = stock[1]
        s_order_cnt = stock[2]
        s_remote_cnt = stock[3]

        writer = csv.writer(file)
        writer.writerow([exp_num, warehouse[0], d_ytd, d_next_o_id, c_balance, c_ytd_payment, c_payment_cnt,
                         c_delivery_cnt, o_id, o_ol_cnt, ol_amount, ol_quantity, s_quantity, s_ytd, s_order_cnt, s_remote_cnt])


def parse_cmdline():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default="postgresql://root@192.168.48.194:25273/wholesale?sslmode=disable",
        help="database connection string [default: %(default)s]",
    )

    parser.add_argument("-v", "--verbose",
                        action="store_true", help="print debug info")

    parser.add_argument("-EN", "--experiment_number",
                        action="store", required=True, dest="exp_num")

    opt = parser.parse_args()

    return opt


if __name__ == "__main__":
    main()
