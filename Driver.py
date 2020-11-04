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

import PaymentTransaction
import NewOrderTransaction
import DeliveryTransaction
import OrderStatusTransaction
import StockLevelTransaction
import PopularItemTransaction
import TopBalanceTransaction
import RelatedCustomerTransaction

debug = True


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

    # EXP num - cleint pair
    exp = {'5': '20', '6': '20', '7': '40', '8': '40'}
    exp_num = opt.exp_num
    numClient = exp[opt.exp_num]
    fileNum = 1
    transactionTimeCol = []
    while(fileNum <= int(numClient)):
        # When new file(client) is read, load balancer will select different node to run the queries.
        log_name = 'Transaction-Log.log'
        logging.basicConfig(filename=log_name, filemode='a',
                            format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

        conn = psycopg2.connect(opt.dsn)

        fileName = "project-files/xact-files/" + str(fileNum) + '.txt'
        print(fileName)
        readFile = open(fileName, 'r')
        lines = readFile.readlines()
        # store if code is N
        xList = []
        cList = []
        for i in range(0, len(lines)):
            start = time.time()
            line = lines[i].strip()
            split = line.split(',')

            if(len(split) > 0):
                xactType = split[0]

            if(xactType == 'N'):
                nLen = split[4]
                # first = split[1] + "," + split[2] + "," + split[3] + "," + nLen
                for index in range(1, 5):
                    cList.append(split[index])

                while(int(nLen) > 0):
                    i = i + 1
                    xList.append(lines[i])
                    # print(nLen)
                    nLen = int(nLen) - 1

                # run N transaction
                try:
                    run_transaction(
                        conn, lambda conn: NewOrderTransaction.make_new_order(conn, cList, xList))
                except ValueError as ve:
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass

                cList.clear()
                xList.clear()
                end = time.time()
            elif(xactType == 'P'):
                C_W_ID = int(split[1])
                C_D_ID = int(split[2])
                C_ID = int(split[3])
                PAYMENT = float(split[4])

                try:
                    run_transaction(
                        conn, lambda conn: PaymentTransaction.make_payment(conn, C_W_ID, C_D_ID, C_ID, PAYMENT))
                except ValueError as ve:
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass

                end = time.time()
            elif(xactType == 'D'):
                W_ID = int(split[1])
                CARRIER_ID = int(split[2])

                try:
                    run_transaction(
                        conn, lambda conn: DeliveryTransaction.make_delivery(conn, W_ID, CARRIER_ID))
                except ValueError as ve:
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass

                end = time.time()
            elif(xactType == 'O'):
                C_W_ID = int(split[1])
                C_D_ID = int(split[2])
                C_ID = int(split[3])

                try:
                    run_transaction(
                        conn, lambda conn: OrderStatusTransaction.req_order_status(conn, C_W_ID, C_D_ID, C_ID))
                except ValueError as ve:
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass

                end = time.time()
            elif(xactType == 'S'):
                W_ID = int(split[1])
                D_ID = int(split[2])
                T = int(split[3])
                L = int(split[4])

                try:
                    run_transaction(
                        conn, lambda conn: StockLevelTransaction.get_stock_level(conn,  W_ID, D_ID, T, L))
                except ValueError as ve:
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass

                end = time.time()
            elif(xactType == 'I'):
                W_ID = int(split[1])
                D_ID = int(split[2])
                L = int(split[3])

                try:
                    run_transaction(
                        conn, lambda conn: PopularItemTransaction.get_popular_item(conn, W_ID, D_ID, L))
                except ValueError as ve:
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass

                end = time.time()
            elif(xactType == 'T'):
                # print("T")
                try:
                    run_transaction(
                        conn, lambda conn: TopBalanceTransaction.get_top_balance(conn))
                except ValueError as ve:
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass

                end = time.time()
            elif(xactType == 'R'):
                C_W_ID = int(split[1])
                C_D_ID = int(split[2])
                C_ID = int(split[3])

                try:
                    run_transaction(
                        conn, lambda conn: RelatedCustomerTransaction.get_related_customer(conn,  C_W_ID, C_D_ID, C_ID))
                except ValueError as ve:
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass

                end = time.time()

            transactionTimeCol.append(end-start)

        output_transactions_stats(transactionTimeCol, exp_num, fileNum)
        # Load next file
        fileNum = fileNum + 1

    # Close communication with the database.
    conn.close()
    # print("Minutes taken: {:.2f}".format(end-start))
    # output_transactions_stats(transactionTimeCol, exp_num, numClient)


def output_transactions_stats(transactionTime, exp_num, clientNum):
    with open('clients.csv', 'a+', newline='') as file:
        transactionTime = [i for i in transactionTime if i > 0.0]
        num_xacts = len(transactionTime)
        totalSec = sum(transactionTime)  # second
        throughput = num_xacts / totalSec
        toMs = totalSec * 1000
        average = toMs / num_xacts
        median = np.percentile(transactionTime, 50)
        percentile95 = np.percentile(transactionTime, 95)
        percentile99 = np.percentile(transactionTime, 99)
        if debug:
            print("Number of Transactions: {}".format(
                num_xacts), file=sys.stderr)
            print("Total Transactions Time: {} sec".format(
                totalSec), file=sys.stderr)
            print("Throughput: {} xacts/sec".format(throughput), file=sys.stderr)
            print("Average Transaction Latency: {} ms".format(
                average), file=sys.stderr)
            print("Median Transaction Latency: {} ms".format(
                median), file=sys.stderr)
            print("95th Percentile Transaction Latrency: {} ms".format(
                percentile95), file=sys.stderr)
            print("99th Percentile Transaction Latrency: {} ms".format(
                percentile99), file=sys.stderr)
            print()
        writer = csv.writer(file)
        writer.writerow([exp_num, clientNum, num_xacts, totalSec,
                         throughput, average, median, percentile95, percentile99])


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
