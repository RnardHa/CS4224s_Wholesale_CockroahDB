#!/usr/bin/env python3

import time
import random
import logging
from argparse import ArgumentParser
import sys
import getopt
import time

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
    logging.basicConfig(level=logging.DEBUG if opt.verbose else logging.INFO)

    conn = psycopg2.connect(opt.dsn)

    # read input data
    # numOfServer = input("Number of running server: ")
    numClient = input("Number of Client: ")
    fileNum = 1
    start = time.time()
    while(fileNum <= int(numClient)):
        fileName = str(fileNum) + '.txt'
        print(fileName)
        readFile = open(fileName, 'r')
        lines = readFile.readlines()
        # store if code is N
        xList = []
        cList = []
        for i in range(0, len(lines)):
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

                    # The function below is used to test the transaction retry logic.  It
                    # can be deleted from production code.
                    # run_transaction(conn, test_retry_loop)
                except ValueError as ve:
                    # Below, we print the error and continue on so this example is easy to
                    # run (and run, and run...).  In real code you should handle this error
                    # and any others thrown by the database interaction.
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass

                cList.clear()
                xList.clear()
            elif(xactType == 'P'):
                for i in range(1, 5):
                    xList.append(split[i])

                try:
                    run_transaction(
                        conn, lambda conn: PaymentTransaction.make_payment(conn, xList))

                    # The function below is used to test the transaction retry logic.  It
                    # can be deleted from production code.
                    # run_transaction(conn, test_retry_loop)
                except ValueError as ve:
                    # Below, we print the error and continue on so this example is easy to
                    # run (and run, and run...).  In real code you should handle this error
                    # and any others thrown by the database interaction.
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass
                xList.clear()
            elif(xactType == 'D'):
                for i in range(1, 3):
                    xList.append(split[i])

                try:
                    run_transaction(
                        conn, lambda conn: DeliveryTransaction.make_delivery(conn, xList))

                    # The function below is used to test the transaction retry logic.  It
                    # can be deleted from production code.
                    # run_transaction(conn, test_retry_loop)
                except ValueError as ve:
                    # Below, we print the error and continue on so this example is easy to
                    # run (and run, and run...).  In real code you should handle this error
                    # and any others thrown by the database interaction.
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass
                xList.clear()
            elif(xactType == 'O'):
                # print("O")
                for i in range(1, 4):
                    xList.append(split[i])

                try:
                    run_transaction(
                        conn, lambda conn: OrderStatusTransaction.req_order_status(conn, xList))

                    # The function below is used to test the transaction retry logic.  It
                    # can be deleted from production code.
                    # run_transaction(conn, test_retry_loop)
                except ValueError as ve:
                    # Below, we print the error and continue on so this example is easy to
                    # run (and run, and run...).  In real code you should handle this error
                    # and any others thrown by the database interaction.
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass
                xList.clear()
            elif(xactType == 'S'):
                for i in range(1, 5):
                    xList.append(split[i])

                try:
                    run_transaction(
                        conn, lambda conn: StockLevelTransaction.get_stock_level(conn, xList))

                    # The function below is used to test the transaction retry logic.  It
                    # can be deleted from production code.
                    # run_transaction(conn, test_retry_loop)
                except ValueError as ve:
                    # Below, we print the error and continue on so this example is easy to
                    # run (and run, and run...).  In real code you should handle this error
                    # and any others thrown by the database interaction.
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass
                xList.clear()
            elif(xactType == 'I'):
                for i in range(1, 4):
                    xList.append(split[i])

                try:
                    run_transaction(
                        conn, lambda conn: PopularItemTransaction.get_popular_item(conn, xList))

                    # The function below is used to test the transaction retry logic.  It
                    # can be deleted from production code.
                    # run_transaction(conn, test_retry_loop)
                except ValueError as ve:
                    # Below, we print the error and continue on so this example is easy to
                    # run (and run, and run...).  In real code you should handle this error
                    # and any others thrown by the database interaction.
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass
                xList.clear()
            elif(xactType == 'T'):
                print("T")
                try:
                    run_transaction(
                        conn, lambda conn: TopBalanceTransaction.get_top_balance(conn))

                    # The function below is used to test the transaction retry logic.  It
                    # can be deleted from production code.
                    # run_transaction(conn, test_retry_loop)
                except ValueError as ve:
                    # Below, we print the error and continue on so this example is easy to
                    # run (and run, and run...).  In real code you should handle this error
                    # and any others thrown by the database interaction.
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass
            elif(xactType == 'R'):
                for i in range(1, 4):
                    xList.append(split[i])

                try:
                    run_transaction(
                        conn, lambda conn: RelatedCustomerTransaction.get_related_customer(conn, xList))

                    # The function below is used to test the transaction retry logic.  It
                    # can be deleted from production code.
                    # run_transaction(conn, test_retry_loop)
                except ValueError as ve:
                    # Below, we print the error and continue on so this example is easy to
                    # run (and run, and run...).  In real code you should handle this error
                    # and any others thrown by the database interaction.
                    logging.debug("run_transaction(conn, op) failed: %s", ve)
                    pass
                xList.clear()

                # Load next file
        fileNum = fileNum + 1

    # Close communication with the database.
    conn.close()
    end = time.time()
    print(end-start)


def parse_cmdline():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default="postgresql://root@192.168.48.194:25273/wholesale?sslmode=disable",
        help="database connection string [default: %(default)s]",
    )

    parser.add_argument("-v", "--verbose",
                        action="store_true", help="print debug info")

    opt = parser.parse_args()
    return opt


if __name__ == "__main__":
    main()
