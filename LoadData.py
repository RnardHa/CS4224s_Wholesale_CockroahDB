#!/usr/bin/env python3

import time
import random
import logging
from argparse import ArgumentParser

import psycopg2
from psycopg2.errors import SerializationFailure

import csv


# delete warehouse
def delete_warehouse(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS warehouse")
        logging.debug("delete_warehouse(): status message: %s",
                      cur.statusmessage)
    conn.commit()

# delete district


def delete_district(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS district")
        logging.debug("delete_district(): status message: %s",
                      cur.statusmessage)
    conn.commit()

# delete customer


def delete_customer(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS customer")
        logging.debug("delete_customer(): status message: %s",
                      cur.statusmessage)
    conn.commit()

# delete order


def delete_order(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS orders")
        logging.debug("delete_order(): status message: %s",
                      cur.statusmessage)
    conn.commit()

# delete item


def delete_item(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS item")
        logging.debug("delete_item(): status message: %s",
                      cur.statusmessage)
    conn.commit()

# delete orderline


def delete_orderLine(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS orderLine")
        logging.debug("delete_orderLine(): status message: %s",
                      cur.statusmessage)
    conn.commit()

# delete stock


def delete_stock(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS stock")
        logging.debug("delete_stock(): status message: %s",
                      cur.statusmessage)
    conn.commit()

# create warehouse


def create_warehouse(conn):
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS warehouse (W_ID INT PRIMARY KEY, W_NAME VARCHAR(10), W_STREET_1 VARCHAR(20), W_STREET_2 VARCHAR(20), W_CITY VARCHAR(20), W_STATE CHAR(2), W_ZIP CHAR(9), W_TAX DECIMAL(4,4), W_YTD DECIMAL(12,2))"
        )

        with open('project-files/data-files/warehouse.csv', 'r') as f:
            f = csv.reader(f)
            for row in f:
                cur.execute(
                    "INSERT INTO warehouse VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)

        logging.debug("create_warehouse(): status message: %s",
                      cur.statusmessage)
    conn.commit()

# create district


def create_district(conn):
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS district (D_W_ID INT, D_ID INT, D_NAME VARCHAR(10), D_STREET_1 VARCHAR(20), D_STREET_2 VARCHAR(20), D_CITY VARCHAR(20), D_STATE CHAR(2), D_ZIP CHAR(9), D_TAX DECIMAL(4,4), D_YTD DECIMAL(12,2), D_NEXT_O_ID INT, PRIMARY KEY(D_W_ID, D_ID), CONSTRAINT fk_warehouse FOREIGN KEY(D_W_ID) REFERENCES warehouse(W_ID))"
        )
    conn.commit()

    with conn.cursor() as cur:
        with open('project-files/data-files/district.csv', 'r') as f:
            f = csv.reader(f)
            for row in f:
                cur.execute(
                    "UPSERT INTO district VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)

        logging.debug("create_district(): status message: %s",
                      cur.statusmessage)
    conn.commit()

# create customer


def create_customer(conn):
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS customer (C_W_ID INT, C_D_ID INT, C_ID INT, C_FIRST VARCHAR(16), C_MIDDLE CHAR(2), C_LAST VARCHAR(16), C_STREET_1 VARCHAR(20), C_STREET_2 VARCHAR(20), C_CITY VARCHAR(20), C_STATE CHAR(2), C_ZIP CHAR(9), C_PHONE CHAR(16), C_SINCE TIMESTAMP, C_CREDIT CHAR(2), C_CREDIT_LIM DECIMAL(12,2), C_DISCOUNT DECIMAL(4,4), C_BALANCE DECIMAL(12,2), C_YTD_PAYMENT FLOAT, C_PAYMENT_CNT INT, C_DELIVERY_CNT INT, C_DATA VARCHAR(500), PRIMARY KEY(C_W_ID, C_D_ID, C_ID), CONSTRAINT fk_district FOREIGN KEY (C_W_ID, C_D_ID) REFERENCES district(D_W_ID, D_ID))"
        )
    conn.commit()

    with conn.cursor() as cur:
        with open('project-files/data-files/customer.csv', 'r') as f:
            f = csv.reader(f)
            count = 0
            for row in f:
                count = count + 1
                if(count % 1000 == 0):
                    print(count)
                    print("c")
                cur.execute(
                    "INSERT INTO customer VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)

        logging.debug("create_customer(): status message: %s",
                      cur.statusmessage)
    conn.commit()

# create order


def create_order(conn):
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS orders (O_W_ID INT, O_D_ID INT, O_ID INT, O_C_ID INT, O_CARRRIER_ID INT, O_OL_CNT DECIMAL(2,0), O_ALL_LOCAL DECIMAL(1,0), O_ENTRY_D TIMESTAMP, PRIMARY KEY(O_W_ID, O_D_ID, O_ID), CONSTRAINT fk_customer FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) REFERENCES customer(C_W_ID, C_D_ID, C_ID))"
        )
    conn.commit()

    with conn.cursor() as cur:
        with open('project-files/data-files/order.csv', 'r') as f:
            f = csv.reader(f)
            count = 0
            for row in f:
                count = count + 1
                if(count % 1000 == 0):
                    print(count)
                    print("o")
                cur.execute(
                    "INSERT INTO orders VALUES (%s, %s, %s, %s, NULLIF(%s,'null')::integer, %s, %s, %s)", row)

        logging.debug("create_orders(): status message: %s",
                      cur.statusmessage)
    conn.commit()

# create item


def create_item(conn):
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS item (I_ID INT PRIMARY KEY, I_NAME VARCHAR(24), I_PRICE DECIMAL(5,2), I_IM_ID INT, I_DATA VARCHAR(50))"
        )
    conn.commit()

    with conn.cursor() as cur:
        with open('project-files/data-files/item.csv', 'r') as f:
            f = csv.reader(f)
            count = 0
            for row in f:
                count = count + 1
                if(count % 1000 == 0):
                    print(count)
                    print("i")
                cur.execute(
                    "INSERT INTO item VALUES (%s, %s, %s, %s, %s)", row)
        logging.debug("create_item(): status message: %s",
                      cur.statusmessage)
    conn.commit()

# create orderline


def create_orderLine(conn):
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS orderLine (OL_W_ID INT, OL_D_ID INT, OL_O_ID INT, OL_NUMBER INT, OL_I_ID INT, OL_DELIVERY_D TIMESTAMP, OL_AMOUNT DECIMAL(6,2), OL_SUPPLY_W_ID INT, OL_QUANTITY DECIMAL(2,0), OL_DIST_INFO CHAR(24), PRIMARY KEY(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER), CONSTRAINT fk_order FOREIGN KEY(OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES orders(O_W_ID, O_D_ID, O_ID), CONSTRAINT fk_item FOREIGN KEY(OL_I_ID) REFERENCES item(I_ID))"
        )
    conn.commit()

    with conn.cursor() as cur:
        with open('project-files/data-files/order-line.csv', 'r') as f:
            f = csv.reader(f)
            count = 0
            for row in f:
                count = count + 1
                if(count % 1000 == 0):
                    print(count)
                    print("ol")
                cur.execute(
                    "INSERT INTO orderLine VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
        logging.debug("create_orderLine(): status message: %s",
                      cur.statusmessage)
    conn.commit()

# create stock


def create_stock(conn):
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS stock (S_W_ID INT, S_I_ID INT, S_QUANTITY DECIMAL(4,0), S_YTD DECIMAL(8,2), S_ORDER_CNT INT, S_REMOTE_CNT INT, S_DIST_01 CHAR(24), S_DIST_02 CHAR(24), S_DIST_03 CHAR(24), S_DIST_04 CHAR(24), S_DIST_05 CHAR(24), S_DIST_06 CHAR(24), S_DIST_07 CHAR(24), S_DIST_08 CHAR(24), S_DIST_09 CHAR(24), S_DIST_10 CHAR(24), S_DATA VARCHAR(50), PRIMARY KEY(S_W_ID, S_I_ID), CONSTRAINT fk_warehouse FOREIGN KEY(S_W_ID) REFERENCES warehouse(W_ID), CONSTRAINT fk_item FOREIGN KEY(S_I_ID) REFERENCES item(I_ID))"
        )
    conn.commit()

    with conn.cursor() as cur:
        with open('project-files/data-files/stock.csv', 'r') as f:
            f = csv.reader(f)
            count = 0
            for row in f:
                count = count + 1
                if(count % 1000 == 0):
                    print(count)
                    print("s")
                cur.execute(
                    "INSERT INTO stock VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)

        logging.debug("create_stock(): status message: %s",
                      cur.statusmessage)
    conn.commit()


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

    # delete_stock(conn)
    # delete_orderLine(conn)
    # delete_item(conn)
    # delete_order(conn)
    # delete_customer(conn)
    # delete_district(conn)
    # delete_warehouse(conn)

    # create_warehouse(conn)
    # create_district(conn)
    create_customer(conn)
    # create_order(conn)
    # create_item(conn)
    # create_orderLine(conn)
    # create_stock(conn)

    # try:
    #     run_transaction(conn, lambda conn: transfer_funds(
    #         conn, fromId, toId, amount))

    #     # The function below is used to test the transaction retry logic.  It
    #     # can be deleted from production code.
    #     # run_transaction(conn, test_retry_loop)
    # except ValueError as ve:
    #     # Below, we print the error and continue on so this example is easy to
    #     # run (and run, and run...).  In real code you should handle this error
    #     # and any others thrown by the database interaction.
    #     logging.debug("run_transaction(conn, op) failed: %s", ve)
    #     pass

    # Close communication with the database.
    conn.close()


def parse_cmdline():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        # default="postgresql://root@localhost:26257/test?sslmode=disable",
        default="postgresql://root@192.168.48.194:26267/wholesale?sslmode=disable",
        help="database connection string [default: %(default)s]",
    )

    parser.add_argument("-v", "--verbose",
                        action="store_true", help="print debug info")

    opt = parser.parse_args()
    return opt


if __name__ == "__main__":
    main()
