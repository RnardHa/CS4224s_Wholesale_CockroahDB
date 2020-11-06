#!/usr/bin/env python3
import time
import random
import logging
import datetime

# O,C W ID,C D ID,C ID.

debug = False


def req_order_status(conn, c_w_id, c_d_id, c_id):
    start = time.time()
    # print("-----Order Status-----")
    logging.info("-----Order Status-----")

    # customer information
    customer = get_customer(conn, c_w_id, c_d_id, c_id)
    if debug:
        print(f"Customer at {time.asctime()}:")
        for row in customer:
            print("Name: {} {} {}".format(row[0], row[1], row[2]))
            print("Balance: {}".format(row[3]))
            print()
    for row in customer:
        logging.info("[Name, Balance]")
        logging.info("{} {} {}, {}".format(row[0], row[1], row[2], row[3]))

    # customer last order
    order = get_last_order(conn, c_w_id, c_d_id, c_id)
    if debug:
        print(f"Customer's last order at {time.asctime()}:")
    for row in order:
        o_id = row[0]
        if debug:
            print("Order ID: {}".format(o_id))
            print("Entry Date and Time: {}".format(row[1]))
            print("Carrier Identifier: {}".format(row[2]))
            print()
        logging.info("[O_ID, Entry Date, Carrier_ID]")
        logging.info("{}, {}, {}".format(o_id, row[1], row[2]))

    # for each item in customer last order, display
    orderList = get_order_list(conn, c_w_id, c_d_id, o_id)
    if debug:
        print(f"Items {time.asctime()}:")
        for row in orderList:
            print("Item Number: {}".format(row[0]))
            print("Supply Warehouse Number: {}".format(row[1]))
            print("Quantity Ordered: {}".format(row[2]))
            print("Total Price: {}".format(row[3]))
            print("Data and Time of Delivery: {}".format(row[4]))
            print("--------------------------------------------------")
            print()

    for row in orderList:
        logging.info(
            "[I_Num, Sup_W, Quantity, Total_Price, Date and Time Delivery]")
        logging.info("{}, {}, {}, {}, {}".format(
            row[0], row[1], row[2], row[3], row[4]))

    end = time.time()
    return end - start


def get_customer(conn, warehouse_id, district_id, customer_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select c_first, c_middle, c_last, c_balance from customer where c_w_id = %s and c_d_id = %s and c_id = %s", [warehouse_id, district_id, customer_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows


def get_last_order(conn, warehouse_id, district_id, customer_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select o_id, o_entry_d, o_carrier_id from orders where o_w_id = %s and o_d_id = %s and o_c_id = %s", [warehouse_id, district_id, customer_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows


def get_order_list(conn, warehouse_id, district_id, order_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from orderline where ol_w_id = %s and ol_d_id = %s and ol_o_id = %s", [warehouse_id, district_id, order_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows
