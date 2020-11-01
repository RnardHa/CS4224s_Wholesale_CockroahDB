#!/usr/bin/env python3
import time
import random
import logging
import datetime

# O,C W ID,C D ID,C ID.


def req_order_status(conn, data):
    # data
    c_w_id = data[0]
    c_d_id = data[1]
    c_id = data[2]

    # print("Data {}, {}, {}".format(c_w_id, c_d_id, c_id))
    print("-----Order Status-----")

    # customer information
    customer = get_customer(conn, c_w_id, c_d_id, c_id)
    print(f"Customer at {time.asctime()}:")
    for row in customer:
        print("Name: {} {} {}".format(row[0], row[1], row[2]))
        print("Balance: {}".format(row[3]))
        print()

    # customer last order
    order = get_last_order(conn, c_w_id, c_d_id, c_id)
    print(f"Customer's last order at {time.asctime()}:")
    for row in order:
        o_id = row[0]
        print("Order ID: {}".format(o_id))
        print("Entry Date and Time: {}".format(row[1]))
        print("Carrier Identifier: {}".format(row[2]))
        print()

    # for each item in customer last order, display
    orderList = get_order_list(conn, c_w_id, c_d_id, o_id)
    print(f"Items {time.asctime()}:")
    for row in orderList:
        print("Item Number: {}".format(row[0]))
        print("Supply Warehouse Number: {}".format(row[1]))
        print("Quantity Ordered: {}".format(row[2]))
        print("Total Price: {}".format(row[3]))
        print("Data and Time of Delivery: {}".format(row[4]))
        print("--------------------------------------------------")
        print()


def get_customer(conn, warehouse_id, district_id, customer_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select c_first, c_middle, c_last, c_balance from customer where c_w_id = %s and c_d_id = %s and c_id = %s", [warehouse_id, district_id, customer_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows

# database does not have enough data to test if user has multiple orders


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
