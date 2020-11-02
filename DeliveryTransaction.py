#!/usr/bin/env python3
import time
import random
import logging
import datetime

# D,W ID,CARRIER ID.


def make_delivery(conn, data):
    w_id = data[0]
    carrier_id = data[1]
    # print(w_id + " " + carrier_id)
    logging.info("-----Delivery-----")
    print("-----Delivery-----")

    for d_id in range(1, 11):
        order = get_order(conn, w_id, d_id)
        o_id = order[0]
        c_id = order[1]
        # print(order)
        update_carrier(conn, w_id, d_id, o_id, carrier_id)
        update_orderLine(conn, w_id, d_id, o_id)
        total_amount = get_sum_of_ol(conn, w_id, d_id, o_id)
        # print("Amount {}".format(total_amount))
        update_customer(conn, w_id, d_id, c_id, total_amount)


def get_order(conn, warehouse_id, district_id):
    with conn.cursor() as cur:
        # print("Warehouse {}, District {}".format(warehouse_id, district_id))
        cur.execute(
            "Select o_id, o_c_id from orders where o_w_id = %s and o_d_id = %s and o_carrier_id is NULL order by o_id limit 1", [warehouse_id, district_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        for row in rows:
            result = row

        return result


def update_carrier(conn, warehouse_id, district_id, order_id, order_carrier_id):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE orders SET o_carrier_id = %s where o_w_id = %s and o_d_id = %s and o_id = %s", [order_carrier_id, warehouse_id, district_id, order_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        conn.commit()


def update_orderLine(conn, warehouse_id, district_id, order_id):
    date = datetime.datetime.now()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE orderline SET ol_delivery_d = %s where ol_w_id = %s and ol_d_id = %s and ol_o_id = %s", [date, warehouse_id, district_id, order_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        conn.commit()


def get_sum_of_ol(conn, warehouse_id, district_id, order_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select sum(ol_amount) from orderline where ol_w_id = %s and ol_d_id = %s and ol_o_id = %s", [warehouse_id, district_id, order_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        for row in rows:
            total = row[0]

        return total


def update_customer(conn, warehouse_id, district_id, customer_id, amount):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE customer SET c_balance = c_balance + %s, c_delivery_cnt = c_delivery_cnt + 1 where c_w_id = %s and c_d_id = %s and c_id = %s", [amount, warehouse_id, district_id, customer_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        conn.commit()
