#!/usr/bin/env python3
import time
import random
import logging
import datetime

# D,W ID,CARRIER ID.


def make_delivery(conn, w_id, carrier_id):
    logging.info("-----Delivery-----")
    # print("-----Delivery-----")
    order_list = get_order(conn, w_id)
    if (len(order_list) > 0):
        get_first = order_list[0]  # Get the first item to extract the o_id
        o_id = get_first[1]
        update_carrier(conn, w_id, o_id, carrier_id)
        update_orderLine(conn, w_id, o_id)
        for o in order_list:
            d_id = o[0]
            c_id = o[2]

            total_amount = get_sum_of_ol(conn, w_id, d_id, o_id)
            # # print("Amount {}".format(total_amount))
            update_customer(conn, w_id, d_id, c_id, total_amount)

    return time.thread_time()


def get_order(conn, warehouse_id):
    val = get_o_id(conn, warehouse_id)
    if (len(val) > 0):
        o_id = val[0]
        with conn.cursor() as cur:
            cur.execute(
                "Select o_d_id, o_id, o_c_id from orders where o_id = %s and o_w_id = %s and o_d_id in (1,2,3,4,5,6,7,8,9,10) and o_carrier_id is NULL", [o_id[0], warehouse_id])
            logging.debug("make payment(): status message: %s",
                          cur.statusmessage)
            rows = cur.fetchall()
            conn.commit()

        return rows


def get_o_id(conn, warehouse_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select o_id from orders where o_w_id = %s and o_d_id in (1,2,3,4,5,6,7,8,9,10) and o_carrier_id is NULL limit 1", [warehouse_id])
        rows = cur.fetchall()
        conn.commit()

    return rows


def update_carrier(conn, warehouse_id, order_id, order_carrier_id):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE orders SET o_carrier_id = %s where o_w_id = %s and o_d_id in (1,2,3,4,5,6,7,8,9,10) and o_id = %s", [order_carrier_id, warehouse_id, order_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        conn.commit()


def update_orderLine(conn, warehouse_id, order_id):
    date = datetime.datetime.now()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE orderline SET ol_delivery_d = %s where ol_w_id = %s and ol_d_id in (1,2,3,4,5,6,7,8,9,10) and ol_o_id = %s", [date, warehouse_id, order_id])
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
