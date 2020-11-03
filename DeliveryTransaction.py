#!/usr/bin/env python3
import time
import random
import logging
import datetime

# D,W ID,CARRIER ID.


def make_delivery(conn, w_id, carrier_id):
    # w_id = W_ID
    # carrier_id = CARRIER_ID
    # print(w_id + " " + carrier_id)
    logging.info("-----Delivery-----")
    print("-----Delivery-----")
    order_list = get_order(conn, w_id)
    for o in order_list:
        d_id = o[0]
        o_id = o[1]
        c_id = o[2]

        update_carrier(conn, w_id, d_id, o_id, carrier_id)
        update_orderLine(conn, w_id, d_id, o_id)
        total_amount = get_sum_of_ol(conn, w_id, d_id, o_id)
        # print("Amount {}".format(total_amount))
        update_customer(conn, w_id, d_id, c_id, total_amount)

    # for d_id in range(1, 11):
    #     order = get_order(conn, w_id, d_id)
    #     if order is not "":
    #         o_id = order[0]
    #         c_id = order[1]
    #         # print(order)
    #         update_carrier(conn, w_id, d_id, o_id, carrier_id)
    #         update_orderLine(conn, w_id, d_id, o_id)
    #         total_amount = get_sum_of_ol(conn, w_id, d_id, o_id)
    #         # print("Amount {}".format(total_amount))
    #         update_customer(conn, w_id, d_id, c_id, total_amount)


def get_order(conn, warehouse_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select o_id from orders where o_w_id = 1 and o_d_id = 1 and o_carrier_id is NULL limit 1")
        rows = cur.fetchall()
        for row in rows:
            o_id = row[0]
        # print(o_id)
        cur.execute(
            "Select o_d_id, o_id, o_c_id from orders where o_id = %s and o_w_id = %s and o_d_id in (1,2,3,4,5,6,7,8,9,10) and o_carrier_id is NULL", [o_id, warehouse_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows


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
