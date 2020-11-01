#!/usr/bin/env python3
import time
import random
import logging

# S,W ID,D ID,T,L


def get_stock_level(conn, data):
    # data
    w_id = data[0]
    d_id = data[1]
    t = data[2]
    l = data[3]

    # print("Data {}, {}, {}, {}".format(w_id, d_id, t, l))
    print("-----Stock Level-----")

    district = get_district(conn, w_id, d_id)
    d_next_o_id = district

    # get o_id of l orders
    get_o_id = int(d_next_o_id) - 1 - int(l)

    orderLines = get_order_lines(conn, w_id, d_id, get_o_id, d_next_o_id)

    itemsSet = set()
    for item in orderLines:
        itemsSet.add(item)

    count = get_stock_count(conn, w_id, itemsSet, t)
    print("W_ID: {}".format(w_id))
    print("Number of items with S_Quantity < {}: {}".format(t, count))
    print()


def get_district(conn, warehouse_id, district_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select d_next_o_id from district where d_w_id = %s and d_id = %s", [warehouse_id, district_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        for row in rows:
            next_id = row[0]

        return next_id


def get_order_lines(conn, warehouse_id, district_id, order_id, district_next_o_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select ol_i_id, ol_o_id from orderline where ol_w_id = %s and ol_d_id = %s and ol_o_id > %s and ol_o_id < %s", [warehouse_id, district_id, order_id, district_next_o_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows


def get_stock_count(conn, warehouse_id, items, treshold):
    with conn.cursor() as cur:
        counter = 0
        for i in items:
            cur.execute(
                "Select s_i_id from stock where s_w_id = %s and s_i_id = %s and s_quantity < %s", [warehouse_id, i[0], treshold])
            logging.debug("make payment(): status message: %s",
                          cur.statusmessage)
            rows = cur.fetchall()
            for row in rows:
                counter += 1
            conn.commit()

        return counter
