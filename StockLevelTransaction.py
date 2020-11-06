#!/usr/bin/env python3
import time
import random
import logging

# S,W ID,D ID,T,L

debug = False


def get_stock_level(conn, w_id, d_id, t, l):
    start = time.time()
    # print("Data {}, {}, {}, {}".format(w_id, d_id, t, l))
    # print("-----Stock Level-----")
    logging.info("-----Stock Level-----")

    district = get_district(conn, w_id, d_id)
    d_next_o_id = district

    # get o_id of l orders
    get_o_id = int(d_next_o_id) - 1 - int(l)

    orderLines = get_order_lines(conn, w_id, d_id, get_o_id, d_next_o_id)

    itemsSet = set()
    for item in orderLines:
        itemsSet.add(item[0])

    count = get_stock_count(conn, w_id, itemsSet, t)
    if debug:
        print("W_ID: {}".format(w_id))
        print("Number of items with S_Quantity < {}: {}".format(t, count[0]))
        print()

    logging.info("[W_ID, Num_I with S_Quantity < {}]".format(t))
    logging.info("{}, {}".format(w_id, count[0]))

    end = time.time()
    return end - start


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


def get_stock_count(conn, warehouse_id, itemList, treshold):
    item = tuple(itemList)
    with conn.cursor() as cur:
        cur.execute(
            "Select count(s_i_id) from stock where s_w_id = %s and s_i_id in %s and s_quantity < %s", [warehouse_id, item, treshold])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        for row in rows:
            res = row

        return res
