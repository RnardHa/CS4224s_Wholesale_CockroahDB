#!/usr/bin/env python3
import time
import random
import logging

# I,W ID,D ID,L.

debug = False


def get_popular_item(conn, data):
    # data
    w_id = data[0]
    d_id = data[1]
    l = data[2]

    print("-----Popular Items-----")
    logging.info("-----Popular Items-----")
    # print("data {} {} {}".format(w_id, d_id, l))
    if debug:
        print("District Indentifier: {}, {}".format(w_id, d_id))
        print("Number of orders to be examined: {}".format(l))
        print()

    logging.info("[D_ID, Num_Order]")
    logging.info("{} {}, {}".format(w_id, d_id, l))

    d_next_o_id = get_district(conn, w_id, d_id)
    # print(get_d_next_o_id)

    # get o_id of l orders
    get_o_id = int(d_next_o_id) - 1 - int(l)

    # list of orders
    items = {}
    orders = get_orders(conn, w_id, d_id, get_o_id, d_next_o_id)
    for order in orders:
        o_id = order[0]
        c_id = order[1]
        o_entry_d = order[2]
        customer = get_customer(conn, w_id, d_id, c_id)
        if debug:
            print("*****Orders Info*****")
            print("Order ID: {}".format(o_id))
            print("Order Entry Date: {}".format(o_entry_d))
            print("Name: {} {} {}".format(
                customer[0], customer[1], customer[2]))
            print()

        logging.info("*****Orders Info*****")
        logging.info("[O_ID, O_Entry_D, Name]")
        logging.info("{}, {}, {} {} {}".format(
            o_id, o_entry_d, customer[0], customer[1], customer[2]))

        orderLines = get_max_orderlines(conn, w_id, d_id, o_id)
        for orderLine in orderLines:
            i_id = orderLine[0]
            item = get_item(conn, i_id)
            i_name = item[0]
            itemCount = items.get(i_name) if i_name in items else 0
            items.update({i_name: itemCount + 1})
            if debug:
                print("Item Name: {}".format(i_name))
                print("Quantity: {}".format(orderLine[1]))
                print()

            logging.info("[I_Name, Quantity]")
            logging.info("{}, {}".format(i_name, orderLine[1]))

    setItems = set(items.keys())
    if debug:
        print("*****Percentage of Orders*****")
    logging.info("*****Percentage of Orders*****")
    for item in setItems:
        counter = items.get(item)
        percent = float(counter) / (float(l) * 100.0)

        if debug:
            print("Item Name: {}".format(item))
            print("Percentage of Orders : {}".format(percent))
            print()
        logging.info("[I_Name, Percentage of Order]")
        logging.info("{}, {}".format(item, percent))


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


def get_orders(conn, warehouse_id, district_id, order_id, district_next_o_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select o_id, o_c_id, o_entry_d from orders where o_w_id = %s and o_d_id = %s and o_id > %s and o_id < %s", [warehouse_id, district_id, order_id, district_next_o_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows


def get_customer(conn, warehouse_id, district_id, customer_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select c_first, c_middle, c_last, c_balance from customer where c_w_id = %s and c_d_id = %s and c_id = %s", [warehouse_id, district_id, customer_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        for row in rows:
            cust = row

        return cust


def get_max_orderlines(conn, warehouse_id, district_id, order_id):
    ol_quantity = get_max_quantity(conn, warehouse_id, district_id, order_id)

    with conn.cursor() as cur:
        cur.execute(
            "Select ol_i_id, ol_quantity from orderline where ol_w_id = %s and ol_d_id = %s and ol_o_id = %s and ol_quantity <= %s", [warehouse_id, district_id, order_id, ol_quantity])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows


def get_max_quantity(conn, warehouse_id, district_id, order_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select ol_quantity from orderline where ol_w_id = %s and ol_d_id = %s and ol_o_id = %s order by ol_quantity desc limit 1", [warehouse_id, district_id, order_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows[0]


def get_item(conn, item_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select i_name from item where i_id = %s", [item_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows[0]
