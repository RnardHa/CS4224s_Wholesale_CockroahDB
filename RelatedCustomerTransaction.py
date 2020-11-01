#!/usr/bin/env python3
import time
import random
import logging
import datetime

# R,C W ID,C D ID,C ID.


def get_related_customer(conn, data):
    # data
    c_w_id = data[0]
    c_d_id = data[1]
    c_id = data[2]

    print("-----Related Customer-----")
    print("Customer Indentifier: {} {} {}".format(c_w_id, c_d_id, c_id))
    print()

    relatedCust = set()
    orders = get_orders(conn, c_w_id, c_d_id, c_id)
    for order in orders:
        o_id = order[0]
        orderLines = get_orderlines(conn, c_w_id, c_d_id, o_id)
        items = []
        for orderLine in orderLines:
            items.append(orderLine[0])

        add_related_cust(conn, c_w_id, items, relatedCust)

        print("*****Customer*****")
        if(len(relatedCust) > 0):
            for cust in relatedCust:
                print("Customer Identifier: {}, {}, {}".format(
                    cust[0], cust[1], cust[2]))
                print()
        else:
            print("NIL")
            print()


def get_orders(conn, warehouse_id, district_id, customer_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select o_id from orders where o_w_id = %s and o_d_id = %s and o_c_id = %s", [warehouse_id, district_id, customer_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows


def get_orderlines(conn, warehouse_id, district_id, order_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select ol_i_id, ol_o_id from orderline where ol_w_id = %s and ol_d_id = %s and ol_o_id = %s", [warehouse_id, district_id, order_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows


def add_related_cust(conn, warehouse_id, itemList, relatedCustSet):
    items = tuple(itemList)
    with conn.cursor() as cur:
        cur.execute(
            "Select ol_w_id, ol_d_id, ol_o_id, sum(1) as Count from orderline where ol_w_id != %s and ol_i_id in %s group by ol_w_id, ol_d_id, ol_o_id", [warehouse_id, items])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        result = cur.fetchall()
        conn.commit()

        for each in result:
            if(each[3] >= 2):
                ol_w_id = each[0]
                d_id = each[1]
                o_id = each[2]
                c_id = get_cust(conn, ol_w_id, d_id, o_id)
                customer = [ol_w_id, d_id, c_id[0]]
                relatedCustSet.add(tuple(customer))

    return relatedCustSet


def get_cust(conn, warehouse_id, district_id, order_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select o_c_id from orders where o_w_id = %s and o_d_id = %s and o_id = %s", [warehouse_id, district_id, order_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        for row in rows:
            c_id = row

        return c_id