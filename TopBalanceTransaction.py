#!/usr/bin/env python3
import time
import random
import logging
import datetime

TOP_N = 10

debug = False


def get_top_balance(conn):
    start = time.time()
    # print("-----Top Balance-----")
    logging.info("-----Top Balance-----")

    get_warehouse_names = get_warehouse(conn)
    warehouse = {}
    for name in get_warehouse_names:
        warehouse[name[0]] = name[1]

    district = {}
    get_district_name = get_district(conn)
    for d in get_district_name:
        key = str(d[0]) + ',' + str(d[1])
        district[key] = d[2]

    customers = get_top_customer(conn, TOP_N)
    for cust in customers:
        w_id = cust[0]
        d_id = cust[1]
        district_key = str(w_id) + ',' + str(d_id)

        if debug:
            print("Customer Name: {} {} {}".format(cust[2], cust[3], cust[4]))
            print("Balance: {}".format(cust[5]))
            print("Warehouse Name: {}".format(warehouse.get(w_id)))
            print("District Name: {}".format(district.get(district_key)))
            print()
        logging.info("[C_Name, Balance, W_Name, D_Name]")
        logging.info("{} {} {}, {}, {}, {}".format(
            cust[2], cust[3], cust[4], cust[5], warehouse.get(w_id), district.get(district_key)))

    end = time.time()
    return end - start


def get_top_customer(conn, n):
    with conn.cursor() as cur:
        cur.execute(
            "Select c_w_id, c_d_id, c_first, c_middle, c_last, c_balance from customer order by c_balance desc limit %s", [n])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows


def get_warehouse(conn):
    with conn.cursor() as cur:
        cur.execute(
            "Select w_id, w_name from warehouse")
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows


def get_district(conn):
    with conn.cursor() as cur:
        cur.execute(
            "Select d_w_id, d_id, d_name from district")
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows
