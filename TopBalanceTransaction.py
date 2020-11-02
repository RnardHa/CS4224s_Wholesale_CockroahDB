#!/usr/bin/env python3
import time
import random
import logging
import datetime

TOP_N = 10

debug = False


def get_top_balance(conn):
    print("-----Top Balance-----")
    logging.info("-----Top Balance-----")

    customers = get_top_customer(conn, TOP_N)
    for cust in customers:
        w_id = cust[0]
        d_id = cust[1]
        warehouseInfo = get_warehouse(conn, w_id)
        districtInfo = get_district(conn, w_id, d_id)

        if debug:
            print("Customer Name: {} {} {}".format(cust[2], cust[3], cust[4]))
            print("Balance: {}".format(cust[5]))
            print("Warehouse Name: {}".format(warehouseInfo))
            print("District Name: {}".format(districtInfo))
            print()
        logging.info("[C_Name, Balance, W_Name, D_Name]")
        logging.info("{} {} {}, {}, {}, {}".format(
            cust[2], cust[3], cust[4], cust[5], warehouseInfo, districtInfo))


def get_top_customer(conn, n):
    with conn.cursor() as cur:
        cur.execute(
            "Select c_w_id, c_d_id, c_first, c_middle, c_last, c_balance from customer order by c_balance desc limit %s", [n])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows


def get_warehouse(conn, warehouse_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select w_name from warehouse where w_id = %s", [warehouse_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        for row in rows:
            w_name = row[0]

        return w_name


def get_district(conn, warehouse_id, district_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select d_name from district where d_w_id = %s and d_id = %s", [warehouse_id, district_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        for row in rows:
            d_name = row[0]

        return d_name
