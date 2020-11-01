#!/usr/bin/env python3
import time
import random
import logging
import datetime

from decimal import Decimal

# N,C ID,W ID,D ID,M.

# OL I ID,OL SUPPLY W ID,OL QUANTITY.


def make_new_order(conn, c_data, data):
    # for n in data:
    #     print(n)

    # get N --> next available order number D_NEXT_O_ID for district(w_id, d_id)
    # increment N by 1
    # create new order
    print("-----New Order-----")
    with conn.cursor() as cur:
        # data
        c_id = c_data[0]
        w_id = c_data[1]
        d_id = c_data[2]
        m = c_data[3]

        # print("First Line {}, {}, {}, {}".format(c_id, w_id, d_id, m))
        district = get_district(conn, w_id, d_id)
        o_id = district[0]
        # print(o_id)
        o_all_local = id_all_local(w_id, data)
        # print(o_all_local)
        insert_order(conn, w_id, d_id, o_id, c_id, m, o_all_local)
        o_id += 1
        update_d_new_id(conn, w_id, d_id, o_id)

        customer = get_customer(conn, w_id, d_id, c_id)
        print("Customer Identifier: {} {} {}".format(w_id, d_id, c_id))
        print("Customer Last Name: {}".format(customer[0]))
        print("Customer Credit: {}".format(customer[1]))
        print("Customer Discount: {}".format(customer[2]))
        print()

        d_tax = district[1]
        warehouse = get_warehouse(conn, w_id)
        print("Warehouse Tax: {} \nDistrict Tax: {} \n".format(warehouse, d_tax))

        curr_o_id = o_id - 1
        totalAmount = insert_orderLine(conn, w_id, d_id, curr_o_id, data)
        # print(totalAmount)

        c_disc = 1 - customer[2]

        totalAmount = totalAmount * (1 + warehouse + d_tax) * c_disc
        # print(totalAmount)
        print("Num Items: {} \nTotal Amount: {:.2f}\n".format(m, totalAmount))


def get_district(conn, warehouse_id, district_id):
    with conn.cursor() as cur:
        # get next available oder id
        cur.execute(
            "Select d_next_o_id, d_tax from district where d_w_id = %s and d_id = %s", [warehouse_id, district_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        for row in rows:
            res = row

        return res


def id_all_local(warehouse_id, data):
    local = 1
    for d in data:
        split = d.split(",")
        # print(d + " " + split[1])
        if(int(warehouse_id) is not int(split[1])):
            local = 0
            return local

    return local


def insert_order(conn, warehouse_id, district_id, order_id, customer_id, m_count, order_all_local):
    with conn.cursor() as cur:
        cur.execute(
            "Insert into orders (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) values (%s, %s, %s, %s, NULL, %s, %s, %s)", [warehouse_id, district_id, order_id, customer_id, m_count, order_all_local, datetime.datetime.now()])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)

        conn.commit()


def update_d_new_id(conn, warehouse_id, district_id, new_o_id):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE district SET d_next_o_id = %s where d_w_id = %s and d_id = %s", [new_o_id, warehouse_id, district_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)

        conn.commit()


def get_customer(conn, warehouse_id, district_id, customer_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select c_last, c_credit, c_discount from customer where c_w_id = %s and c_d_id = %s and c_id = %s", [warehouse_id, district_id, customer_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        for row in rows:
            res = row

        return res


def get_warehouse(conn, warehouse_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select w_tax from warehouse where w_id = %s", [warehouse_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        for row in rows:
            res = row[0]

        return res


def insert_orderLine(conn, warehouse_id, district_id, curr_order_id, orderLines):
    totalAmount = 0
    ol_dist_info = "S_DIST_{:02d}".format(int(district_id))
    ol_num = 1
    for o in orderLines:
        orderLine = o.strip()
        split = orderLine.split(',')

        item_id = split[0]
        ol_supply_w_id = split[1]
        ol_quantity = split[2]

        # update stock
        # print("w: {}, sup {}".format(warehouse_id, ol_supply_w_id))
        is_remote = warehouse_id is not ol_supply_w_id
        stock = get_stock(conn, ol_supply_w_id, item_id)
        old_s_quantity = stock[0]
        s_quantity = old_s_quantity - Decimal(ol_quantity)

        if(s_quantity < 10):
            s_quantity += 100

        update_stock(conn, ol_supply_w_id, item_id, ol_quantity,
                     s_quantity, is_remote, old_s_quantity)

        item = get_item(conn, item_id)
        item_name = item[0]
        item_price = item[1]
        item_amount = Decimal(ol_quantity) * item_price

        insert_new_orderline(conn, warehouse_id, district_id, curr_order_id,
                             ol_num, item_id, item_amount, ol_supply_w_id, ol_quantity, ol_dist_info)

        ol_num += 1
        # print("Q: {}, p: {}, total: {}".format(
        #     ol_quantity, item_price, item_amount))

        totalAmount += item_amount

        print("Item Number: {}".format(item_id))
        print("Item Name: {}".format(item_name))
        print("Supplier Warehouse: {}".format(ol_supply_w_id))
        print("Quantity: {}".format(ol_quantity))
        print("OL Amount: {}".format(item_amount))
        print("Stock Quantity: {}".format(s_quantity))
        print()

    return totalAmount


def get_stock(conn, ol_sup_w_id, i_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select s_quantity from stock where s_w_id = %s and s_i_id = %s", [ol_sup_w_id, i_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows[0]


def update_stock(conn, ol_supply_w_id, i_id, ol_quantity, s_quantity, is_remote, old_s_quantity):
    s_remote_cnt = 1 if is_remote else 0
    # print(s_remote_cnt)
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE stock SET s_quantity = %s, s_ytd = s_ytd + %s, s_order_cnt = s_order_cnt + %s, s_remote_cnt = s_remote_cnt + %s where s_w_id = %s and s_i_id = %s and s_quantity = %s", [s_quantity, ol_quantity, 1, s_remote_cnt, ol_supply_w_id, i_id, old_s_quantity])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)

        conn.commit()


def get_item(conn, i_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select i_name, i_price from item where i_id = %s", [i_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows[0]


def insert_new_orderline(conn, warehouse_id, district_id, curr_order_id, ol_num, item_id, item_amount, ol_supply_w_id, ol_quantity, ol_dist_info):
    with conn.cursor() as cur:
        cur.execute(
            "Insert into orderline (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d, ol_dist_info) values (%s, %s, %s, %s, %s, %s, %s, %s, NULL, %s)", [curr_order_id, district_id, warehouse_id, ol_num, item_id, ol_supply_w_id, ol_quantity, item_amount, ol_dist_info])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)

        # print("{} {} {} {} {} {} {} {} {}".format(curr_order_id, district_id, warehouse_id,
        #                                           ol_num, item_id, ol_supply_w_id, ol_quantity, item_amount, ol_dist_info))

        conn.commit()
