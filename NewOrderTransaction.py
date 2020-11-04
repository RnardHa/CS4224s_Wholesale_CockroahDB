#!/usr/bin/env python3
import time
import random
import logging
import datetime

from decimal import Decimal

# N,C ID,W ID,D ID,M.

# OL I ID,OL SUPPLY W ID,OL QUANTITY.
# conn, N_W_ID, N_D_ID, N_C_ID, M, itemNumber, supplierWarehouse, quantity

debug = False


# def make_new_order(conn, w_id, d_id, c_id, m, data):
def make_new_order(conn, c_data, data):
    # for n in data:
    #     print(n)

    # get N --> next available order number D_NEXT_O_ID for district(w_id, d_id)
    # increment N by 1
    # create new order
    # print("-----New Order-----")
    logging.info("-----New Order-----")
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
        insert = insert_order(conn, w_id, d_id, o_id, c_id, m, o_all_local)
        o_entry_d = insert
        o_id += 1
        update_d_new_id(conn, w_id, d_id, o_id)

        customer = get_customer(conn, w_id, d_id, c_id)
        if debug:
            print("Customer Identifier: {} {} {}".format(w_id, d_id, c_id))
            print("Customer Last Name: {}".format(customer[0]))
            print("Customer Credit: {}".format(customer[1]))
            print("Customer Discount: {}".format(customer[2]))
            print()

        logging.info(
            "C_ID \tC_Last \t\tC_Credit \tC_Discount")
        logging.info("{} {} {} \t{} \t{} \t\t{}".format(
            w_id, d_id, c_id, customer[0], customer[1], customer[2]))

        d_tax = district[1]
        warehouse = get_warehouse(conn, w_id)
        if debug:
            print("Warehouse Tax: {} \nDistrict Tax: {} \n".format(warehouse, d_tax))

        logging.info(
            "W_Tax \tD_Tax")
        logging.info(
            "{} \t{}".format(warehouse, d_tax))

        curr_o_id = o_id - 1
        totalAmount = insert_orderLine(conn, w_id, d_id, curr_o_id, data)
        # print(totalAmount)

        c_disc = 1 - customer[2]

        totalAmount = totalAmount * (1 + warehouse + d_tax) * c_disc
        # print(totalAmount)
        if debug:
            print("Order ID: {} \nOrder Entry Date: {}".format(
                curr_o_id, o_entry_d))
            print("Num Items: {} \nTotal Amount: {:.2f}\n".format(m, totalAmount))
            print()

        logging.info("Order ID \tOrder Entry Date")
        logging.info("{} \t{}".format(curr_o_id, o_entry_d))
        logging.info("Num Items \tTotal Amount")
        logging.info("{} \t\t{:.2f}".format(m, totalAmount))

        return time.thread_time()


def get_district(conn, warehouse_id, district_id):
    with conn.cursor() as cur:
        # get next available oder id
        cur.execute(
            "Select d_next_o_id, d_tax from district where d_w_id = %s and d_id = %s", [warehouse_id, district_id])
        logging.debug("get_district(): status message: %s",
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
    time = datetime.datetime.now()
    with conn.cursor() as cur:
        cur.execute(
            "Insert into orders (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) values (%s, %s, %s, %s, NULL, %s, %s, %s)", [warehouse_id, district_id, order_id, customer_id, m_count, order_all_local, time])
        logging.debug("insert_order(): status message: %s",
                      cur.statusmessage)

        conn.commit()

        return time


def update_d_new_id(conn, warehouse_id, district_id, new_o_id):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE district SET d_next_o_id = %s where d_w_id = %s and d_id = %s", [new_o_id, warehouse_id, district_id])
        logging.debug("update_d_new_id(): status message: %s",
                      cur.statusmessage)

        conn.commit()


def get_customer(conn, warehouse_id, district_id, customer_id):
    with conn.cursor() as cur:
        cur.execute(
            "Select c_last, c_credit, c_discount from customer where c_w_id = %s and c_d_id = %s and c_id = %s", [warehouse_id, district_id, customer_id])
        logging.debug("get_customer(): status message: %s",
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
        logging.debug("get_warehouse(): status message: %s",
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
    ol_insert_list = []
    item_set = set()
    ol_sup = set()
    for each in orderLines:
        orderLine = each.strip()
        split = orderLine.split(',')

        item_set.add(int(split[0]))
        ol_sup.add(int(split[1]))

    stock = get_stock(conn, ol_sup, item_set)
    stock_dic = {}
    # s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt
    for s in stock:
        key = s[0]
        val = (s[1], s[2], s[3], s[4])
        stock_dic[key] = val

    item = get_item(conn, item_set)
    item_dic = {}
    for i in item:
        key = i[0]
        val = (i[1], i[2])
        item_dic[key] = val

    update_stock_list = []
    stock_list = {}
    for o in orderLines:
        orderLine = o.strip()
        split = orderLine.split(',')

        item_id = int(split[0])
        ol_supply_w_id = split[1]
        ol_quantity = split[2]

        if warehouse_id is not ol_supply_w_id:
            is_remote = 0
        else:
            is_remote = 1

        stock_info = stock_dic.get(item_id)
        quantity = stock_info[0]
        s_ytd = stock_info[1]
        s_order_cnt = stock_info[2]
        s_remote_cnt = stock_info[3]

        s_quantity = quantity - Decimal(ol_quantity)

        if(s_quantity < 10):
            s_quantity += 100

        if item_id not in stock_list:
            up_stock = (ol_supply_w_id, item_id, s_quantity, s_ytd +
                        Decimal(ol_quantity), s_order_cnt + 1, s_remote_cnt + is_remote)
            stock_list[item_id] = up_stock
        else:
            info = stock_list.get(item_id)
            quan = info[2]
            up_s_ytd = info[3]
            up_o_cnt = info[4]
            up_rem_cnt = info[5]
            up_stock = (ol_supply_w_id, item_id, Decimal(quan) - Decimal(ol_quantity),
                        Decimal(up_s_ytd) + Decimal(ol_quantity), up_o_cnt + 1, up_rem_cnt + is_remote)
            stock_list[item_id] = up_stock

        item = item_dic[item_id]
        item_name = item[0]
        item_price = item[1]
        item_amount = Decimal(ol_quantity) * item_price

        ol = (str(curr_order_id), str(district_id), str(warehouse_id), str(ol_num), str(
            item_id), str(ol_supply_w_id), str(ol_quantity), Decimal(item_amount), str(ol_dist_info))

        ol_insert_list.append(ol)

        ol_num += 1
        # print("Q: {}, p: {}, total: {}".format(
        #     ol_quantity, item_price, item_amount))

        totalAmount += item_amount

        if debug:
            print("Item Number: {}".format(item_id))
            print("Item Name: {}".format(item_name))
            print("Supplier Warehouse: {}".format(ol_supply_w_id))
            print("Quantity: {}".format(ol_quantity))
            print("OL Amount: {}".format(item_amount))
            print("Stock Quantity: {}".format(s_quantity))
            print()

        logging.info(
            "I_Num, I_Name, Sup_W, Quantity, OL_Amt, S_Quantity")
        logging.info("{}, {}, {}, {}, {}, {}".format(
            item_id, item_name, ol_supply_w_id, ol_quantity, item_amount, s_quantity))
    # bulk insert here
    for i in stock_list:
        update_stock_list.append(stock_list.get(i))

    update_stock(conn, update_stock_list)
    insert_new_orderline(conn, ol_insert_list)
    return totalAmount


def get_stock(conn, ol_sup_w_id, i_id):
    ol_set = tuple(ol_sup_w_id)
    i_id_set = tuple(i_id)
    with conn.cursor() as cur:
        cur.execute(
            "Select s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt from stock where s_w_id in %s and s_i_id in %s", [ol_set, i_id_set])
        logging.debug("get_stock(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows


def update_stock(conn, update_list):
    with conn.cursor() as cur:
        val_str = b','.join(cur.mogrify(
            "(%s,%s,%s,%s,%s,%s)", x) for x in update_list)
        cur.execute(
            "UPSERT into stock (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt) VALUES " + str(val_str, 'utf-8'))
        logging.debug("update_stock(): status message: %s",
                      cur.statusmessage)

        conn.commit()


def get_item(conn, i_id):
    i_id_set = tuple(i_id)
    with conn.cursor() as cur:
        cur.execute(
            "Select i_id, i_name, i_price from item where i_id in %s", [i_id_set])
        logging.debug("get_item(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()

        return rows


def insert_new_orderline(conn, insert_list):
    # val = tuple(insert_list)
    # print(val)
    with conn.cursor() as cur:
        val_str = b','.join(cur.mogrify(
            "(%s,%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in insert_list)
        cur.execute(
            "Insert into orderline (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) values " + str(val_str, 'utf-8'))
        logging.debug("insert_new_orderline(): status message: %s",
                      cur.statusmessage)

        # print("{} {} {} {} {} {} {} {} {}".format(curr_order_id, district_id, warehouse_id,
        #                                           ol_num, item_id, ol_supply_w_id, ol_quantity, item_amount, ol_dist_info))

        conn.commit()
