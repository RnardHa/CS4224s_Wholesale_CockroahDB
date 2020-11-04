#!/usr/bin/env python3
import time
import random
import logging

from decimal import Decimal

# P,C W ID,C D ID,C ID,PAYMENT.
# P,1,2,40,723.94

debug = False


def make_payment(conn, c_w_id, c_d_id, c_id, payment):
    # update warehouse C_W_ID --> increment W_YTD + PAYMENT
    # Update district C_W_ID & C_D_ID --> increment D_YTD + PAYMENT
    # Update customer c_w_id, c_d_id, c_id
    #       decrement c_balance - payment
    #       icrement c_ytd_payment + payment
    #       increment c_pay4ment_cnt + 1

    # output
    # 1. Customer’s identifier (C W ID, C D ID, C ID), name (C FIRST, C MIDDLE, C LAST), address
    # (C STREET 1, C STREET 2, C CITY, C STATE, C ZIP), C PHONE, C SINCE, C CREDIT,
    # C CREDIT LIM, C DISCOUNT, C BALANCE
    # 2. Warehouse’s address (W STREET 1, W STREET 2, W CITY, W STATE, W ZIP)
    # 3. District’s address (D STREET 1, D STREET 2, D CITY, D STATE, D ZIP)
    # 4. Payment amount PAYMENT
    with conn.cursor() as cur:
        logging.info("-----Payment-----")
        # print("-----Payment-----")

        # update warehouse
        cur.execute(
            "UPDATE warehouse SET w_ytd = w_ytd + %s where w_id = %s", [payment, c_w_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)

        # update district
        cur.execute(
            "UPDATE district SET d_ytd = d_ytd + %s where d_w_id = %s and d_id = %s", [payment, c_w_id, c_d_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)

        # update customer
        cur.execute(
            "UPDATE customer SET c_balance = c_balance - %s, c_ytd_payment = c_ytd_payment + %s, c_payment_cnt = c_payment_cnt + %s where c_w_id = %s and c_d_id = %s and c_id = %s", [payment, payment, 1, c_w_id, c_d_id, c_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)

        conn.commit()

        # output

        cur.execute(
            "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance FROM customer where c_w_id = %s and c_d_id = %s and c_id = %s", [c_w_id, c_d_id, c_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        if debug:
            print(f"Customer at {time.asctime()}:")
            for row in rows:
                print("Customer's Identifier: {}, {}, {}".format(
                    c_w_id, c_d_id, c_id))
                print("Name: {} {} {}".format(row[0], row[1], row[2]))
                print("Address: {}, {}, {}, {}, {}".format(
                    row[3], row[4], row[5], row[6], row[7]))
                print("Phone: {}".format(row[8]))
                print("Since: {}".format(row[9]))
                print("Credit: {}".format(row[10]))
                print("Credit Limit: {}".format(row[11]))
                print("Discount: {}".format(row[12]))
                print("Balance: {}".format(row[13]))

        for row in rows:
            logging.info(
                "[C_ID, Name,  Address, Phone, Since, Credit, C_Limit, Discount, Balance]")
            logging.info(
                "[{} {} {}, {} {} {}, {} {} {} {} {}, {}, {}, {}, {}, {}, {}]".format(c_w_id, c_d_id, c_id, row[0], row[1], row[2], row[3], row[4], row[5], row[6],
                                                                                      row[7], row[8], row[9], row[10], row[11], row[12], row[13]))

            # print(row)

        cur.execute(
            "SELECT w_street_1, w_street_2, w_city, w_state, w_zip FROM warehouse where w_id = %s", [c_w_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        if debug:
            print()
            print(f"Warehouse at {time.asctime()}:")
            for row in rows:
                print("Warehouse's Address: {}, {}, {}, {}, {}".format(
                    row[0], row[1], row[2], row[3], row[4]))
        for row in rows:
            logging.info("[W_Address]")
            logging.info("{} {} {} {} {}".format(
                row[0], row[1], row[2], row[3], row[4]))

            # print(row)

        cur.execute(
            "SELECT d_street_1, d_street_2, d_city, d_state, d_zip FROM district where d_w_id = %s and d_id = %s", [c_w_id, c_d_id])
        logging.debug("make payment(): status message: %s",
                      cur.statusmessage)
        rows = cur.fetchall()
        conn.commit()
        if debug:
            print()
            print(f"District at {time.asctime()}:")
            for row in rows:
                print("District's Address: {}, {}, {}, {}, {}".format(
                    row[0], row[1], row[2], row[3], row[4]))

        for row in rows:
            logging.info("[D_Address]")
            logging.info("{} {} {} {} {}".format(
                row[0], row[1], row[2], row[3], row[4]))
            # print(row)

        if debug:
            print()
            print("Payment Amount: {}".format(payment))
            print()
        logging.info("[Payment Amount]")
        logging.info("{}".format(payment))

    return time.thread_time()
