#!/usr/bin/env python3

import time
import random
import logging
from argparse import ArgumentParser
import sys
import getopt
import time
import numpy as np
import csv
from statistics import mean


def main():
    readFile = open("clients.csv", 'r')
    lines = readFile.readlines()

    # experiment number, min throughput, avg throughput, max throughput
    case5_throughput = []
    case6_throughput = []
    case7_throughput = []
    case8_throughput = []

    for l in lines:
        split = l.split(',')
        if (int(split[0]) == 5):
            case5_throughput.append(float(split[4]))
        elif (int(split[0]) == 6):
            case6_throughput.append(float(split[4]))
        elif (int(split[0]) == 7):
            case7_throughput.append(float(split[4]))
        elif (int(split[0]) == 8):
            case8_throughput.append(float(split[4]))

    with open('throughput.csv', 'a+', newline='') as file:
        if len(case5_throughput) > 0:
            case5_min = min(case5_throughput)
            case5_avg = mean(case5_throughput)
            case5_max = max(case5_throughput)
            writer = csv.writer(file)
            writer.writerow([5, case5_min, case5_avg, case5_max])

        if len(case6_throughput) > 0:
            case6_min = min(case6_throughput)
            case6_avg = mean(case6_throughput)
            case6_max = max(case6_throughput)
            writer = csv.writer(file)
            writer.writerow([6, case6_min, case6_avg, case6_max])

        if len(case7_throughput) > 0:
            case7_min = min(case7_throughput)
            case7_avg = mean(case7_throughput)
            case7_max = max(case7_throughput)
            writer = csv.writer(file)
            writer.writerow([7, case7_min, case7_avg, case7_max])

        if len(case8_throughput) > 0:
            case8_min = min(case8_throughput)
            case8_avg = mean(case8_throughput)
            case8_max = max(case8_throughput)
            writer = csv.writer(file)
            writer.writerow([8, case8_min, case8_avg, case8_max])


if __name__ == "__main__":
    main()
