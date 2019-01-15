#!/usr/bin/python

import csv
import sys
import getopt
import time
import paho.mqtt.client as paho
import argparse


def send_message_loop(client, filename, wait_time, device_name):
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        
        while True:
            line_count = 0
            column_names = []
            for row in csv_reader:
                if line_count == 0:
                    print(f'Column names are {", ".join(row)}')
                    column_names = row
                    line_count += 1
                else:
                    print(f'Datas are {", ".join(row)}')

                    i = 0
                    while i < len(column_names):
                        client.publish("device/" + device_name + "/" + column_names[i],row[i])
                        i = i + 1
                    
                    line_count += 1
                time.sleep(wait_time)
            csv_file.seek(0)


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("broker", type=str,
                    help="MQTT broker url")
    parser.add_argument("file", type=str,
                    help="Input csv file")
    parser.add_argument("device", type=str,
                    help="device identifier for MQTT topic (for example 'device/[device]/temperature'")
    parser.add_argument("-u", "--user", help="Username to login to MQTT server")
    parser.add_argument("-p", "--password", help="Password for MQTT server")
    parser.add_argument("-t", "--wait_time", help="Time to wait (seconds) between each send to MQTT topic", type=float)
    args = parser.parse_args()

    if args.wait_time is None:
        args.wait_time = 1

    client = paho.Client("client-" + args.device)
    if (args.user is not None and args.password is not None):
        client.username_pw_set(username=args.user,password=args.password)
    client.connect(args.broker)
    send_message_loop(client, args.file, args.wait_time, args.device)


if __name__ == "__main__":
    main(sys.argv[1:])
