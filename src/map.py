#!/usr/bin/env python

import sys
import json
import bson


# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # turn data into json dict
    doc = json.loads(line)

    for signal in doc['signals']:
        print line
        # assemble a doc here
        if 'imei' in doc:
            imei = doc['imei']
        else:
            imei = None

        rec = {
            'id': str(bson.ObjectId()),
            'receiveTimestamp': doc['receiveTimestamp'],
            'jid': doc['jid'],
            'timestamp': doc['timestamp'],
            'imei': imei,
            'ssid': None,
            'bssid': signal['bssid'],
            'rssi': signal['rssi']
        }

        print '%s\t%s' % (rec['id'], json.dumps(rec))



    # # split the line into words
    # words = line.split()
    # # increase counters
    # for word in words:
    #     # write the results to STDOUT (standard output);
    #     # what we output here will be the input for the
    #     # Reduce step, i.e. the input for reducer.py
    #     #
    #     # tab-delimited; the trivial word count is 1
    #     print '%s\t%s' % (word, 1)
