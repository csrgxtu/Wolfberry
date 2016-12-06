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
        # assemble a doc here
        if 'imei' in doc:
            imei = doc.get('imei')
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

        receiveTimestamp = rec.get('receiveTimestamp')['$numberLong']
        timestamp = rec.get('timestamp')['$numberLong']

        # csvStr = rec.get('id') + ',' + doc.get('receiveTimestamp') + ',' + doc.get('jid') + ',' + doc.get('timestamp') + ',' + imei + ',' + None + ',' + signal.get('bssid') + ',' + signal.get('rssi')
        csvStr = str(rec['id']) + ',' + receiveTimestamp + ',' + str(rec['jid']) + ',' + timestamp + ',' + str(rec['imei']) + ',,'
        csvStr = csvStr + str(signal['bssid']) + ',' + str(signal['rssi'])

        print '%s' % csvStr



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
