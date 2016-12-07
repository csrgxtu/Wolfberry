#!/usr/bin/env python
# coding=utf-8
#
# File: SplitBySignal.py
# Date: 07/Nov/2016
# Desc: 按照信号列表刨分original_signal成csv文件，以准备给hive使用
# Author: Archer

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
import json
import bson

class SplitBySignal(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):
        doc = json.loads(line.strip())

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

            csvStr = str(rec['id']) + ',' + receiveTimestamp + ',' + str(rec['jid']) + ',' + timestamp + ',' + str(rec['imei']) + ',,'
            csvStr = csvStr + str(signal['bssid']) + ',' + str(signal['rssi'])

            # yield (str(rec['id']), csvStr)
            yield None, csvStr

if __name__ == '__main__':
    SplitBySignal.run()
