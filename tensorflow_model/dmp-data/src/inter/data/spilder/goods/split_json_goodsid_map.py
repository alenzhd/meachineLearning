#!/usr/bin/python
# coding: utf-8

import sys
import os
import json


if __name__ == '__main__':

    for line in sys.stdin:
        try:
            #line = '[{"name":"Ôø¾­×îÃÀ","typeid":"D","value":"0.20563675559279293"},{"name":"taobao-17438567791","typeid":"E","value":"1.8432"}]'
            newline=json.loads(line)
            for ln in newline:
                if (ln["typeid"]=='E'):
                    print ln["name"]
        except Exception, err:
            continue
