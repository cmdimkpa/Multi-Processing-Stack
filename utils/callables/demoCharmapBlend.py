# this demo callable will receive an array of all outputs (charmaps) from processing
# It will blend the results into a single charmap and write the result to a key with
# pattern: "DEMO_CHARMAP_targetPrefix"

import sys
import requests as http
import json
import os

masterNodeBaseURL = "http://bus.parallelscore.io:8818/multiproc/api/v1/"
baseHeaders = { "Content-Type" : "application/json" }

def masterNode(command, key, dataObject={}):
    url = "%smasterNode/%s/%s" % (masterNodeBaseURL, command, key)
    payload = {
        "dataObject" : dataObject
    }
    resp = http.put(url, json.dumps(payload), headers=baseHeaders)
    return resp.json()

# data input
dataFile, targetPrefix = sys.argv[1:]
with open(dataFile, "rb") as handle:
    outputs = json.loads(handle.read().decode('latin-1'))
try:
    os.remove(dataFile)
except:
    pass

# blend charmaps
blended_charmap = {}
for charmap in outputs:
    for char in charmap:
        if char not in blended_charmap:
            blended_charmap[char] = charmap[char]
        else:
            blended_charmap[char] += charmap[char]

# save blended charmap to masterNode
key = "DEMO_CHARMAP_%s" % targetPrefix
masterNode("set", key, blended_charmap)

# exit
sys.exit()