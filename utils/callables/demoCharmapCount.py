# this demo callable will receive a video chunk and calculate the charmap of the chunk
# the output will be saved to the masterNode using the key pattern: "MULTIPROC_OUTPUT_targetPrefix_jobId"

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
dataFile = sys.argv[1]
with open(dataFile, "rb") as handle:
    targetPrefix, jobId, chunk = json.loads(handle.read().decode('latin-1'))
try:
    os.remove(dataFile)
except:
    pass

# generate charmap
charmap = {}
for iota in chunk:
    try:
        char = ord(iota)
    except:
        char = 0
    if char in charmap:
        charmap[char] += 1
    else:
        charmap[char] = 1

# save charmap to masterNode
key = "MULTIPROC_OUTPUT_%s_%s" % (targetPrefix, jobId)
masterNode("set", key, charmap)

# exit
sys.exit()