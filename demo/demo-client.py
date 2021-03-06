# This client performs the actions detailed in the README
# ensure you are within the /demo folder when running this script

import requests as http
import json
import pickle
import os
import sys
from random import random
from time import sleep
import datetime

HERE = os.getcwd()
if "\\" in HERE:
    slash = "\\"
else:
    slash = "/"
HERE += slash

masterNodeBaseURL = "http://bus.parallelscore.io:8818/multiproc/api/v1/"
baseHeaders = { "Content-Type" : "application/json" }

# Helper functions

def now(): return datetime.datetime.today()

def display(done, maxJobs):
    buffer = 2
    for i in range(buffer):
        print("")
    percent_done = done/maxJobs*100
    ticks = int(percent_done)
    tick_lines = "".join(['-' for i in range(ticks)])
    status = "stream progress: %s > %.2f percent complete" % (tick_lines, percent_done)
    print(status)

def get_uid(n=6):
    return str(random()).split(".")[1][:n]

def get_chunks(stream, max_chunk_length):
    stream_length = len(stream)
    whole_chunks = stream_length // max_chunk_length
    chunks = []
    for i in range(whole_chunks):
        start = i*max_chunk_length
        stop = (i+1)*max_chunk_length
        chunk = stream[start:stop]
        chunks.append(chunk)
    rem_bytes = stream_length % max_chunk_length
    if rem_bytes:
        partial_chunk = stream[-rem_bytes:]
        chunks.append(partial_chunk)
    return chunks

def StreamJob(targetPrefix, jobId, job):
    url = f"{masterNodeBaseURL}StreamJob"
    payload = {
        "targetPrefix" : targetPrefix,
        "jobId" : jobId,
        "job" : job
    }
    resp = http.post(url, json.dumps(payload), headers=baseHeaders)
    return resp.json()

def WorkerAction(actionType, agentType, targetPrefix, runner, callable):
    url = f"{masterNodeBaseURL}{actionType}Worker?agentType={agentType}&targetPrefix={targetPrefix}&runner={runner}&callable={callable}"
    payload = {}
    resp = http.put(url, json.dumps(payload), headers=baseHeaders)
    return resp.json()

def masterNode(command, key, dataObject={}):
    url = f"{masterNodeBaseURL}masterNode/{command}/{key}"
    payload = {
        "dataObject" : dataObject
    }
    resp = http.put(url, json.dumps(payload), headers=baseHeaders)
    return resp.json()

## PREPROCESSING

# read video input (as binary blob)

videoId = sys.argv[1]
videoPath = f"{HERE}video{slash}sample{videoId}.mp4"

with open(videoPath, "rb") as handle:
    blob = handle.read()

# pickle the binary data to string

string_to_stream = pickle.dumps(blob).decode('latin-1')

# set max_chunk_length

max_chunk_length = 500000 # 500KB chunks

# get chunks

chunks = get_chunks(string_to_stream, max_chunk_length)

# get number of chunks

num_chunks = len(chunks)

## STREAMING

started = now()

targetPrefix = "demoVideoProcessing%s" % get_uid()
runner = "python"
procCallable = "demoCharmapCount.py"
aggCallable = "demoCharmapBlend.py"

print("StreamingSession started. Context: ==> targetPrefix:%s, runner:%s, procCallable:%s, aggCallable:%s" % (
    targetPrefix, runner,
    procCallable, aggCallable
))

# 1. spin up a `proc` worker for the session
WorkerAction("SpinUp", "proc", targetPrefix, runner, procCallable)
print("PROC worker spun-up successfully")

# 2. stream chunks
print("Streaming started. ChunkSize:%d, NumberofChunks:%d" % (
    max_chunk_length, num_chunks
))
jobId = 0
for chunk in chunks:
    jobId += 1
    job = {
        "max_jobs" : num_chunks,
        "job_number" : jobId,
        "callable_fields" : ["targetPrefix", "jobId", "chunk"],
        "targetPrefix" : targetPrefix,
        "jobId" : jobId,
        "chunk" : chunk
    }
    StreamJob(targetPrefix, jobId, job)
    display(jobId, num_chunks)

# 4. spin up an `agg` worker to aggregate the stream
print("Spinning up AGG worker...")
WorkerAction("SpinUp", "agg", targetPrefix, runner, aggCallable)

# 5. wait for output
output = None
epoch = 3
epochs = 0
key = "DEMO_CHARMAP_%s" % targetPrefix
while not output:
    output = masterNode("get", key)["data"]
    sleep(epoch)
    epochs += 1
    print("retrying after %d epochs..." % epochs)

# 7. Print Result
print("Session ended. Output below:", output)

elapsed = (now() - started).total_seconds()
print("")
print("performance: processed %d chunks in %.2f seconds (%.2f chunks/sec)" % (num_chunks, elapsed, num_chunks/elapsed))




