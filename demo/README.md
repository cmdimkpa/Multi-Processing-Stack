## Multiprocessing Demo

In this demo, we create multiple clients that are streaming video data to the server at the same time.

We are able to see the real-time analytics from the server as their jobs are being processed concurrently. At the end of each job, the result of processing will be displayed.

###  Selected Use Case: Video Analysis

- Each client in this demo will stream one of the demo videos to the server in the following steps:
    - First, the client will spin up a `proc` worker for the session.
    - Next, the videos will be split into a significant number of small chunks and streamed to the server.
    - As each chunk hits the server, we expect it to be processed by the active `proc` worker which will invoke a `callable` script that actually performs the processing action on the chunk.
    - In this example, the callable script will build a `charmap` of the binary chunk and write its output to the redis server via the `masterNode`.
    - At the end of the stream, the `proc` worker is torn down by the client and the `agg` worker will be triggered.
    - The agg worker will aggregate all processing outputs and send to the aggregation `callable`.
    - In this example, the aggregation callable will plot the `character density` of each `char` in the `charmap` over the entire video.
    - At the end of the aggregation, the agg callable will `tear down` the `agg` worker and save the result to redis.
    - the waiting client will receive the result, display and terminate.