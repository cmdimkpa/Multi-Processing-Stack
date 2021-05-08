# Multi Processing Stack

<img src="https://parallelscore-staging.s3.amazonaws.com/misc/Parallel+Video+processing.svg"/>

## About

This project sets up a generic multi-processing pipeline that can even be utilized beyond the video processing context.

- It is an API that can be used to spin up and tear down workers that process callables (target scripts) against the jobs that come into a Redis queue. 

- There is a queuing endpoint (to add jobs to the Redis queue) and a configurable and callable aggregator service that aggregates the outputs of each worker run in the proper context (current implementation will leverage an internal scheduler to fire this endpoint at small intervals).





