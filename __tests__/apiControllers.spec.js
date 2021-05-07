/**
 * @jest-environment node
 */

// API Controller Tests

jest.setTimeout(60000);

const {
    masterNode,
    readStore,
    StreamJob,
    SpinUpWorker,
    TearDownWorker,
    StreamAnalytics
} = require('../controllers/apiControllers');

const { v4 : uuidv4 } = require('uuid');
const shajs = require("sha.js");
const { promisify } = require('util');
const sleep = promisify(setTimeout);

const hash = (obj) => {
    obj = obj.toString();
    return shajs('sha256').update(obj).digest('hex');
}

let targetPrefix = uuidv4(),
    jobId,
    job;

describe("StreamJob controller tests", () => {

    test("should return 400 code on incomplete data", async () => {
        job = {}
        let [code, ...rest] = await StreamJob([
            targetPrefix, jobId, job
        ]);
        expect(code).toBe(400);
    });

    test("should return 201 code on complete data", async () => {
        jobId = 1
        job = {
            text : "hello",
            callable_fields : ["text"],
            job_number : jobId,
            max_jobs : 2
        }
        let [code, ...rest] = await StreamJob([
            targetPrefix, jobId, job
        ]);
        expect(code).toBe(201);
    });

    test("the emitted job should be on the stream", async () => {
        let streamId = `${targetPrefix}_${jobId}`;
        expect(JSON.parse(await masterNode.get(streamId))).toStrictEqual(job);
    });
});


describe("SpinUpWorker controller tests", () => {

    let agentType,
        runner, 
        callable,
        store,
        sessionKey,
        pidState,
        pids;

    test("should return 400 code on incomplete data", async () => {
        let [code, ...rest] = await SpinUpWorker([
            agentType, targetPrefix, 
            runner, callable
        ]);
        expect(code).toBe(400);
    });

    test("should return 200 code on complete data", async () => {
        agentType = "proc";
        runner = "python";
        callable = "dummy.py";
        let [code, ...rest] = await SpinUpWorker([
            agentType, targetPrefix, 
            runner, callable
        ]);
        expect(code).toBe(200);
    });

    test("there should be >=1 proc worker for this stream", async () => {
        await sleep(1000);
        store = readStore();
        sessionKey = hash(`${agentType}_${targetPrefix}_${runner}_${callable}`);
        pidState = store[sessionKey];
        pids = Object.keys(pidState);
        expect(pids.length>=1).toBeTruthy();
        expect(pidState[pids[0]]["agentType"]).toBe("proc");
    });

});

describe("TearDownWorker controller tests", () => {

    let agentType,
        runner, 
        callable;

    test("should return 400 code on incomplete data", () => {
        let [code, ...rest] = TearDownWorker([
            agentType, targetPrefix, 
            runner, callable
        ]);
        expect(code).toBe(400);
    });

    test("should return 200 code on complete data", () => {
        agentType = "proc";
        runner = "python";
        callable = "dummy.py";
        let [code, ...rest] = TearDownWorker([
            agentType, targetPrefix, 
            runner, callable
        ]);
        expect(code).toBe(200);
    });

    test("there should be 0 proc workers for this stream after teardown", () => {
        let pids = masterNode.getPIDs(agentType, targetPrefix, runner, callable);
        expect(pids.length).toBe(0);
    });

});

describe("StreamAnalytics controller tests", () => {

    test("invocation returns populated report", async () => {
        let [code, message, report] = await StreamAnalytics();
        expect(report[targetPrefix].count).toBeTruthy();
    });
});