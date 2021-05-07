/**
 * @jest-environment node
 */

// Redis Worker Tests

jest.setTimeout(60000);

const { coordinator, readStore } = require('../models/redisWorker');
const { v4 : uuidv4 } = require('uuid');
const shajs = require("sha.js");

const hash = (obj) => {
    obj = obj.toString();
    return shajs('sha256').update(obj).digest('hex');
}

let worker = new coordinator();

describe("Basic Redis Worker tests", () => {
    let key = uuidv4();
    test("should be able to write/read data", async () => {
        let value = "Hello, World!"
        await worker.set(key, value);
        expect(await worker.get(key)).toBe(value);
    });
    test("should be able to delete data", async () => {
        await worker.del(key);
        expect(await worker.get(key)).toBeFalsy();
    });
    test("should be able to find patterned keys", async () => {
        let suffixes = ["cat", "dog", "hen"];
        let patterned_key;
        for (const suffix of suffixes){
            patterned_key = `${key}_${suffix}`;
            await worker.set(patterned_key, suffix);
        }
        let matches = await worker.keys(`${key}*`);
        expect(await matches.length).toBe(suffixes.length);
    });
});

describe("Coordinator tests - Spin up", () => {
    let targetPrefix = "myTestJob"
    let runner = "python"
    let callable = "dummy.py"
    test("should be able to spin up a processor worker", async () => {
        let agentType = "proc"
        await worker.spinUp(agentType, targetPrefix, runner, callable);
        let store = readStore();
        let sessionKey = hash(`${agentType}_${targetPrefix}_${runner}_${callable}`);
        expect(store[sessionKey]).toBeTruthy();
    });
    test("should be able to spin up an aggregator worker", async () => {
        let agentType = "agg"
        await worker.spinUp(agentType, targetPrefix, runner, callable);
        let store = readStore();
        let sessionKey = hash(`${agentType}_${targetPrefix}_${runner}_${callable}`);
        expect(store[sessionKey]).toBeTruthy();
    });
});


describe("Coordinator tests - Tear down", () => {
    let targetPrefix = "myTestJob"
    let runner = "python"
    let callable = "dummy.py"
    test("should be able to tear down processor workers", async () => {
        let agentType = "proc"
        worker.tearDown(agentType, targetPrefix, runner, callable);
        await worker.updatePIDs(agentType, targetPrefix, runner, callable);
        let new_pids = worker.getPIDs(agentType, targetPrefix, runner, callable);
        expect(new_pids.length).toBe(0);
    });
    test("should be able to tear down aggregator workers", async () => {
        let agentType = "agg"
        worker.tearDown(agentType, targetPrefix, runner, callable);
        await worker.updatePIDs(agentType, targetPrefix, runner, callable);
        let new_pids = worker.getPIDs(agentType, targetPrefix, runner, callable);
        expect(new_pids.length).toBe(0);
    });
});
