// Redis Worker Model

const redis = require("redis");
const config = require("../utils/config.json");
const redis_url = config.REDIS_URL || "redis://localhost";
const client = redis.createClient(redis_url);
const { promisify } = require("util");
const sleep = promisify(setTimeout);
const { v4 : uuidv4 } = require("uuid");
const { exec } = require("child_process");
const pollAgent = `${__dirname.split('/models')[0]}/utils/listeners/poll.js`;
const aggAgent = `${__dirname.split('/models')[0]}/utils/listeners/agg_poll.js`;
const storePath = `${__dirname.split('/models')[0]}/utils/store.json`;
const fs = require("fs");
const shajs = require("sha.js");

const readStore = () => {
    let store = {};
    try {
        let data_str = fs.readFileSync(storePath, 'utf8');
        store =  JSON.parse(data_str);
    } catch(err){}
    return store;
}

const writeStore = (data) => {
    fs.writeFileSync(storePath, JSON.stringify(data));
    return data;
}

const hash = (obj) => {
    obj = obj.toString();
    return shajs('sha256').update(obj).digest('hex');
}

class basicWorker {
    constructor (){
        this.get = promisify(client.get).bind(client);
        this.set = promisify(client.set).bind(client);
        this.del = promisify(client.del).bind(client);
        this.keys = promisify(client.keys).bind(client);
    }
}

class coordinator extends basicWorker {
    constructor (){
        super()
    }
    async updatePIDs(agentType, targetPrefix, runner, callable){
        let sessionKey = hash(`${agentType}_${targetPrefix}_${runner}_${callable}`);
        await sleep(config.UPDATE_PIDS_INTERVAL);
        exec('forever logs', (error, stdout, stderr)=>{
            let header = '/.forever/';
            let footer = '.log';
            let context = `/utils/listeners/${agentType === 'agg' ? 'agg_' : ''}poll.js`;
            let pids = stdout.split('\n')
                       .filter(line => line.includes(header) && line.includes(context))
                       .map(line => line.split(header)[1].split(footer)[0])
            let old_pids = this.getPIDs(agentType, targetPrefix, runner, callable);
            let new_pids;
            if (old_pids.length  > 0){
                new_pids = pids.filter((pid) => old_pids.indexOf(pid) === -1);
            } else {
                new_pids = [pids.slice(-1)[0]];
            }
            let pidState = {};
            for (let pid of new_pids){
                pidState[pid] = {
                    agentType,
                    targetPrefix,
                    runner,
                    callable
                }
            }
            let store = readStore();
            store[sessionKey] = pidState;
            writeStore(store);
        }); 
    }
    getPIDs(agentType, targetPrefix, runner, callable){
        let sessionKey = hash(`${agentType}_${targetPrefix}_${runner}_${callable}`);
        let pids = [];
        try {
            let store = readStore();
            let pidState = store[sessionKey];
            pids = Object.keys(pidState);
        } catch(err){}
        return pids;
    }
    async spinUp(agentType, targetPrefix, runner, callable){
        let agentMap = {
            agg : aggAgent,
            proc : pollAgent
        };
        let run_cmd = `forever start -c node ${agentMap[agentType]} ${targetPrefix} ${runner} ${callable}`;
        exec(run_cmd);
        await this.updatePIDs(agentType, targetPrefix, runner, callable);
    }
    tearDown(agentType, targetPrefix, runner, callable){
        let sessionKey = hash(`${agentType}_${targetPrefix}_${runner}_${callable}`);
        let pidState = {};
        let store = readStore();
        try {
            pidState = store[sessionKey];
            delete store[sessionKey];
            writeStore(store);
        } catch (err){}
        try {
            let pids = Object.keys(pidState);
            let run_cmd;
            for (let pid of pids){
                run_cmd = `forever stop ${pid}`;
                exec(run_cmd);
            }
        } catch(err){}
    }
}

module.exports = {
    basicWorker,
    coordinator,
    readStore
}