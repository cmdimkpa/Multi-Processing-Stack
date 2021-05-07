/*
    Poll Redis for finished jobs.
    If a finished job is found, aggregate all its partial outputs into
    a list and pass them to a background callable task for final action.
    Also clear out all traces of the job from both the PROCESSING and FINISHED
    streams.
*/

const { basicWorker } = require('../../models/redisWorker');
const sleep = require('util').promisify(setTimeout);
let {
    SCAN_INTERVAL
} = require('../../utils/config.json');
const [ targetPrefix, runner, callable ] = process.argv.slice(2,);
const callablePath = `${__dirname.split('/listeners')[0]}/callables/${callable}`;
const { exec } = require("child_process");
const fs = require("fs");

const randFile = (data) => {
    let rToken = `${Math.random().toString().split(".")[1].slice(0,6)}.args`
    let rFilePath = `${__dirname.split('/listeners')[0]}/callables/${rToken}`;
    fs.writeFileSync(rFilePath, JSON.stringify(data));
    return rFilePath;
}

const scanner = new basicWorker();

const scan = async () => {
    let keyPattern = `MULTIPROC_FINISHED_${targetPrefix}*`;
    let matches;
    while (true) {
        matches = await scanner.keys(keyPattern);
        if (matches.length > 0){
            let outputs = [];
            keyPattern = `MULTIPROC_OUTPUT_${targetPrefix}*`;
            matches = await scanner.keys(keyPattern);
            if (matches.length > 0){
                // aggregate job outputs
                matches = matches.sort();
                for (let jobKey of matches){
                    try {
                        let artefact = JSON.parse(await scanner.get(jobKey));
                        if (artefact){
                            outputs.push(artefact);
                        }
                    } catch(err){}
                    await scanner.del(jobKey);
                }
                // cleanup all traces of this job
                keyPattern = `*${targetPrefix}*`;
                matches = await scanner.keys(keyPattern);
                for (let jobKey of matches){ await scanner.del(jobKey) }
                // send outputs to callable
                let args_file = randFile(outputs);
                let callable_cmd = `sudo ${runner} ${callablePath} ${args_file} ${targetPrefix}`;
                exec(callable_cmd);
            }
            
        }
        await sleep(SCAN_INTERVAL);
    }
}

scan()