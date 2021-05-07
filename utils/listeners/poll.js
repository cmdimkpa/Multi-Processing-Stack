/*
    Poll Redis for new jobs containing a particular key.
    If a matching job is found, and it has callable fields,
    invoke a background callable task.
*/

const { basicWorker } = require('../../models/redisWorker');
const sleep = require('util').promisify(setTimeout);
let {
    SCAN_INTERVAL,
    STREAM_CLOSURE_INTERVAL, 
    PROCESSED_JOBS_PATTERN, 
    FINISHED_JOBS_PATTERN
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
    let keyPattern = `${targetPrefix}*`;
    let matches;
    while (true) {
        matches = await scanner.keys(keyPattern);
        if (matches.length > 0){
            for (let jobKey of matches){
                try {
                    let job = JSON.parse(await scanner.get(jobKey));
                    if (job.callable_fields && job.job_number && job.max_jobs){
                        let args = job.callable_fields.map((field) => job[field]);
                        let args_file = randFile(args);
                        let callable_cmd = `sudo ${runner} ${callablePath} ${args_file}`;
                        exec(callable_cmd);
                        let signal = job.job_number == job.max_jobs ?
                                     FINISHED_JOBS_PATTERN : PROCESSED_JOBS_PATTERN
                        if (signal === FINISHED_JOBS_PATTERN){
                            await sleep(STREAM_CLOSURE_INTERVAL);
                        }
                        signal = signal.replace('{}', jobKey);
                        await scanner.set(signal, JSON.stringify(job));
                    }
                } catch(err){}
                await scanner.del(jobKey);
            }
        }
        await sleep(SCAN_INTERVAL);
    }
}

scan()