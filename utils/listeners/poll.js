/*
    Poll Redis for new jobs containing a particular key.
    If a matching job is found, and it has callable fields,
    invoke a background callable task.
*/

const { coordinator } = require('../../models/redisWorker');
const sleep = require('util').promisify(setTimeout);
let {
    SCAN_INTERVAL,
    PROCESSED_JOBS_PATTERN, 
    FINISHED_JOBS_PATTERN,
    ACCEPTABLE_CUTOFF,
    BREAK_OUT_EPOCHS
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

const scanner = new coordinator();

const notify = async (signal, jobKey, job) => {
    signal = signal.replace('{}', jobKey);
    await scanner.set(signal, JSON.stringify(job));
}

const scan = async () => {
    let keyPattern = `${targetPrefix}*`;
    let matches;
    let pendingJobs;
    let coverage;
    let epochs;
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
                            keepWaiting = true;
                            coverage = 0;
                            epochs = 0;
                            keyPattern = `MULTIPROC_OUTPUT_${targetPrefix}*`;
                            while (keepWaiting){
                                matches = await scanner.keys(keyPattern);
                                pendingJobs = Boolean(matches.length < job.max_jobs);
                                coverage = matches.length/job.max_jobs;
                                epochs++;
                                if (!pendingJobs) keepWaiting = false;
                                if (coverage >= ACCEPTABLE_CUTOFF && epochs > BREAK_OUT_EPOCHS) keepWaiting = false;
                                await sleep(SCAN_INTERVAL);
                            }
                            await notify(signal, jobKey, job);
                            scanner.tearDown("proc", targetPrefix, runner, callable)
                        }
                        await notify(signal, jobKey, job);
                    }
                } catch(err){}
                await scanner.del(jobKey);
            }
        }
        await sleep(SCAN_INTERVAL);
    }
}

scan()
