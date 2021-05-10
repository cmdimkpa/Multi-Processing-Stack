// API controllers

const {
    coordinator,
    readStore
} = require('../models/redisWorker');

let masterNode = new coordinator();

// 1. StreamJob
// @desc    use this controller to stream jobs into the multiprocessor
// @route   POST /multiproc/api/v1/StreamJob

async function StreamJob(controllerParams){
    let [targetPrefix, jobId, job] = controllerParams;
    let code = 400,
        message = "Bad Request: please check your data (must have `targetPrefix` and `jobId` and the following fields must be present in your job: [`callable_fields`, `job_number`, `max_jobs`])",
        data = { you_sent : {
            targetPrefix,
            jobId,
            job
        } };
    if (targetPrefix && jobId && job.callable_fields && job.job_number && job.max_jobs){
        let streamId = `${targetPrefix}_${jobId}`;
        let job_str = JSON.stringify(job);
        await masterNode.set(streamId, job_str);
        code = 201;
        message = `job #${streamId} has been added to the stream`;
    }
    return [code, message, data]
}

// 2. SpinUpWorker
// @desc    use this controller to spin up a proc or agg worker for your streamed jobs
// @route   PUT /multiproc/api/v1/SpinUpWorker?agentType=x&targetPrefix=x&runner=x&callable=x

async function SpinUpWorker(controllerParams){
    let [agentType, targetPrefix, runner, callable] = controllerParams;
    let code = 400,
        message = "Bad Request: please check your data (must have `agentType`, `targetPrefix`, `runner` and `callable` fields)",
        data = { you_sent : {
            agentType,
            targetPrefix,
            runner,
            callable
        } };
    if (agentType && targetPrefix && runner && callable){
        let pids = masterNode.getPIDs(agentType, targetPrefix, runner, callable);
        if (pids.length > 0){
            code = 403;
            message = `Forbidden Request: there is already 1 ${agentType} worker with PID: ${pids}, assigned to this stream`;
        } else {
            await masterNode.spinUp(agentType, targetPrefix, runner, callable);
            code = 200;
            message = `Success: 1 ${agentType} worker has been allocated to your stream`;
        }
    }
    return [code, message, data]
}

// 3. TearDownWorker
// @desc    use this controller to tear down a proc or agg worker for your streamed jobs
// @route   PUT /multiproc/api/v1/TearDownWorker?agentType=x&targetPrefix=x&runner=x&callable=x

function TearDownWorker(controllerParams){
    let [agentType, targetPrefix, runner, callable] = controllerParams;
    let code = 400,
        message = "Bad Request: please check your data (must have `agentType`, `targetPrefix`, `runner` and `callable` fields)",
        data = { you_sent : {
            agentType,
            targetPrefix,
            runner,
            callable
        } };
    if (agentType && targetPrefix && runner && callable){
        masterNode.tearDown(agentType, targetPrefix, runner, callable);
        code = 200;
        message = `Success: a ${agentType} worker was destroyed`;
    }
    return [code, message, data]
}

// 4. StreamAnalytics
// @desc    use this controller to get the status of running jobs in the stream
// @route   GET /multiproc/api/v1/StreamAnalytics

async function StreamAnalytics(){
    let code = 200,
        message = "Success: the status of running streams is attached";
    
    let all_jobkeys = await masterNode.keys("MULTIPROC_PROCESSED_*");
    let report = JSON.parse(await masterNode.get("STREAM_ANALYTICS") || '{}');
    let max_jobs = {};
    for (let jobKey of all_jobkeys){
        let target = jobKey.split("MULTIPROC_PROCESSED_")[1].split("_")[0];
        if (target in report){
            report[target].count++
            report[target].percent_done = +(100*(report[target].count/max_jobs[target])).toFixed(2)
            report[target].last_checked = Date.now()
        } else {
            try {
                let sampleJob = JSON.parse(await masterNode.get(jobKey));
                max_jobs[target] = sampleJob.max_jobs;
                report[target] = {
                    count : 1,
                    percent_done : +(100*(1/max_jobs[target])).toFixed(2),
                    last_checked : Date.now()
                }
            } catch(err){}
        }
    }
    await masterNode.set("STREAM_ANALYTICS", JSON.stringify(report))
    return [code, message, report]
}


module.exports = {
    StreamJob,
    SpinUpWorker,
    TearDownWorker,
    StreamAnalytics,
    masterNode,
    readStore
}
