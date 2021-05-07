/*
    Multiprocessing Server
    Version: 0.1.0
*/

const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const compression = require('compression');
const {
    StreamJob, 
    SpinUpWorker,
    TearDownWorker,
    StreamAnalytics,
    masterNode
} = require('./controllers/apiControllers');
const {
    MULTIPROC_SERVER_PORT
} = require('./utils/config.json');

//

const app = express();
app.use(cors())
app.options('*', cors())
app.use(bodyParser.json({ limit: '2gb'  }));
app.use(bodyParser.urlencoded({ extended: true, limit: '2gb'  }));
app.use(compression())
app.use((error, req, res, next) => {
    if (error instanceof SyntaxError) {
        res.status(400);
        res.json({ code: 400, message: "Malformed request. Check and try again." });
    } else {
        res.status(400);
        res.json({ code: 400, message: "There was a problem with your request." });
        next();
    }
});

const APIRequestFlow = async (res, params) => {
    let {
        controller,
        controllerParams,
    } = params;
    let [code, message, data] = await controller(controllerParams);
    res.status(code).json({
        code,
        message,
        data
    });
};

// Routes

app.post('/multiproc/api/v1/StreamJob', async (req, res, next) => {
    let {
        targetPrefix, 
        jobId, 
        job
    } = req.body;
    await APIRequestFlow(res, {
        controller : StreamJob,
        controllerParams : [
            targetPrefix,
            jobId,
            job
        ]
    });
});

app.put('/multiproc/api/v1/SpinUpWorker', async (req, res, next) => {
    let {
        agentType, 
        targetPrefix, 
        runner,
        callable
    } = req.query;
    await APIRequestFlow(res, {
        controller : SpinUpWorker,
        controllerParams : [
            agentType, 
            targetPrefix, 
            runner,
            callable
        ]
    });
});

app.put('/multiproc/api/v1/TearDownWorker', async (req, res, next) => {
    let {
        agentType, 
        targetPrefix, 
        runner,
        callable
    } = req.query;
    await APIRequestFlow(res, {
        controller : TearDownWorker,
        controllerParams : [
            agentType, 
            targetPrefix, 
            runner,
            callable
        ]
    });
});

app.get('/multiproc/api/v1/StreamAnalytics', async (req, res, next) => {
    await APIRequestFlow(res, {
        controller : StreamAnalytics,
        controllerParams : []
    });
});

app.put('/multiproc/api/v1/masterNode/:command/:key', async (req, res, next) => {
    let { command, key } = req.params;
    let { dataObject } = req.body;
    let [code, message, data] = [
        400, 
        `Failure: command: ${command} could not be performed on key: ${key}`, 
        { you_sent : {
            command,
            key,
            dataObject
        } }
    ];
    let supportedCommands = ["get", "set", "del", "keys"];
    if (supportedCommands.indexOf(command) !== -1){
        try {
            if (command === "get"){
                data = JSON.parse(await masterNode.get(key));
            }
            if (command === "set"){
                await masterNode.set(key, JSON.stringify(dataObject));
                data = dataObject;
            }
            if (command === "keys"){
                data = await masterNode.keys(key);
            }
            if (command === "del"){
                await masterNode.del(key);
            }
            code = 200
            message = "Success: some contextual information may be attached"
        } catch(err){}
    }
    res.status(code).json({
        code,
        message,
        data
    });
});

const PORT = process.env.PORT || MULTIPROC_SERVER_PORT;
const http = require('http').Server(app);
http.listen(PORT, () => console.log(`Multiprocessing Server is running on port ${PORT}`));