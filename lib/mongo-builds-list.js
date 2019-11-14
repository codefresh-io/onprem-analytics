'use strict';

const MongoClient = require('mongodb').MongoClient;
const ObjectId = require('mongodb').ObjectId;
const Promise     = require('bluebird');
const _ = require('lodash');
const {table} = require('table');
const { Parser } = require('json2csv');
const fs = require('fs');
const path = require('path');

//TODO
// 0. validate numbers are not good for hover
// 1. fix sorting of different level
// 2. create a dhasboard!
// 3. create another data set for current usage

const PACK_ID = {
    '5cd1746617313f468d669013': 'Small',
    '5cd1746717313f468d669014': 'Medium',
    '5cd1746817313f468d669015': 'Large',
};

_.insertSortedBy = (a, v, i) => a.splice(_.sortedIndexBy(a, v, i), 0, v);

let db;

const main = async () => {

    // Connection url
    const url = process.env.MONGO_URI || 'mongodb://localhost';
    // Connect using MongoClient
    const adminDb        = await MongoClient.connect(url);
    db = adminDb.db(process.env.API_DB || "local");

    try {
        console.log('Running builds summary\n');
        await exportDocs();
    } catch (err) {
        console.error(err.stack);
    }

    await adminDb.close();
};


const _getDelayedBuilds = (builds) => {
    return _.filter(builds, (build) => {
        return build.pendingLicense;
    });
};


const _getTotalElectionToRunningTime = (builds) => {
  return _millisToMinutes(_.reduce(builds, (totalRunningMinutes, build) => {
     if (!build.started || !build.electionDate) {
         return totalRunningMinutes;
     }

     return totalRunningMinutes + (build.started - build.electionDate);
  }, 0));
};

const _getRunningMinutes = (builds) => {
    return _millisToMinutes(_.reduce(builds, (totalRunningMinutes, build) => {
        if (!build.started || !build.finishedForParallel) {
            return totalRunningMinutes;
        }

        return totalRunningMinutes + (build.finishedForParallel - build.started);
    }, 0));
};

const _getDelayedMinutes = (builds) => {
    const delayedBuilds = _getDelayedBuilds(builds);
    return _millisToMinutes(_.reduce(delayedBuilds, (totalDelayedMinutes, build) => {
        if (!build.electionDate) {
            return totalDelayedMinutes;
        }

        return totalDelayedMinutes + (build.electionDate - build.created);
    }, 0));
};

const _getAllBuilds = async () => {
    const col       = db.collection('workflowprocesses');
    const FROM_DATE = process.env.FROM_DATE || '2019-01-01';
    const TO_DATE = process.env.TO_DATE || '';

    const cursor    = col.find(
        {
            "_id": { $gte: ObjectId(Math.floor((new Date(`${FROM_DATE}T00:00:00.000+0000`).getTime()) / 1000).toString(16) + "0000000000000000"), ...(TO_DATE && {$lte: ObjectId(Math.floor((new Date(`${TO_DATE}T00:00:00.000+0000`).getTime()) / 1000).toString(16) + "0000000000000000")}) }, ...(process.env.ACCOUNT_ID &&
                { "account": ObjectId(process.env.ACCOUNT_ID) })
        },
        {
            _id: 1,
            created: 1,
            started: 1,
            finished: 1,
            pipeline: 1,
            "scmMetadata.userName": 1,
            user: 1,
            webhookTriggered: 1,
            electionDate: 1,
            startImmediately: 1,
            terminationRequest: {$slice: 1},
            account: 1,
            pendingLicense: 1,
            status: 1,
            trigger: 1,
            "runtimeEnvironmentMetadata.name": 1,
            packId: 1
        }
    );

    return await cursor.toArray();
};

const _splitBuildsByAccount = (builds, events = []) => {
    const buildsPerAccount = _.groupBy(builds, (build) => {
        return build.account.toString();
    });

    const eventsPerAccount = _.groupBy(events, (event) => {
        return event.account.toString();
    });

    return {buildsPerAccount, eventsPerAccount};
};

const _millisToMinutes = (millisec) => {
    var seconds = (millisec / 1000).toFixed(1);

    var minutes = (millisec / (1000 * 60)).toFixed(1);

    var hours = (millisec / (1000 * 60 * 60)).toFixed(1);

    var days = (millisec / (1000 * 60 * 60 * 24)).toFixed(1);

    if (seconds < 60) {
        return seconds + " Sec";
    } else if (minutes < 60) {
        return minutes + " Min";
    } else if (hours < 24) {
        return hours + " Hrs";
    } else {
        return days + " Days"
    }
};

const _calculateTable = (builds) => {
    const data = [];

    _.forEach(builds, (build) => {
        const totalElectionToRunningTime = _getTotalElectionToRunningTime([build]);
        data.push({'Id': build._id, 'Status': build.status, 'Created': new Date(build.created), 'Elected': build.electionDate ? new Date(build.electionDate): '', 'Started': build.started ? new Date(build.started) : '', 'Finished': new Date(build.finished), 'Pipeline': build.pipeline , 'User': build.webhookTriggered ? '' : build.user, 'Committer': _.get(build, 'scmMetadata.userName'), 'Manual': !build.webhookTriggered, 'Total Election to Running Time': totalElectionToRunningTime ? totalElectionToRunningTime : '', 'Total Running Time': _getRunningMinutes([build]), 'Total Time Spent In Delayed': _getDelayedMinutes([build])});
    });


    return data;
};

const _calculateFinalFields = (builds) => {
    _.forEach(builds, (build) => {
        if (!_.isArray(build.terminationRequest) && build.terminationRequest) {
            build.terminationRequest = [build.terminationRequest];
        }

        const finishedForParallel = _.get(build, 'terminationRequest.0.date', build.finished);
        if (finishedForParallel) {
            build.finishedForParallel = finishedForParallel.getTime();
        }

        if (build.finished) {
            build.finished = build.finished.getTime();
        }

        if (build.started) {
            build.started = build.started.getTime();
        }

        if (build.created) {
            build.originalCreated = build.created;
            build.created = build.created.getTime();
        }

        if (build.electionDate) {
            build.electionDate = build.electionDate.getTime();
        }

        if (build.packId) {
           build.packId = PACK_ID[build.packId] || build.packId;
        }

        build.pricing = build.packId ? 'new' : 'old';
    });
};

const fields = ['Id', 'Created', 'Elected', 'Started', 'Finished', 'Status', 'Pipeline', 'User', 'Committer', 'Manual', 'Total Election to Running Time', 'Total Running Time', 'Total Time Spent In Delayed'];
const csvs = [];

const _printTable = (title, data) => {

    const finalTable = [fields];
    _.forEach(data, (d) => {
        finalTable.push(_.map(fields, (f) => {
            return d[f];
        }));
    });

    console.log(title);
    console.log(table(finalTable));
};

const _extendCSV = (title, data) => {
    const opts = { fields };

    try {
        const parser = new Parser(opts);
        const csv = parser.parse(data);
        csvs.push({title, csv});
    } catch (err) {
        console.error(err);
    }
};

const _exportCSV = () => {
    console.log(csvs[0].csv);
    _.forEach(csvs, (csv) => {
        fs.writeFileSync(path.resolve('./', `${csv.title}.csv`), csv.csv);
    });
};

const exportDocs = async () => {
    const builds = await _getAllBuilds();

    console.log('builds loaded');

    _calculateFinalFields(builds);

    const {buildsPerAccount, eventsPerAccount} = _splitBuildsByAccount(builds);

    _.forEach(buildsPerAccount, (accountBuilds, accountId) => {
        const title = `Summary for account: ${accountId}`;
        const data = _calculateTable(accountBuilds, eventsPerAccount[accountId]);
        //_printTable(title, data);
        _extendCSV(title, data);
    });

    const title = `System Summary`;
    const data = _calculateTable(builds);
    //_printTable(title, data);
    _extendCSV(title, data);

    _exportCSV();
};





Promise.resolve()
    .then(async () => {
        await main();
    });
