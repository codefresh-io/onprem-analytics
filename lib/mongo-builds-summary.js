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

    await db.close();
};


const _getDelayedBuildsCount = (builds) => {
    const delayedBuilds = _.countBy(builds, (build) => {
        return build.pendingLicense;
    });
    return delayedBuilds.true || 0;
};

const _getStatusCount = (builds) => {
    const totals = _.countBy(builds, (build) => {
        return build.status;
    });
    return JSON.stringify(totals, 2, 4);
};

const _getOperationCount = (builds) => {
    const totals = _.countBy(builds, (build) => {
        return build.trigger;
    });
    delete totals.undefined;
    return JSON.stringify(totals, 2, 4);
};

const _getTerminationCount = (builds) => {
    const totals = _.countBy(builds, (build) => {
        return _.get(build, 'terminationRequest.0.cause');
    });
    delete totals.undefined;
    return JSON.stringify(totals, 2, 4);
};

const _getRuntimeCount = (builds) => {
    const totals = _.countBy(builds, (build) => {
        return _.get(build, 'runtimeEnvironmentMetadata.name');
    });
    delete totals.undefined;
    return JSON.stringify(totals, 2, 4);
};

const _getMaxParallelDelayedCount = (events) => {
    let maxDelayed = 0;

    let currentDelayed = 0;
    _.forEach(events, (event) => {
        if (event.type === 'delayed') {
            currentDelayed++;
        }

        if (event.type === 'pending') {
            currentDelayed--;
        }

        if (currentDelayed > maxDelayed) {
            maxDelayed = currentDelayed;
        }
    });

    return maxDelayed;
};

const _getMaxParallelRunningCount = (events) => {
    let maxRunning = 0;
    let maxBuildIds = {};

    let currentRunning = 0;
    let currentBuildIds = {};
    _.forEach(events, (event) => {
        if (event.type === 'elected') {
            currentRunning++;
            if (process.env.PRINT_MAX_DELAYED_BUILDS) {
                currentBuildIds[event.buildId.toString()] = event.buildId.toString();
            }
        }

        if (event.type === 'finished') {
            currentRunning--;
            if (process.env.PRINT_MAX_DELAYED_BUILDS) {
                delete currentBuildIds[event.buildId.toString()];
            }
        }

        if (currentRunning > maxRunning) {
            maxRunning = currentRunning;
            if (process.env.PRINT_MAX_DELAYED_BUILDS) {
                maxBuildIds = _.clone(currentBuildIds);
            }
        }
    });

    return `${maxRunning} ${process.env.PRINT_MAX_DELAYED_BUILDS ? JSON.stringify(_.map(maxBuildIds, id => id), 4, 2) : ''}`;
};

const _getDelayedBuilds = (builds) => {
    return _.filter(builds, (build) => {
        return build.pendingLicense;
    });
};


const _getRunningMinutes = (builds) => {
  return _millisToMinutes(_.reduce(builds, (totalRunningMinutes, build) => {
     if (!build.started || !build.finishedForParallel) {
         return totalRunningMinutes;
     }

     return totalRunningMinutes + (build.finishedForParallel - build.started);
  }, 0));
};

const _getTotalElectionToRunningAVGTime = (builds) => {
    let totalValidBuilds = 0;
    const totalTime = _.reduce(builds, (totalRunningMinutes, build) => {
        if (!build.started || !build.electionDate) {
            return totalRunningMinutes;
        }

        totalValidBuilds++;
        return totalRunningMinutes + (build.started - build.electionDate);
    }, 0);

    return _millisToMinutes(totalTime / totalValidBuilds);
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

const _getBuildsCount = (builds) => {
    return builds.length;
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

const _splitBuildsByAccount = (builds, events) => {
    const buildsPerAccount = _.groupBy(builds, (build) => {
        return build.account.toString();
    });

    const eventsPerAccount = _.groupBy(events, (event) => {
        return event.account.toString();
    });

    return {buildsPerAccount, eventsPerAccount};
};

const _splitBuildsByMonth = (builds, events) => {
    const buildsPerMonth = _.groupBy(builds, (build) => {
        return `${build.originalCreated.toString().substr(11, 4)}:${build.originalCreated.toString().substr(4, 3)}`;
    });

    const eventsPerMonth = _.groupBy(events, (event) => {
        const originalEventDate = new Date(event.eventDate);
        return `${originalEventDate.toString().substr(11, 4)}:${originalEventDate.toString().substr(4, 3)}`;
    });

    return {buildsPerMonth, eventsPerMonth};
};

const _splitBuildsByPricingType = (builds, events) => {
    const buildsPerPricingType = _.groupBy(builds, (build) => {
        return build.pricing;
    });

    const eventsPerPricingType = _.groupBy(events, (event) => {
        return event.pricing;
    });

    return {buildsPerPricingType, eventsPerPricingType};
};

const _splitBuildsByPricingPack = (builds, events) => {
    const buildPerPack = _.groupBy(builds, (build) => {
        return build.packId;
    });

    const eventsPerPack = _.groupBy(events, (event) => {
        return event.packId;
    });

    return {buildPerPack, eventsPerPack};
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

const _calculateTable = (builds, events) => {
    const data = [];

    const {buildsPerMonth, eventsPerMonth} = _splitBuildsByMonth(builds, events);

    const previousRowsAdded = finalRowsAdded;
    _.forEach(buildsPerMonth, (monthBuilds, month) => {
        const previousRowsAdded = finalRowsAdded;
        const monthEvents = eventsPerMonth[month] || [];
        const {buildsPerPricingType, eventsPerPricingType} = _splitBuildsByPricingType(monthBuilds, monthEvents);
        _.forEach(buildsPerPricingType, (pricingBuilds, pricing) => {
            const pricingEvents = eventsPerPricingType[pricing] || [];
            const previousRowsAdded = finalRowsAdded;
            if (pricing === 'new') {
                const {buildPerPack, eventsPerPack} = _splitBuildsByPricingPack(pricingBuilds, pricingEvents);
                _.forEach(buildPerPack, (packBuilds, pack) => {
                    const packEvents = eventsPerPack[pack] || [];
                    data.push({'Title': `Month: ${month}. Pricing-${pricing}. Pack: ${pack} Total`, 'Builds': _getBuildsCount(packBuilds), 'Operation': _getOperationCount(packBuilds), 'Status': _getStatusCount(packBuilds), 'Termination Reasons': _getTerminationCount(packBuilds), 'Runtime': _getRuntimeCount(packBuilds), 'Max Parallel Running': _getMaxParallelRunningCount(packEvents), 'Delayed': _getDelayedBuildsCount(packBuilds), 'Max Parallel Delayed': _getMaxParallelDelayedCount(packEvents), 'Total Running Time': _getRunningMinutes(packBuilds), 'Total Time Spent In Delayed': _getDelayedMinutes(packBuilds), 'Total Election to Running Average Time': _getTotalElectionToRunningAVGTime(packBuilds)});
                    finalRowsAdded++;
                });
            }

            if (pricing === 'old' || finalRowsAdded > previousRowsAdded + 1) {
                data.push({'Title': `Month: ${month}. Pricing-${pricing} Total`, 'Builds': _getBuildsCount(pricingBuilds), 'Operation': _getOperationCount(pricingBuilds), 'Status': _getStatusCount(pricingBuilds), 'Termination Reasons': _getTerminationCount(pricingBuilds), 'Runtime': _getRuntimeCount(pricingBuilds), 'Max Parallel Running': _getMaxParallelRunningCount(pricingEvents), 'Delayed': _getDelayedBuildsCount(pricingBuilds), 'Max Parallel Delayed': _getMaxParallelDelayedCount(pricingEvents), 'Total Running Time': _getRunningMinutes(pricingBuilds), 'Total Time Spent In Delayed': _getDelayedMinutes(pricingBuilds), 'Total Election to Running Average Time': _getTotalElectionToRunningAVGTime(pricingBuilds)});
                finalRowsAdded++;
            }
        });

        if (finalRowsAdded > previousRowsAdded + 1) {
            data.push({'Title': `Month: ${month} Total`, 'Builds': _getBuildsCount(monthBuilds), 'Operation': _getOperationCount(monthBuilds), 'Status': _getStatusCount(monthBuilds), 'Termination Reasons': _getTerminationCount(monthBuilds), 'Runtime': _getRuntimeCount(monthBuilds), 'Max Parallel Running': _getMaxParallelRunningCount(monthEvents), 'Delayed': _getDelayedBuildsCount(monthBuilds), 'Max Parallel Delayed': _getMaxParallelDelayedCount(monthEvents), 'Total Running Time': _getRunningMinutes(monthBuilds), 'Total Time Spent In Delayed': _getDelayedMinutes(monthBuilds), 'Total Election to Running Average Time': _getTotalElectionToRunningAVGTime(monthBuilds)});
            finalRowsAdded++;
        }

    });

    if (finalRowsAdded > previousRowsAdded + 1){
        data.push({'Title': `Total`, 'Builds': _getBuildsCount(builds), 'Operation': _getOperationCount(builds), 'Status': _getStatusCount(builds), 'Termination Reasons': _getTerminationCount(builds), 'Runtime': _getRuntimeCount(builds), 'Max Parallel Running': _getMaxParallelRunningCount(events), 'Delayed': _getDelayedBuildsCount(builds), 'Max Parallel Delayed': _getMaxParallelDelayedCount(events), 'Total Running Time': _getRunningMinutes(builds), 'Total Time Spent In Delayed': _getDelayedMinutes(builds), 'Total Election to Running Average Time': _getTotalElectionToRunningAVGTime(builds)});
        finalRowsAdded++;
    }

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

const _createEvents = (builds) => {
    const events = [];
    _.forEach(builds, (build) => {
        if (build.started && build.finishedForParallel && !build.startImmediately) {
            if (build.pendingLicense) {
                _.insertSortedBy(events, {
                    type: 'delayed',
                    buildId: build._id,
                    eventDate: build.created,
                    account: build.account,
                    pricing: build.pricing,
                    packId: build.packId
                }, 'eventDate');

                _.insertSortedBy(events, {
                    type: 'pending',
                    buildId: build._id,
                    eventDate: build.electionDate,
                    account: build.account,
                    pricing: build.pricing,
                    packId: build.packId
                }, 'eventDate');
            }

            _.insertSortedBy(events, {
                type: 'elected',
                buildId: build._id,
                eventDate: build.electionDate,
                account: build.account,
                pricing: build.pricing,
                packId: build.packId
            }, 'eventDate');

            _.insertSortedBy(events, {
                type: 'started',
                buildId: build._id,
                eventDate: build.started,
                account: build.account,
                pricing: build.pricing,
                packId: build.packId
            }, 'eventDate');

            _.insertSortedBy(events, {
                type: 'finished',
                buildId: build._id,
                eventDate: build.finishedForParallel,
                account: build.account,
                pricing: build.pricing,
                packId: build.packId
            }, 'eventDate');
        }
    });
    return events;
};

let finalRowsAdded = 0;
const fields = ['Title', 'Builds', 'Operation', 'Status', 'Termination Reasons', 'Runtime', 'Max Parallel Running', 'Delayed', 'Max Parallel Delayed', 'Total Running Time', 'Total Time Spent In Delayed', 'Total Election to Running Average Time'];
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

    const events = _createEvents(builds);

    // print all builds for debug
    /*_.forEach(builds, (build) => {
        console.log(`build: ${build._id.toString()}, created: ${build.created}, started: ${build.started}, elected: ${build.electionDate}, finished: ${build.finishedForParallel}, original finished: ${build.finished}, termination: ${_.get(build, 'terminationRequest.0.date')}`)
    });*/

    const {buildsPerAccount, eventsPerAccount} = _splitBuildsByAccount(builds, events);

    if (!process.env.SYSTEM_OVERVIEW) {
        _.forEach(buildsPerAccount, (accountBuilds, accountId) => {
            const title = `Summaryy for account: ${accountId}`;
            const data = _calculateTable(accountBuilds, eventsPerAccount[accountId]);
            _printTable(title, data);
            _extendCSV(title, data);
        });
    }

    if (_.size(buildsPerAccount) > 1 || process.env.SYSTEM_OVERVIEW) {
        const title = `System Summary`;
        const data = _calculateTable(builds, events);
        _printTable(title, data);
        _extendCSV(title, data);
    }

    _exportCSV();
};





Promise.resolve()
    .then(async () => {
        await main();
    });
