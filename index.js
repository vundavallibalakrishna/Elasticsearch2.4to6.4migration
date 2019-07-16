const fs = require('fs');
var rp = require('request-promise');
var candidateObj = {
    data: []
}
var jobObj = {
    data: []
}
var recruiterObj = {
    data: []
}
var scrollId_cand = null;

var scrollId_job = null;

var scrollId_recruter = null;

const IP = "localhost:9200"

var candidateQuery = {
    "size": 1000,
    "_source": {
        "exclude": ["recruiterId", "name", "email", "alternateEmails", "phoneNumber", "alternatePhoneNumbers", "candidateResume.*", "otherDocuments.*", "rtrFileOriginalName", "rtrFileLocation", "createdBy", "modifiedBy", "messengers.*", "linkedInURL", "jobAccountManagerEmail", "jobClientRecruiterEmail", "jobPostedByTeam.ownerEmail", "jobPostedByRecruiterEmail", "jobWorkAssignment.*", "team.ownerEmail", "contactActionsObjs.createdByEmail", "contactActionsObjs.result.createdByEmail", "candidateTasks.*", "mappedCandidateResumes.*", "profileURLs"],
    },
    "query": {
        "bool": {
            "must": [
                {
                    "range": {
                        "createdOn": {
                            "gte": "20/06/2018",
                            "format": "dd/MM/yyyy"
                        }
                    }
                },
                {
                    "bool": {
                        "should": [
                            {
                                "bool": {
                                    "must_not": {
                                        "exists": {
                                            "field": "isJobCandidateMapping"
                                        }
                                    }
                                }
                            },
                            { "match": { "isJobCandidateMapping": false } }
                        ]
                    }
                }
            ]
        }
    },
    "sort": [
        { "createdOn": { "order": "desc" } }
    ]
}

var jobQuery = {
    "size": 1000,
    "_source": {
        "exclude": ["recruiterId", "createdBy", "modifiedBy", "clientRecruiter", "accountManagerEmail", "team.ownerEmail", "latestSubmission", "firstSubmission", "submissionsList"],
    },
    "query": {
        "bool": {
            "must": [
                {
                    "range": {
                        "createdOn": {
                            "gte": "20/06/2018",
                            "format": "dd/MM/yyyy"
                        }
                    }
                }
            ]
        }
    },
    "sort": [
        { "createdOn": { "order": "desc" } }
    ]
}

var recruiterQuery = {
    "size": 1000,
    "_source": {
        "exclude": ["email", "alternateEmails", "phoneNumber", "alternatePhoneNumbers", "createdBy", "modifiedBy", "workAssignments", "ownsTeams", "belongsToTeams", "removedFromTeams", "oldEmails"],
    },
    "query": {
        "bool": {
            "must": [
                {
                    "range": {
                        "createdOn": {
                            "gte": "20/06/2018",
                            "format": "dd/MM/yyyy"
                        }
                    }
                }
            ]
        }
    },
    "sort": [
        { "createdOn": { "order": "desc" } }
    ]
}

function sendData(obj, command, method, qs) {
    var options = {
        method: method,
        uri: command,
        body: obj ? obj : {},
        qs: qs,
        json: true
    };
    return rp(options);

}


function loadCandidateData() {
    if (scrollId_cand == null) {
        sendData(candidateQuery, "http://" + IP + "/jobcandidateinteraction/jobcandidateinteraction/_search?scroll=1m", "POST").then(function (response) {
            scrollId_cand = response["_scroll_id"];
            if (response["hits"]["hits"].length > 0) {
                response["hits"]["hits"].forEach(function (hit) {
                    candidateObj.data.push(hit);
                });
                flushCandidate();
                loadCandidateData();
            } else {
                //flushCandidate();
            }
        })
    } else {
        var scrollQuery = {
            "scroll": "1m",
            "scroll_id": scrollId_cand
        }
        sendData(scrollQuery, "http://" + IP + "/_search/scroll", "POST").then(function (response) {
            scrollId_cand = response["_scroll_id"];
            if (response["hits"] && response["hits"]["hits"] && response["hits"]["hits"].length > 0) {
                response["hits"]["hits"].forEach(function (hit) {
                    candidateObj.data.push(hit);
                });
                flushCandidate();
                loadCandidateData();
            } else {
                //flushCandidate();
            }
        })
    }
}

function loadJobData() {
    if (scrollId_job == null) {
        sendData(jobQuery, "http://" + IP + "/job/job/_search?scroll=1m", "POST").then(function (response) {
            scrollId_job = response["_scroll_id"];
            if (response["hits"]["hits"].length > 0) {
                response["hits"]["hits"].forEach(function (hit) {
                    jobObj.data.push(hit);
                });
                loadJobData();
            } else {
                flushJob();
            }
        })
    } else {
        var scrollQuery = {
            "scroll": "1m",
            "scroll_id": scrollId_job
        }
        sendData(scrollQuery, "http://" + IP + "/_search/scroll", "POST").then(function (response) {
            scrollId_job = response["_scroll_id"];
            if (response["hits"] && response["hits"]["hits"] && response["hits"]["hits"].length > 0) {
                response["hits"]["hits"].forEach(function (hit) {
                    jobObj.data.push(hit);
                });
                loadJobData();
            } else {
                flushJob();
            }
        })
    }
}

function loadRecruiterData() {
    if (scrollId_recruter == null) {
        sendData(recruiterQuery, "http://192.31.2.61:9200/recruiter/recruiter/_search?scroll=1m", "POST").then(function (response) {
            scrollId_recruter = response["_scroll_id"];
            if (response["hits"]["hits"].length > 0) {
                response["hits"]["hits"].forEach(function (hit) {
                    recruiterObj.data.push(hit);
                });
                loadRecruiterData();
            } else {
                flushRecruiter();
            }
        })
    } else {
        var scrollQuery = {
            "scroll": "1m",
            "scroll_id": scrollId_recruter
        }
        sendData(scrollQuery, "http://192.31.2.61:9200/_search/scroll", "POST").then(function (response) {
            scrollId_recruter = response["_scroll_id"];
            if (response["hits"] && response["hits"]["hits"] && response["hits"]["hits"].length > 0) {
                response["hits"]["hits"].forEach(function (hit) {
                    recruiterObj.data.push(hit);
                });
                loadRecruiterData();
            } else {
                flushRecruiter();
            }
        })
    }
}

function mask(myemailId) {
    var maskid = "";
    var prefix = myemailId.substring(0, myemailId.lastIndexOf("@"));
    var postfix = myemailId.substring(myemailId.lastIndexOf("@"));

    for (var i = 0; i < prefix.length; i++) {
        if (i == 0 || i == prefix.length - 1) {   ////////
            maskid = maskid + prefix[i].toString();
        }
        else {
            maskid = maskid + "*";
        }
    }
    return maskid + postfix;
}

function flushCandidate() {
    fs.writeFileSync('results-candidate-' + (new Date()).getTime() + '.json', JSON.stringify(candidateObj, null, 2), 'utf-8');
    candidateObj = {
        data: []
    }
}

function flushJob() {
    fs.writeFileSync('results-job-' + (new Date()).getTime() + '.json', JSON.stringify(jobObj, null, 2), 'utf-8');
}

function flushRecruiter() {
    fs.writeFileSync('results-recruiter-' + (new Date()).getTime() + '.json', JSON.stringify(recruiterObj, null, 2), 'utf-8');
}

loadCandidateData();
//loadJobData();
//loadRecruiterData();

