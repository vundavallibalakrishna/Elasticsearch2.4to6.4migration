const fs = require('fs');
var rp = require('request-promise');
var obj = {
    data: []
}
var scrollId = null;

var query = {
    "_source": {
        "include": ["clientJobCode", "jobClientRecruiterName", "jobClientRecruiterEmail", "source", "stageDate", "submittedOn", "status", "mappedJobTitle", "currentJobTitle", "currentEmployer", "applicationStage", "applicationState", "jobTags"]
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
                            { "missing": { "field": "isJobCandidateMapping" } },
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


function loadData() {

    if (scrollId == null) {
        sendData(query, "http://192.31.2.61:9200/jobcandidateinteraction/jobcandidateinteraction/_search?scroll=1m", "POST").then(function (response) {
            scrollId = response["_scroll_id"];
            if (response["hits"]["hits"].length > 0) {
                response["hits"]["hits"].forEach(function (hit) {
                    obj.data.push(hit);
                });
                loadData();
            } else {
                flush();
            }
        })
    } else {
        var scrollQuery = {
            "scroll": "1m",
            "scroll_id": scrollId
        }
        sendData(scrollQuery, "http://192.31.2.61:9200/jobcandidateinteraction/jobcandidateinteraction/scroll", "POST").then(function (response) {
            scrollId = response["_scroll_id"];
            if (response["hits"]["hits"].length > 0) {
                response["hits"]["hits"].forEach(function (hit) {
                    obj.data.push(hit);
                });
                loadData();
            } else {
                flush();
            }
        })
    }
}

function flush() {
    fs.writeFileSync('results-' + (new Date()).getTime() + '.json', JSON.stringify(obj, null, 2), 'utf-8');
}

loadData();

