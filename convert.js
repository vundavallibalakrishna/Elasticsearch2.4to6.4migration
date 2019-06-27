const fs = require('fs');
const rp = require('request-promise');

const ES_24_IP = "http://192.31.254.21:9200/";
const ES_64_IP = "http://localhost:9200/";

const migrationsIndex = {
    "list": []
}

function start() {
    if (!fs.existsSync('./5.0-upgraded')) {
        fs.mkdirSync('./5.0-upgraded');
    }
    if (!fs.existsSync('./spring-boot')) {
        fs.mkdirSync('./spring-boot');
    }

    fs.readdir("2.4/", function (err, filenames) {
        for (var i in filenames) {
            console.log("Converting file " + filenames[i]);
            startConversion(filenames[i]);
        }
        createMappings();
    })
}

async function sendData(obj, command, method, qs) {
    var options = {
        method: method,
        timeout: 3600000, // 1 hr.
        uri: command,
        body: obj ? obj : {},
        qs: qs,
        json: true
    };
    return await rp(options);
}


function startConversion(fileName) {
    let json24 = JSON.parse(fs.readFileSync('2.4/' + fileName));
    var indexname24 = Object.keys(json24)[0];
    var actualIndex24 = Object.keys(json24[indexname24]["mappings"]);

    for (var mappings in actualIndex24) {
        if (json24[indexname24]["mappings"][actualIndex24[mappings]].properties) {

            var finalIndexName = actualIndex24[mappings];
            var outputCopy = JSON.parse(fs.readFileSync('2.4/' + fileName));
            if (finalIndexName != indexname24) {
                outputCopy[finalIndexName] = outputCopy[indexname24];
                delete outputCopy[indexname24];
            }
            updateMappings(json24[indexname24]["mappings"][actualIndex24[mappings]].properties);
            outputCopy[finalIndexName]["mappings"] = {};
            outputCopy[finalIndexName]["mappings"][finalIndexName] = {};
            outputCopy[finalIndexName]["mappings"][finalIndexName] = json24[indexname24]["mappings"][actualIndex24[mappings]];
            outputCopy[finalIndexName]["settings"] = json24[indexname24]["settings"].index;
            migrationsIndex.list.push({
                sourceIndex: indexname24,
                sourceType: actualIndex24[mappings],
                destinationIndex: actualIndex24[mappings],
                destinationType: actualIndex24[mappings]
            });
            fs.writeFileSync('./5.0-upgraded/' + indexname24 + "_" + actualIndex24[mappings] + "_" + fileName, JSON.stringify(outputCopy, null, 2), 'utf-8');
            fs.writeFileSync('./spring-boot/' + indexname24 + "_" + actualIndex24[mappings] + "_" + fileName, JSON.stringify(outputCopy[finalIndexName]["mappings"][finalIndexName], null, 2), 'utf-8');
        }
    }
    console.log("Converted file ./5.0-upgraded/" + fileName);



    function updateMappings(properties24) {
        if (properties24) {
            var keys24 = Object.keys(properties24)
            for (var i in keys24) {
                var node = properties24[keys24[i]];
                var multiField = true;
                if (node.properties != null) {
                    updateMappings(node.properties);
                } else {
                    if (node.fields == null) {
                        multiField = false;
                    }
                    if (!multiField) {
                        if (node.type == "string" && node.index == "not_analyzed") {
                            node.type = "keyword";
                        } else if (node.type == "string") {
                            node.type = "text";
                        }
                    } else {
                        if (node.type == "string") {
                            node.type = "text";
                        }
                        var fieldsNames = Object.keys(node.fields);
                        for (var j in fieldsNames) {
                            var field = node.fields[fieldsNames[j]];
                            if (field.type == "string" && field.index == "not_analyzed") {
                                field.type = "keyword";
                            } else if (field.type == "string") {
                                field.type = "text";
                            }
                            delete field.index;
                        }
                    }
                    delete node.index;
                }
            }
        }
    }
}


async function createMappings() {
    var filenames = fs.readdirSync("./5.0-upgraded/");
    for (var i in filenames) {
        let json24 = JSON.parse(fs.readFileSync('./5.0-upgraded/' + filenames[i]));
        var indexname24 = Object.keys(json24)[0];
        delete json24[indexname24].warmers;
        delete json24[indexname24].settings.creation_date;
        delete json24[indexname24].settings.uuid;
        delete json24[indexname24].settings.routing;
        delete json24[indexname24].settings.version;

        delete json24[indexname24].aliases;
        console.log("Creating index " + indexname24);
        try {
            var response = await sendData(json24[indexname24], ES_64_IP + indexname24, "PUT");
            console.log("Created index " + indexname24);
            console.log(response);
        } catch (e) {
            console.log("Unable to create index due to exception ");
            console.log(e);
            break;
        }
    }

    reindexTheData();

}

async function reindexTheData() {
    var step_1 = {
        "persistent": {
            "cluster.routing.allocation.enable": "all"
        }
    }
    var response = await sendData(step_1, ES_24_IP + "_cluster/settings", "PUT");
    console.log(response);
    var response = await sendData(step_1, ES_24_IP + "_cat/recovery", "GET");
    console.log(response);
    for (var i in migrationsIndex.list) {
        var migration = migrationsIndex.list[i];
        console.log("Migrating");
        console.log(migration);
        var migrationStep = {
            "source": {
                "index": migration.sourceIndex,
                "type": migration.sourceType
            },
            "dest": {
                "remote": {
                    "host": ES_64_IP,
                },
                "index": migration.destinationIndex,
                "type": migration.destinationType
            }
        };
        var response = await sendData(migrationStep, ES_24_IP + "_reindex?wait_for_completion=true&pretty=true", "POST");
        console.log(response);
    }
}

start();
//reindexTheData();