var async = require('async');
var AWS = require('aws-sdk');
var csv = require('csv-parser');
var request = require('request');
var Readable = require('stream').Readable

var s3 = new AWS.S3();
var bufferStream = new Readable;
var elasticsearch = 'http://search-qinsights-aabzxfibr4meoriwcbjzo62oai.us-east-1.es.amazonaws.com';
var indexname = process.env.indexname || "bancolombia-csvbeat-transactions";
var AWS_CLIENT_ID = process.env.AWS_ACCESS_KEY_ID;
var AWS_CLIENT_SECRET = process.env.AWS_SECRET_ACCESS_KEY;
var AWS_SESSION_TOKEN = process.env.AWS_SESSION_TOKEN
exports.handler = (event, context, callback) => {


    var dt = new Date();
    var ts = dt.getFullYear() + "-" + (dt.getMonth() + 1) + "-" + dt.getDate() + " " + dt.getHours() + ":" + dt.getMinutes() + ":" + dt.getSeconds();
    var srcBucket = event["Records"][0].s3.bucket.name;
    console.log("Src bucket:" + srcBucket);
    var srcKey = decodeURIComponent(event["Records"][0].s3.object.key.replace(/\+/g, " "));
    console.log("Src key:" + srcKey);
    var dstBucket = srcBucket;
    console.log("Dest bucket:" + dstBucket);
    var dstKey = "processed/" + ts + "-" + srcKey;
    console.log("Dest key:" + dstKey);

    async.waterfall([
        function download(next) {
            // Download the image from S3 into a buffer.
            s3.getObject({
                Bucket: srcBucket,
                Key: srcKey
            }, next);
        },
        function transform(response, next) {
            next(null, response.ContentType, response.Body)
        },
        function upload(contentType, data, next) {
            // Stream the transformed image to a different S3 bucket.
            s3.putObject({
                Bucket: dstBucket,
                Key: dstKey,
                Body: data,
                ContentType: contentType
            }, function (error, s3data) {
                if (error) {
                    next(error);
                } else {
                    next(null, data);
                }
            });
        },
        function processData(filedata, next) {
            bufferStream.push(filedata);
            bufferStream.push(null);
            var body = [];
            var type = process.env.indextype || "transaction";
            bufferStream
                .pipe(csv(["domain", "host", "process", "agentname", "resource", "metricname", "correctedvalue"]))
                .on('data', function (data) {
                    for (i in data) {
                        data[i] = data[i].trim();
                    }
                    data["filename"] = dstKey;
                    var domainpart = data["domain"].split(" ");
                    var domaindate = domainpart[0].split("/");
                    var domaintime = domainpart[1].split(":");
                    var dt = new Date(domaindate[2], (parseInt(domaindate[0]) - 1), domaindate[1], domaintime[0], domaintime[1], domaintime[2], 0);
                    data["@timestamp"] = dt.toISOString();
                    data["correctedvalue"] = parseFloat(data["correctedvalue"]);
                    body.push(JSON.stringify({ "index": { "_index": indexname, "_type": type } }));
                    body.push(JSON.stringify(data));
                }).on('end', function () {
                    var options = {
                        url: elasticsearch + "/_bulk",
                        body: body.join("\n\r") + "\n\r",
                        headers: {
                            "Content-Type": "application/x-ndjson"
                        },
                        aws: {
                            key: AWS_CLIENT_ID,
                            secret: AWS_CLIENT_SECRET,
                            session: AWS_SESSION_TOKEN,
                            sign_version: 4
                        }
                    }
                    request.post(options, function (error, response, body) {
                        next(error, body);
                    });
                });
        }
    ], function (err, result) {
        if (err) {
            console.error(
                'Unable to copy file to new destination due to error:' + err
            );
        } else {
            console.log(
                'Successfully copied file to destination and processed' + JSON.stringify(result)
            );
        }

        callback(null, "message");
    });
};
