// dependencies
const AWS = require('aws-sdk');
const csv = require('csvtojson');
const s3 = new AWS.S3();
const { v4: uuidv4 } = require('uuid');
const dynamodb = new AWS.DynamoDB();
const sns = new AWS.SNS();

exports.handler = async (event, context, callback) => {
    const srcBucket = event.Records[0].s3.bucket.name;
    const srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
    const typeMatch = srcKey.match(/\.([^.]*)$/);
    const fileType = typeMatch[1].toLowerCase();
    if (!typeMatch) {
        console.log("Could not determine the csv type.");
        return;
    }
    if (fileType != "csv") {
        console.log(`Only supported  csv type`);
        return;
    }
    const params = {
        Bucket: srcBucket,
        Key: srcKey
    };

    var jsonLines = [];

    s3.getObject(params, function (err, data) {
        if (err)
            return err;
        var csvFile = data.Body.toString('utf-8');
        try {
            csv({ output: "json" })
                .fromString(csvFile)
                .subscribe((csvLine) => {
                    console.log(csvLine);
                    jsonLines.push(JSON.parse(csvLine))
                })
        } catch (error) {
            return error;
        }
    });
    jsonLines.forEach(function (item) {
        if (item.hasOwnProperty("latitude") && item.hasOwnProperty("longitude") && item.hasOwnProperty("address")){
            dynamodb.putItem({
                "TableName": "positions",
                "Item": {
                    "id": uuidv4(),
                    "latitude": item.latitude.replace(/\s+/g, ""),
                    "longitude": item.longitude.replace(/\s+/g, ""),
                    "address": item.address
                }
            }, function (err, data) {
                // if (err) {
                //     console.info('Error putting item into dynamodb failed: ' + err);
                //     context.succeed('error');
                //     return err;
                // }
                // else {
                //     console.info('great success: ' + JSON.stringify(data, null, '  '));
                //     context.succeed('Done');
                // }
                console.log(err)
            });
            console.log(JSON.stringify(item))
        }else{
            var errMsg = "".concat("format error in", JSON.stringify(item));
            sns.publish({
                Subject: "data format error!",
                TopicArn: "arn:aws:sns:us-east-2:187917615240:errorsSNS",
                Message: errMsg
            }, function (err, data) {
                // if (err) {
                //     console.error('error publishing to SNS');
                //     context.fail(err);
                // } else {
                //     console.info('message published to SNS');
                //     context.succeed(null, data);
                // }
                console.log(err)

            });
        }
    });
    console.log(JSON.stringify(jsonLines))
};