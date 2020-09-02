// dependencies
const AWS = require('aws-sdk');
const csv = require('csvtojson');
const s3 = new AWS.S3();
const { v4: uuidv4 } = require('uuid');
const dynamodb = new AWS.DynamoDB();
const sns = new AWS.SNS();


// check csv file type is correct type.
exports.checkCsvFileType = function(Key) {
    const typeMatch = Key.match(/\.([^.]*)$/);
    const fileType = typeMatch[1].toLowerCase();
    if (!typeMatch) {
        console.log("Could not determine the csv type.");
    };
    if (fileType != "csv") {
        console.log(`Only supported csv type`);
    };
    return typeMatch && fileType == "csv";
};


// check csv content is correct format.
exports.checkCsvFormat = function(csvJsonItem) {
    return csvJsonItem.hasOwnProperty("latitude") && csvJsonItem.hasOwnProperty("longitude") && csvJsonItem.hasOwnProperty("address");
};



// save object to dynamoDB
function putItemToDB(item, context){
    dynamodb.putItem({
        "TableName": "Positions",
        "Item": {
            "id": uuidv4(),
            "latitude": item.latitude.replace(/\s+/g, ""),
            "longitude": item.longitude.replace(/\s+/g, ""),
            "address": item.address
        }
    }, function (err, data) {
        if (err) {
            console.info('Error putting item into dynamodb failed: ' + err);
            context.succeed('error');
            return err;
        }
        else {
            console.info('great success: ' + JSON.stringify(data, null, '  '));
            context.succeed('Done');
        }
    })
};


// convert csv line to json object
function csvToObj(event){
    const srcBucket = event.Records[0].s3.bucket.name;
    var jsonLines = [];
    const params = {
        Bucket: srcBucket,
        Key: srcKey
    };
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
    return jsonLines
};


// publish error messages to aws sns.
function publishToSNS(errMsg, context){
    sns.publish({
        Subject: "Data format error!",
        TopicArn: "arn:aws:sns:us-east-2:187917615240:errorsSNS",
        Message: errMsg
    }, function (err, data) {
        if (err) {
            console.error('error publishing to SNS');
            context.fail(err);
        } else {
            console.info('message published to SNS');
            context.succeed(null, data);
        }
    });
}


// main handler
exports.handler = async (event, context, callback) => {
    const srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
    if (checkCsvFileType(srcKey)){
        var objectsList = csvToObj(event)
        objectsList.forEach(function (item) {
            if (checkCsvFormat(item)) {
                putItemToDB(item, context);
            } else {
                var errMsg = "".concat("format error in ", JSON.stringify(item));
                publishToSNS(errMsg, context);
            }
        });
    } else {
        var errMsg = "".concat("csv type error in ", srcKey);
        publishToSNS(errMsg, context)
    }
    console.log(JSON.stringify(objectsList))
    return JSON.stringify(objectsList)
};