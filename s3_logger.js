'use strict';

// Declare AWS services from SDK as variables
const AWS = require('aws-sdk');
const SQS = new AWS.SQS({ apiVersion: '2012-11-05' });
const S3 = new AWS.S3({apiVersion: '2006-03-01'});
const Lambda = new AWS.Lambda({ apiVersion: '2015-03-31' });

// Transform the variables coming from the Lambda environment to simplified variables for the script
const QUEUE_URL = process.env.queueUrl; // string: https://queue.amazonaws.com/123456789012/QUEUENAME
const S3_BUCKET_FOR_LOGGING = process.env.s3BucketForLogging; // string: s3BucketNameHere
const LOG_FOLDER = process.env.logFolder; // string: dome9-log/dome9AuditTrail/ (no leading forward slash)
const LOG_FILE_PREFIX = process.env.logFilePrefix; // string: dome9AuditTrail
const PROCESS_MESSAGE = 'process-message';

// Function to finally write the log message to S3
function s3PutObject(s3Params, message, callback){
		S3.putObject(s3Params, function(err, data) {
		if (err) {
			console.log(err, err.stack); // an error occurred, write it to the Lambda CloudWatch log
		}
		else {
			console.log(data);		 // successful response, write it to the Lambda CloudWatch log
			console.log("Log file written to s3://" + s3Params.Bucket + "/" + s3Params.Key);

			// delete message for SQS queue because it is implied the process was successful
			const sqsParams = {
			   QueueUrl: QUEUE_URL,
			   ReceiptHandle: message.ReceiptHandle,
			};
			SQS.deleteMessage(sqsParams, (err) => callback(err, message));
		}
	});
}


function invokePoller(functionName, message) {
    // this function will take the Message from SQS and call a new execution of this Lambda to process it (look at the very bottom of this script)
	const payload = {
	   operation: PROCESS_MESSAGE,
	   message,
	};
	const params = {
	   FunctionName: functionName,
	   InvocationType: 'Event',
	   Payload: new Buffer.from(JSON.stringify(payload)),
	};
    // Note to patrick: Promises do not exist in Python, e a different method
	return new Promise((resolve, reject) => {
	   Lambda.invoke(params, (err) => (err ? reject(err) : resolve()));
	});
}

// random ID generator which is used to append onto the filename of the S3 object that will be written
var generateId = function() {
	var ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
	var ID_LENGTH = 16;
	var rtn = '';
	for (var i = 0; i < ID_LENGTH; i++) {
			rtn += ALPHABET.charAt(Math.floor(Math.random() * ALPHABET.length));
	}
	return rtn;
};

function processMessage(message, callback) {
	console.log(message);

	// process message
	var msgToJson = JSON.parse(message.Body); //this is the main message body (the actual log text)

	// everything below here is not relevant to the log we want to write, so let's just delete the data from the JSON object
	delete msgToJson.Type;
	//delete msgToJson.MessageId;
	delete msgToJson.TopicArn;
	delete msgToJson.SignatureVersion;
	delete msgToJson.Signature;
	delete msgToJson.SigningCertURL;
	delete msgToJson.UnsubscribeURL;
	
	var msgTimestamp = msgToJson.Timestamp; // Timestamp sample: 2018-04-27T15:58:00Z
	if (! msgTimestamp) {
		console.error("Message timestamp not found.");
	}

	// declare variables so we can write a nice timestamp in the S3 object name
	var tsYear = msgTimestamp.substring(0, 4);
	var tsMonth = msgTimestamp.substring(5, 7);
	var tsDate = msgTimestamp.substring(8, 10);
	var tsHours = msgTimestamp.substring(11, 13);
	var tsMinutes = msgTimestamp.substring(14, 16);
	
	var strMsg = JSON.stringify(msgToJson, null, "\t"); // Indented with tab
	// create a log filename (object) with date, time and a randomly generated ID
	var logFilename = LOG_FILE_PREFIX + tsYear + tsMonth + tsDate + "T" + tsHours + tsMinutes + "Z_" + generateId() + ".json";
	var s3Params ={};

    s3Params= {
        Body: strMsg,
        Bucket: S3_BUCKET_FOR_LOGGING,
        Key: LOG_FOLDER + tsYear + "/" + tsMonth + "/" + tsDate + "/" + logFilename,
        ContentType: "application/json",
        Tagging: "source=dome9AuditTrail&timestamp=" + msgToJson.Timestamp
    };
    // ready to write the S3 object after all that formatting and prep
    s3PutObject(s3Params, message, callback);

}

function poll(functionName, callback) {
	// This function polls the SQS queue and figures out how many times it needs to loop so it gets all the messages in one execution
	var msgCount = 0;
	var estBatches = 0;
	
	const sqsGetQueueAttributesParams = {
	  QueueUrl: QUEUE_URL,
	  AttributeNames: [ "ApproximateNumberOfMessages" ]
	};
	
	const sqsReceiveMessageParams = {
	   QueueUrl: QUEUE_URL,
	   MaxNumberOfMessages: 10,
	   VisibilityTimeout: 15,
	};
	
	SQS.getQueueAttributes(sqsGetQueueAttributesParams, function(err, data) {
		if (err) {
			console.log(err, err.stack); // an error occurred
		}
		else {
			console.log(data);		   // successful response
			// calculate how many batches of messages the SQS queue is holding.
			// The requested amount (above) is 10, however, due to AWS not always giving the max of 10 queue messages per request
			// the equation is cut in half to 5. Therefore, if there are more batches than needed, the process will write zero messages to S3 anyway
			// if the queue is empty at that point
			// but at least this execution read all the queue messages and was able to process them
			var estBatches = Math.ceil( (data.Attributes.ApproximateNumberOfMessages / 5) );
			console.log("Queue Size: " + data.Attributes.ApproximateNumberOfMessages + " Estimated Poll Batches: " + estBatches);
				
            // Loop until number of estimated batches is exhausted
			var i = 0;
			while (i < estBatches) {
				// batch request messages 
				console.log("Processing batch #" + (i + 1));
				SQS.receiveMessage(sqsReceiveMessageParams, (err, data) => {
					if (err) {
					  return callback(err);
					}
				   
					// for each message, reinvoke the function
					if (data.Messages) {
						const promises = data.Messages.map((message) => invokePoller(functionName, message));
					   // complete when all invocations have been made
					   // patrick: Promises do not exist in Python.
						Promise.all(promises).then(() => {
							const batchResult = `Messages received: ${data.Messages.length}`;
							console.log(batchResult);
							msgCount += data.Messages.length;
						});
					}
					else {
						const batchResult = "Woohoo! No messages to process.";
						console.log(batchResult);
						i = estBatches;
					}
				});
				i++;
			}
			const result = "Total Batches: " + estBatches;
			callback(null, result);
		}
	});
}

exports.handler = (event, context, callback) => {
// this is the first thing that runs when the Lambda is executed
// this part tries to figure out if the script was executed because of the schedule OR because this lambda script called ITSELF to process a message
// Think of this like a Lambda calling another lambda, but in this case, it is itself.
	try {
	    // variables like 'event' and 'context' are provided from AWS during the code execution (Lambda metadata)
	   if (event.operation === PROCESS_MESSAGE) {
		  // invoked by poller
		  processMessage(event.message, callback);
	   } else {
		  // invoked by schedule
		  poll(context.functionName, callback);
	   }
	} catch (err) {
	   callback(err);
	}
};
