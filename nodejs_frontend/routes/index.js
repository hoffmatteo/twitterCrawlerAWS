var express = require('express');
const AWS = require('aws-sdk');
var router = express.Router();
AWS.config.update({
  region: 'eu-central-1'
});

/* GET index page. */
router.get('/', function (req, res, next) {
  res.render('index');
});

/* gets called when form is submitted. */
router.post('/', function (req, res, next) {
  //init dynamoDB and SQS Queue
  var sqs = new AWS.SQS();
  var ddb = new AWS.DynamoDB();

  //reads values from submitted form
  var hashtag = req.body.hashtag_field;
  var amount = req.body.amount_field;

  //Parameters are set and Message(body is the hashtag, amount is Attribute) is sent
  var paramsSQS = {
    MessageAttributes: {
      "amount": {
        DataType: "Number",
        StringValue: amount
      }
    },
    MessageBody: hashtag,
    MessageGroupId: "hashtag", // Required for FIFO queues
    QueueUrl: "https://sqs.eu-central-1.amazonaws.com/853007416067/HashtagQueue.fifo"
  };

  sqs.sendMessage(paramsSQS, function (err, data) {
    if (err) {
      console.log("Error", err);
    } else {
      console.log("Success", data.MessageId);
    }
  });

  var paramsDynamo = {
    ExpressionAttributeValues: {
      ':s': {
        S: hashtag
      }
    },
    KeyConditionExpression: 'hashtag = :s',
    TableName: 'twitterimageDatabase'
  };

  //redirect to the results page once enough images are available
  checkFinished().then((finished) => {
    res.redirect(307, '/results');
  })



  /* This method uses promises to check the dynamoDB every 2 seconds. 
  If there are enough entries for the requested amount, the method finishes. */
  async function checkFinished() {
    var finished = false;
    while (!finished) {
      ddb.query(paramsDynamo, function (err, data) {
        if (err) {
          console.log("Error", err);
        } else {
          console.log("dynamo length: " + data.Items.length);
          if (data.Items.length >= amount) {
            finished = true;
            return;
          }
        }
      });
      await sleep(2000);
    }
  }
})

/* Helper Function */
function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}


module.exports = router;