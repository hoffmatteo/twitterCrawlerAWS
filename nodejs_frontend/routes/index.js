var express = require('express');
const AWS = require('aws-sdk');
var router = express.Router();
AWS.config.update({
  region: 'eu-central-1'
});

/* GET users listing. */
router.get('/', function (req, res, next) {
  //res.render('error');
  res.render('index');
});

router.post('/', function (req, res, next) {
  var sqs = new AWS.SQS();
  var ddb = new AWS.DynamoDB();



  var hashtag = req.body.hashtag_field;
  var amount = req.body.amount_field;

  console.log(amount);
  var paramsSQS = {
    // Remove DelaySeconds parameter and value for FIFO queues
    MessageAttributes: {
      "Amount": {
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


  checkFinished().then((finished) => {
    res.redirect(307, '/results');
  })




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

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}


module.exports = router;