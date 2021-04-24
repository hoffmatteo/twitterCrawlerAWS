var express = require('express');
const AWS = require('aws-sdk');
var router = express.Router();
AWS.config.update({
  region: 'eu-central-1'
});


/* GET home page. */
router.get('/', function (req, res, next) {




  let s3 = new AWS.S3();
  var ddb = new AWS.DynamoDB();

  var params = {
    ExpressionAttributeValues: {
      ':s': {
        S: 'dogs'
      }
    },
    KeyConditionExpression: 'Hashtag = :s',
    TableName: 'twitterimageDatabase'
  };

  ddb.query(params, function (err, data) {
    if (err) {
      console.log("Error", err);
    } else {
      //console.log("Success", data.Items);
      data.Items.forEach(function (element, index, array) {
        console.log(element);
        displayImage(element);
      });
      displayImage(data.Items)
    }
  });

  function displayImage(items) {

  }

  async function getImage(media_key) {
    const data = s3.getObject({
        Bucket: 'twitterimagesoth',
        Key: 'dogs/3_1384456328576311299'
      }

    ).promise();
    return data;
  }

  getImage()
    .then((img) => {
      let image = "<img src='data:image/jpeg;base64," + encode(img.Body) + "'" + "/>";
      let startHTML = "<html><body></body>";
      let endHTML = "</body></html>";
      let html = startHTML + image + endHTML;
      res.send(html);
      console.log(htlm);
    }).catch((e) => {
      res.send(e);
    })

  function encode(data) {
    let buf = Buffer.from(data);
    let base64 = buf.toString('base64');
    return base64
  }
});

module.exports = router;