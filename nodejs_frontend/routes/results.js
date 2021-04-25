var express = require('express');
const AWS = require('aws-sdk');
var router = express.Router();
AWS.config.update({
  region: 'eu-central-1'
});


router.post('/', function (req, res, next) {

  var hashtag = req.body.hashtag_field;
  var amount = req.body.amount_field;


  console.log("Searching for: " + req.body.hashtag_field);

  let s3 = new AWS.S3();
  var ddb = new AWS.DynamoDB();

  var params = {
    ExpressionAttributeValues: {
      ':s': {
        S: hashtag
      }
    },
    KeyConditionExpression: 'hashtag = :s',
    TableName: 'twitterimageDatabase'
  };

  ddb.query(params, function (err, data) {
    if (err) {
      console.log("Error", err);
    } else {
      //console.log("Success", data.Items);
      parseImage(data.Items);
    }
  });

  function parseImage(items) {
    var curr_data = [];
    items.forEach(function (element, index, array) {
      console.log(element.media_key.S);
      getImage(element.media_key.S).then((img) => {
        curr_data.push(img);
        if (curr_data.length == items.length) {
          displayImages(curr_data);
        }
      })
    })
  }

  async function getImage(media_key) {
    const data = s3.getObject({
        Bucket: 'twitterimagesoth',
        Key: media_key
      }

    ).promise();
    return data;
  }

  function displayImages(images) {
    var imgs = [];
    images.forEach(function (element, index, array) {
      imgs.push(encode(element.Body));
    })

    res.render('results', {
      images: imgs
    });



  }

  function encode(data) {
    let buf = Buffer.from(data);
    let base64 = buf.toString('base64');
    return base64
  }
});

module.exports = router;