var express = require('express');
const AWS = require('aws-sdk');
var router = express.Router();
AWS.config.update({
  region: 'eu-central-1'
});

/* This page only should be called once a request has been made. */
router.post('/', function (req, res, next) {

  //reads hashtag and amount from form
  var hashtag = req.body.hashtag_field;
  var amount = req.body.amount_field;
  console.log("Searching for: " + req.body.hashtag_field);

  //Initialization for DynamoDB (read media_keys) and S3 (show pictures)
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
  //This queries the dynamoDB for all pictures belonging to the requested hashtag. 
  ddb.query(params, function (err, data) {
    if (err) {
      console.log("Error", err);
    } else {
      parseImage(data.Items);
    }
  });


  /*Helper Function*/
  function encode(data) {
    let buf = Buffer.from(data);
    let base64 = buf.toString('base64');
    return base64
  }

  /*parseImage is responsible for calling getImage for every dynamoDB entry.
  If every entry has been called and returned, then displayImages is called.*/
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

  /*getImage accesses the S3 Bucket asynchronously, and gets a single image by its media_key*/
  async function getImage(media_key) {
    const data = s3.getObject({
        Bucket: 'twitterimagesoth',
        Key: media_key
      }

    ).promise();
    return data;
  }

  /*displayImages formats the images into an array that is then given to the rendered hbs page*/
  function displayImages(images) {
    var imgs = [];
    images.forEach(function (element, index, array) {
      imgs.push(encode(element.Body));
    })
    res.render('results', {
      images: imgs
    });
  }

});


module.exports = router;