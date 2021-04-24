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
    KeyConditionExpression: 'hashtag = :s',
    TableName: 'twitterimageDatabase'
  };

  ddb.query(params, function (err, data) {
    if (err) {
      console.log("Error", err);
    } else {
      //console.log("Success", data.Items);
      data.Items.forEach(function (element, index, array) {});
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
    let imageHTML = "";
    var imgs = [];
    images.forEach(function (element, index, array) {
      let image = "<img src='data:image/jpeg;base64," + encode(element.Body) + "'" + "/>";
      imgs.push(encode(element.Body));
    })
    let startHTML = "<html><body></body>";
    let endHTML = "</body></html>";
    let html = startHTML + imageHTML + endHTML;
    console.log(imgs[0]);

    res.render('layout', {
      //show_polls: polls 
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