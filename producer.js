const MONGO_CONN = "mongodb://127.0.0.1:27017/test";
const QUEUE_NAME = "test";

const mongoQueue = require('mongodb-queue');
const async = require('async');
const mongo = require('mongodb');

function createQueue(cb) {
  console.log("creating queue");
  async.waterfall([
    async.apply(mongo.MongoClient.connect, MONGO_CONN),
    function (db, callback) {
      var queue = mongoQueue(db, QUEUE_NAME);
      queue.createIndexes((err) => {
        return callback(err, queue);
      });
    }
  ], function(err, queue){
    if(err) {
      console.error(`Failed to start process due to error ${err}`);
      process.exit(1);
    }
    console.log("queue created");
    return cb(null, queue);
  });
}

function pushMessages(queue, numberOfMessages) {
  console.log("pushing messages");
  var messageCount = 0;
  async.whilst(() => messageCount < numberOfMessages, (callback) => {
    messageCount++;
    queue.add({count: messageCount}, (err, id) => {
      if(err) {
        console.log(`Failed to add message to queue due to err ${err}`);
      } else {
        console.log(`message pushed. Id = ${id}`);
      }
      return callback();
    });
  }, () => console.log(`all ${numberOfMessages} messages have pushed`));
}

createQueue((err, queue) => {
  pushMessages(queue, 100);
});