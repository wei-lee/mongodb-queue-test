const mongoQueue = require('mongodb-queue');
const cluster = require('cluster');
const async = require('async');
const mongo = require('mongodb');
const numberOfWorkers = 5;

const MONGO_CONN = "mongodb://127.0.0.1:27017/test";
const QUEUE_NAME = "test";

function log(message) {
  console.log(`[worker${process.pid}]:${message}`);
}

function connectMongo(cb) {
  log("connecting mongodb");
  mongo.MongoClient.connect(MONGO_CONN, cb);
}

function createQueue(cb) {
  log("creating queue");
  async.waterfall([
    async.apply(connectMongo),
    function(db, callback) {
      log("creating queue and indexes");
      var queue = mongoQueue(db, QUEUE_NAME, {visibility : 1});
      queue.createIndexes((err) => {
        return callback(err, queue);
      });
    },
    function(queue, callback) {
      queue.clean((err) => callback(err, queue));
    }
  ], (err, queue) => {
    if(err) {
      console.error(`Failed to start process due to error ${err}`);
      process.exit(1);
    }
    return cb(null, queue);
  });
}

function processJob(queue) {
  queue.get((err, msg) => {
    if (err) {
      console.log(`Failed to get message due to error ${err}`);
      return next(queue);
    }
    if (msg) {
      processMessage(queue, msg, () => next(queue));
    } else {
      return next(queue);
    }
  });
}

function next(queue) {
  process.nextTick(() => processJob(queue));
}

function processMessage(queue, msg, cb){
  log(`processing messsage id = ${msg.id} payload = ${msg.payload.count} tries = ${msg.tries}`);
  setTimeout(() => {
    queue.ack(msg.ack, (err, id) => {
      if (err) {
        log(`Failed to ack message due to error ${err}`);
      } else {
        log(`processed message id = ${msg.id} payload = ${msg.payload.count}`);
      }
      return cb();
    });
  }, Math.random()*1500);
}

if(cluster.isMaster) {
  console.log(`Master ${process.pid} is running`);
  for (var i=0;i<numberOfWorkers;i++) {
    cluster.fork();
  }
  cluster.on('exit', (worker, code, signal) => {
    console.log(`worker ${worker.process.pid} died!`);
  });
} else {
  createQueue((err, queue) => {
    log("worker started");
    processJob(queue);
  });
}
