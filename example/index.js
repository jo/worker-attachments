// Worker Attachments Example
//
// example mimimal worker that checks every jpg or png image
//
// (c) Johannes J. Schmidt

var request = require("request");
var WorkerAttachments = require("worker-attachments");

// describe your work here:
var processor = (function() {
  var formats = ['jpg', 'png'];

  return {
    check: function(doc, name) {
      return formats.indexOf(name.toLowerCase().replace(/^.*\.([^\.]+)$/, '$1')) > -1;
    },
    process: function(doc, name, next) {
      this._log(doc, 'found image: ' + name);
      // do stuff...
      // call next(true) if any error happened
      next();
    }
  };
})();

// create a worker config:
//   curl -XPUT http://localhost:5984/mydb/worker-config%2Fattachments \
//     -H 'Content-Type:application/json' \
//     -d'{"_id": "worker-config/attachments"}'
//
var config = {
  server: process.env.HOODIE_SERVER || "http://127.0.0.1:5984",
  name: 'something',
  config_id: 'worker-config/something',
  status_id: 'worker-status/something',
  processor: processor
};

var workers = [];
request(config.server + "/_all_dbs", function(error, response, body) {
  if(error !== null) {
    console.warn("init error, _all_dbs: " + error);
    return;
  }

  var dbs = JSON.parse(body);
  // listen on each db.
  // Note that you have to restart the worker
  // in order to listen to newly created databases.
  dbs.forEach(function(db) {
    var worker = new WorkerAttachments(config, db);
    workers.push(worker);
  });
});
