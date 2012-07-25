var CouchDBChanges = require("CouchDBChanges");
var request = require("request");
var _ = require("underscore");

module.exports = WorkerAttachments;

function WorkerAttachments(options, db) {
  this.options = options || {};

  this.db = db;

  var follow_options = {
    url: this.options.server
  };

  var changes_options = {
    include_docs: true
  };

  console.log('WorkerAttachments running at ' + this.options.server + '/' + this.db);

  var changes = new CouchDBChanges(this.options.server);

  changes.follow(this.db, this._change_cb.bind(this), follow_options, changes_options);
}


WorkerAttachments.prototype._change_cb = function(error, change) {
  if (error !== null) {
    console.warn("error in WorkerAttachments")
    console.warn(error)
    return;
  }

  if (change.doc && change.doc._id === this.options.config_id) {
    this._setConfig(change.doc);
  } else if (this.config) {
    this._process(change.doc);
  }
}

// update worker config from doc
WorkerAttachments.prototype._setConfig = function(doc) {
  if (doc._deleted) {
    // delete
    this._log(doc, 'delete config');
    delete this.config;
  } else {
    // update
    this._log(doc, this.config ? 'update config' : 'create config');

    // apply default worke config
    this.config = _.extend({}, this.options.defaults, doc);
  }
}

// process attachment
WorkerAttachments.prototype._process = function(doc) {
  var attachments = this._selectAttachments(doc);

  if (!attachments.length) return;

  // grap document
  this._setStatus(doc, 'triggered');
  this._saveDoc(doc, _.bind(function(err, resp, data) {
    var cnt = attachments.length;

    // update rev, got it.
    doc._rev = data.rev;
    this._log(doc, 'triggered');

    var cb = _.bind(function() {
      cnt--;

      // TODO: check for errors

      if (cnt === 0) {
        this._setStatus(doc, 'completed');
        this._saveDoc(doc, _.bind(function() {
          this._log(doc, 'completed');
        }, this));
      }
    }, this);

    // start processing each image in paralel
    _.each(attachments, function(name) {
      this.options.processor.process.call(this, doc, name, cb);
    }, this);
  }, this));
};


// get worker status
WorkerAttachments.prototype._getStatus = function(doc) {
  return doc.worker_status &&
    doc.worker_status[this.options.name];
};
  
// set worker status
WorkerAttachments.prototype._setStatus = function(doc, stat) {
  doc.worker_status || (doc.worker_status = {});

  doc.worker_status[this.options.name] = {
    status: stat,
    revpos: parseInt(doc._rev)
  };
};
  
// return true if the doc needs to be processed
WorkerAttachments.prototype._checkStatus = function(doc, attachment) {
  var stat = this._getStatus(doc);

  return !stat ||                    // no status doc
    (
     stat.status === 'completed' &&  // attachment has changed
     attachment.revpos > stat.revpos // after completed processing
    );
};


// select attachments for prozessing
WorkerAttachments.prototype._selectAttachments = function(doc) {
  return _.compact(_.map(doc._attachments, function(attachment, name) {
    if (
        // attachment needs processing
        this._checkStatus(doc, attachment) &&
        // ignore own processed files if folder specified
        (!this.config || !this.config.folder || name.split('/', 1)[0] !== this.config.folder) &&
        // select only images imagemagick can understand
        this.options.processor.check.call(this, doc, name)
      ) return name;

    return null;
  }, this));
};

// return url for an attachment
WorkerAttachments.prototype._urlFor = function(doc, attachment) {
  return this.options.server +
    '/' + encodeURIComponent(this.db) +
    '/' + encodeURIComponent(doc._id) +
    (attachment ? '/' + encodeURIComponent(attachment) : '');
};

// log a message
WorkerAttachments.prototype._log = function(doc, msg) {
  if (!msg) {
    msg = doc;
  }
  console.log('[%s] %s: %s',this.db, doc._id || '', msg);
};

// save doc
// TODO: error handling
WorkerAttachments.prototype._saveDoc = function(doc, cb) {
  request({
    url: this._urlFor(doc),
    method: 'PUT',
    body: doc,
    json: true
  }, cb);
};

