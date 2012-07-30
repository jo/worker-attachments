var follow = require("follow");
var request = require("request");
var _ = require("underscore");

module.exports = WorkerAttachments;

function WorkerAttachments(options, db) {
  this.options = options || {};

  if (!this.options.server) throw('I need a server!');
  if (!this.options.name) throw('I need a name!');
  if (typeof this.options.processor !== 'object') throw('I need a processor!');
  if (typeof this.options.processor.check !== 'function') throw('I need a processor check function!');
  if (typeof this.options.processor.process !== 'function') throw('I need a processor process function!');

  this.options.config_id || (this.options.config_id = 'worker-config/' + this.options.name);
  this.options.status_id || (this.options.status_id = 'worker-status/' + this.options.name);

  this.db = db;

  console.log(this.options.name + ' worker running at ' + this.options.server + '/' + this.db);

  this._getConfigDoc(db, (function(config) {
    var changes_options = {
      db: this.options.server + '/' + encodeURIComponent(db),
      include_docs: true,
      since: 0
    };

    this._getStatusDoc(db, (function(status) {
      // follow since update seq stored in status doc
      changes_options.since = (status && status.last_update_seq) || 0;

      follow(changes_options, this._change_cb.bind(this));
    }).bind(this));
  }).bind(this));
}


// on change
WorkerAttachments.prototype._change_cb = function(error, change) {
  if (error !== null) {
    console.warn("error in WorkerAttachments")
    console.warn(error)
    return;
  }

  if (change.doc && change.id === this.options.config_id) {
    // set config
    this._updateConfig(change.doc);
  } else if (this.config && !change.id.match(/^_design\//)) {
    // write update seq to status doc
    this._writeStatusDoc(change.seq);
    // process doc
    this._process(change.doc);
  }
}


// config handling
//
WorkerAttachments.prototype._getConfigDoc = function(db, cb) {
  var url = this.options.server + 
    '/' + encodeURIComponent(db) +
    '/' + encodeURIComponent(this.options.config_id)

  request(url, (function(error, response, body) {
    if(error !== null) {
      console.warn("init error fetch config for " + db + ": " + error);
      return;
    }

    // set config if present
    if (response.statusCode < 400) {
      body = JSON.parse(body);
      this._updateConfig(body);
    }

    cb(this.config);
  }).bind(this));
};
// update worker config from doc
WorkerAttachments.prototype._updateConfig = function(doc) {
  if (doc._deleted) {
    // delete
    delete this.config;
  } else {
    // update
    // apply default worke config
    this.config = _.extend({}, this.options.defaults, doc);
  }
};
WorkerAttachments.prototype._getStatusDoc = function(db, cb) {
  var url = this.options.server + 
    '/' + encodeURIComponent(db) +
    '/' + encodeURIComponent(this.options.status_id)

  request(url, (function(error, response, body) {
    if (error !== null) {
      console.warn("init error fetch status for " + db + ": " + error);
      return;
    }

    // set config if present
    if (response.statusCode < 400) {
      body = JSON.parse(body);
      this._updateStatus(body);
    }

    cb(this.status);
  }).bind(this));
};
// update worker status 
WorkerAttachments.prototype._updateStatus = function(doc) {
  if (doc._deleted) {
    // delete
    delete this.status;
  } else {
    // update
    this.status = _.extend({}, doc);
  }
};
WorkerAttachments.prototype._writeStatusDoc = function(since) {
  this.status || (this.status = {});
  this.status._id = this.options.status_id;
  this.status.last_update_seq = since;

  this._saveDoc(this.status, function() {});
};

// process attachment
WorkerAttachments.prototype._process = function(doc) {
  var attachments = this._selectAttachments(doc);

  // ignore empty docs
  if (!attachments.length) return;

  // grap document / set status for each attachment we want to process
  _.each(attachments, function(name) {
    this._setDocumentStatus(doc, name, 'triggered');
  }, this);

  this._saveDoc(doc, (function(err, resp, data) {
    // could not grab document
    // ignore (will grab later, no problem)
    if (err !== null) {
      return;
    }

    var cnt = attachments.length;

    // update rev, got it.
    doc._rev = data.rev;
    this._log(doc, 'triggered');

    var cb = (function(error, name) {
      cnt--;

      this._setDocumentStatus(doc, name, error ? 'error' : 'completed');

      // TODO: add timeout/kill
      if (cnt === 0) {
        this._saveDoc(doc, (function(err, resp, data) {
          if (err !== null) {
            // TODO: reset in this case and try again beim naechsten mal
            this._log(doc, 'SHOULD RESET NOW');
          } else {
            this._log(doc, 'completed');
          }
        }).bind(this));
      }
    }).bind(this);

    // start processing each attachment in parallel
    _.each(attachments, function(name) {
      this.options.processor.process.call(this, doc, name, (function(n) {
        return function(error) {
          cb(error, n);
        }
      })(name));
    }, this);
  }).bind(this));
};


// get worker status
WorkerAttachments.prototype._getDocumentStatus = function(doc, name) {
  return doc.worker_status &&
    doc.worker_status[this.options.name] &&
    doc.worker_status[this.options.name][name];
};
  
// set worker status
WorkerAttachments.prototype._setDocumentStatus = function(doc, name, stat) {
  doc.worker_status || (doc.worker_status = {});
  doc.worker_status[this.options.name] || (doc.worker_status[this.options.name] = {});

  doc.worker_status[this.options.name][name] = {
    status: stat,
    revpos: parseInt(doc._rev)
  };
};
  
// return true if the doc needs to be processed
WorkerAttachments.prototype._checkAttachment = function(doc, name) {
  var stat = this._getDocumentStatus(doc, name);

  return !stat ||                    // no status doc
    (
     stat.status === 'completed' &&  // attachment has changed
     doc._attachments[name].revpos > stat.revpos // after completed processing
    );
};


// select attachments for prozessing
WorkerAttachments.prototype._selectAttachments = function(doc) {
  return _.compact(_.map(doc._attachments, function(attachment, name) {
    if (
        // attachment needs processing
        this._checkAttachment(doc, name) &&
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
WorkerAttachments.prototype._saveDoc = function(doc, cb) {
  request({
    url: this._urlFor(doc),
    method: 'PUT',
    body: doc,
    json: true
  }, cb);
};

