var follow = require("follow");
var request = require("request");
var _ = require("underscore");

module.exports = WorkerAttachments;

function WorkerAttachments(options, db) {
  this.options = options || {};

  this.db = db;

  console.log(this.options.name + ' worker running at ' + this.options.server + '/' + this.db);

  request(this.options.server + '/' + encodeURIComponent(db) + '/' + encodeURIComponent(this.options.config_id), _.bind(function(error, response, body) {
    if(error !== null) {
      console.warn("init error fetch config for " + db + ": " + error);
      return;
    }

    var changes_options = {
      db: this.options.server + '/' + encodeURIComponent(db),
      include_docs: true,
      since: 0
    };

    // set config if present
    if (response.statusCode < 400) {
      body = JSON.parse(body);
      this._setConfig(body);
    }
    
    // follow since config update seq
    if (this.config && this.config.last_update_seq) {
      changes_options.since = this.config.last_update_seq;
    }

    follow(changes_options, this._change_cb.bind(this));
  }, this));
}


WorkerAttachments.prototype._change_cb = function(error, change) {
  if (error !== null) {
    console.warn("error in WorkerAttachments")
    console.warn(error)
    return;
  }

  if (change.doc && change.id === this.options.config_id) {
    // set config
    this._setConfig(change.doc);
  } else if (this.config && !change.id.match(/^_design\//)) {
    // write update seq to config
    if (this.config && this.config.last_update_seq !== change.seq) {
      this._writeSince(change.seq);
    }
    // process doc
    this._process(change.doc);
  }
}

// update worker config from doc
WorkerAttachments.prototype._setConfig = function(doc) {
  if (doc._deleted) {
    // delete
    delete this.config;
  } else {
    // update
    // apply default worke config
    this.config = _.extend({}, this.options.defaults, doc);
  }
};
WorkerAttachments.prototype._writeSince = function(since) {
  this.config.last_update_seq = since;

  // todo: use own doc
  this._saveDoc(this.config, function() {});
};

// process attachment
WorkerAttachments.prototype._process = function(doc) {
  var attachments = this._selectAttachments(doc);

  if (!attachments.length) return;

  // grap document / set status for each attachment we want to process
  _.each(attachments, function(name) {
    this._setStatus(doc, name, 'triggered');
  }, this);

  this._saveDoc(doc, _.bind(function(err, resp, data) {
    // could not grab document, ignore (will grab later)
    if (err !== null) {
      return;
    }

    var cnt = attachments.length;

    // update rev, got it.
    doc._rev = data.rev;
    this._log(doc, 'triggered');

    var cb = _.bind(function(error, name) {
      cnt--;

      this._setStatus(doc, name, error ? 'error' : 'completed');

      // TODO: add timeout/kill
      if (cnt === 0) {
        this._saveDoc(doc, _.bind(function(err, resp, data) {
          if (err !== null) {
            // TODO: reset in this case and try again beim naechsten mal
            this._log(doc, 'SHOULD RESET NOW');
          } else {
            this._log(doc, 'completed');
          }
        }, this));
      }
    }, this);

    // start processing each attachment in parallel
    _.each(attachments, function(name) {
      this.options.processor.process.call(this, doc, name, (function(n) {
        return function(error) {
          cb(error, n);
        }
      })(name));
    }, this);
  }, this));
};


// get worker status
WorkerAttachments.prototype._getStatus = function(doc, name) {
  return doc.worker_status &&
    doc.worker_status[this.options.name] &&
    doc.worker_status[this.options.name][name];
};
  
// set worker status
WorkerAttachments.prototype._setStatus = function(doc, name, stat) {
  doc.worker_status || (doc.worker_status = {});
  doc.worker_status[this.options.name] || (doc.worker_status[this.options.name] = {});

  doc.worker_status[this.options.name][name] = {
    status: stat,
    revpos: parseInt(doc._rev)
  };
};
  
// return true if the doc needs to be processed
WorkerAttachments.prototype._checkAttachment = function(doc, name) {
  var stat = this._getStatus(doc, name);

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

