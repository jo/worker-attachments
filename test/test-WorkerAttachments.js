var assert = require("assert");
var WorkerAttachments = require("./../lib/WorkerAttachments");

describe("WorkerAttachments", function() {
  var options = {
    server: 'http://localhost:5984',
    name: 'test-worker',
    config_id: 'worker-config/test-worker',
    defaults: {
      default_option: 'bar'
    },
    processor: {
      check: function(doc, name) {
        return doc.file === name;
      },
      process: function() {}
    }
  };
  var db = '_users';

  var worker = new WorkerAttachments(options, db);

  describe("_getStatus", function() {
    it("should return false if no status on doc", function() {
      assert(!worker._getStatus({}));
    });
    it("should return false if no worker status on doc", function() {
      assert(!worker._getStatus({
        worker_status: {
          foo: 'bar'
        }
      }));
    });
    it("should return false if no status for given attachment", function() {
      assert(!worker._getStatus({
        worker_status: {
          'test-worker': {}
        }
      }, 'myfile'));
    });
    it("should return status for attachment", function() {
      var stat = {
        status: 'completed',
        revpos: 3
      };

      assert.equal(stat, worker._getStatus({
        worker_status: {
          'test-worker': {
            'myfile': stat
          }
        }
      }, 'myfile'));
    });
  });

  describe("_setConfig", function() {
    it("should update config", function() {
      worker._setConfig({
        foo: 'bar'
      });
      assert.equal('bar', worker.config.foo);
    });
    it("should apply defaults", function() {
      worker._setConfig({});
      assert.equal('bar', worker.config.default_option);
    });
    it("should delete config", function() {
      worker._setConfig({
        _deleted: true
      });
      assert(!worker.config);
    });
  });

  describe("_setStatus", function() {
    it("should set status", function() {
      var doc = {};
      worker._setStatus(doc, 'completed');

      assert.equal('completed', doc.worker_status['test-worker'].status);
    });
    it("should set revpos from _rev", function() {
      var doc = { _rev: '3-bla' };
      worker._setStatus(doc, 'completed');

      assert.equal(3, doc.worker_status['test-worker'].revpos);
    });
  });

  describe("_checkAttachment", function() {
    it("should return true if no status object present", function() {
      assert(worker._checkAttachment({}, 'myfile'));
    });
    it("should return true if status object is completed and revpos higher than current", function() {
      assert(worker._checkAttachment({
        worker_status: {
          'test-worker': {
            status: 'completed',
            revpos: 3
          }
        },
        _attachments: {
          myfile: {
            revpos: 4
          }
        }
      }, 'myfile'));
    });
    it("should return false if status object is completed but revpos equals current", function() {
      assert(!worker._checkAttachment({
        worker_status: {
          'test-worker': {
            'myfile': {
              status: 'completed',
              revpos: 3
            }
          }
        },
        _attachments: {
          myfile: {
            revpos: 3
          }
        }
      }, 'myfile'));
    });
    it("should return false if status object is not completed", function() {
      assert(!worker._checkAttachment({
        worker_status: {
          'test-worker': {
            'myfile': {
              status: 'triggered'
            }
          }
        }
      }, 'myfile'));
    });
    it("should return false if status object has lower revpos than attachments revpos", function() {
      assert(!worker._checkAttachment({
        worker_status: {
          'test-worker': {
            myfile: {
              status: 'triggered',
              revpos: 2
            }
          }
        },
        _attachments: {
          myfile: {
            revpos: 3
          }
        }
      }, 'myfile'));
    });
  });

  describe("_selectAttachments", function() {
    it("should run processors check", function() {
      var attachments = worker._selectAttachments({
        file: 'myfile',
        _attachments: {
          myfile: {}
        }
      });

      assert.equal("myfile", attachments[0]);

      attachments = worker._selectAttachments({
        file: 'otherfile',
        _attachments: {
          myfile: {}
        }
      });

      assert.equal(0, attachments.length);
    });
    it("should return the names of the attachments", function() {
      var attachments = worker._selectAttachments({
        file: 'myfile',
        _attachments: {
          myfile: {}
        }
      });

      assert.equal("myfile", attachments[0]);
    });
    it("should exclude own folders", function() {
      // configure a folder
      worker._setConfig({ folder: 'myfolder' });

      var attachments = worker._selectAttachments({
        file: 'myfolder/myfile',
        _attachments: {
          'myfolder/myfile': {}
        }
      });

      assert.equal(0, attachments.length);

      // reset config
      worker._setConfig({ _deleted: true });
    });
    it("should check status", function() {
      var attachments = worker._selectAttachments({
        worker_status: {
          'test-worker': {
            'myfile': {
              status: 'triggered'
            }
          }
        },
        file: 'myfile',
        _attachments: {
          'myfile': {
            revpos: 3
          }
        }
      });

      assert.equal(0, attachments.length);
    });
  });

  describe("_urlFor", function() {
    it("should return url for document", function() {
      assert.equal(options.server + '/' + db + '/mydoc', worker._urlFor({ _id: 'mydoc' }));
    });
    it("should encode id", function() {
      assert.equal(options.server + '/' + db + '/prefix%2Fmydoc', worker._urlFor({ _id: 'prefix/mydoc' }));
    });
    it("should return url for attachment", function() {
      assert.equal(options.server + '/' + db + '/mydoc/myfile', worker._urlFor({ _id: 'mydoc' }, 'myfile'));
    });
    it("should encode attachment name", function() {
      assert.equal(options.server + '/' + db + '/mydoc/myfolder%2Fmyfile', worker._urlFor({ _id: 'mydoc' }, 'myfolder/myfile'));
    });
  });
});
