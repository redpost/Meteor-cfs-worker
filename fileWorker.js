//// TODO: Use power queue to handle throttling etc.
//// Use observe to monitor changes and have it create tasks for the power queue
//// to perform.

var msSec = 1000;
var msMin = 60 * msSec;
var msHour = 60 * msMin;
var msDay = msHour * 24;

// CONFIG
_config = {
  // Generate a server id or use the user defined
  nodeId: Random.id(),

  // A task will timeout after 1 hour or what ever user has defined
  // When timed out it can be pulled from a worker again.
  timeout: msHour,

  // A task will die after default 1 day
  // This makes sure that tempstore is cleaned up and permenant failing tasks are
  // removed.
  expire: msDay,

  // If a task fails we wait a while until its rerun default is 10 min
  // But tasks are sorted by failure and createdAt - so if a new file task is
  // added that goes before failed task - 10min is not a fixed interval its a
  // minimum time to wait until retry
  sleep: 10 * msMin,

  // Limit the number of workers that may be processing simultane
  limit: 1
};

// TASKS COLLECTION
var tasks = new Meteor.Collection('cfs.filesReadyForStorage');

// A task consists of

// Reference:
// * fileId         - Target File
// * collectionName - Target Collection
// * stores         - Target Storage Adapters, array of store names push to fifo

// Worker reference:
// * nodeId         - Assigned node / server
// * workerId       - Assigned woker on that server

// Tracking:
// * createdAt      - Used to determin the life length of the storage task
// * runCount       - count number of runCount
// * nextRun        - this time stamp determins when the task may be run again

_addDate = function(delay) {
  // Return the timestamp + the delay
  return new Date(+new Date() + delay);
};

/**
 * @public
 * @type Object
 */
FS.FileWorker = function() {
  var self = this;

  if (!(self instanceof FS.FileWorker)) {
    throw new Error('FS.FileWorker, Error: missing keyword "new"');
  }

  self.isRunning = 0;

  // Set worker id
  self.id = Random.id();

  // Spawn worker
  self.run();

};


FS.FileWorker.prototype.run = function() {
  var self = this;

  // This is the code that starts up and maintains the
  self.runTask(function(result) {
    // task has completed
    if (result === null) {
      // there where no tasks ready
    } else {
      // Task is complete go to next
      Meteor.setTimeout(function() {
        self.run();
      }, 0);
    }
  });
};

FS.FileWorker.prototype.runTask = function(done) {
  var self = this;

  if (self.isRunning++ === 0) {

    // Request a pending task
    var task = FS.FileWorker.requestTask(self.id);

    if (task) {

      // Get the next storeName
      storeName = task.stores.shift();

      // Handle the task
      self.taskHandler(task, storeName, function(result) {
        // Check if we have passed the timeout
        if (task.nextRun < _addDate(0)) {
          // We are good

          if (result === null) {
            // This task should self destruct - it failed permently

            // Remove all tasks on this file
            FS.FileWorker.removeTask(fileObj);

          } else if (typeof result === 'undefined') {
            // This task has completed
            // we remove the store name
            // And check if it was the last store
            // And remove the worker reference
            if (task.stores.length === 0) {

              // Remove all tasks on this file
              FS.FileWorker.removeTask(fileObj);

            } else {
              tasks.update({ _id: task._id }, {
                $set: {
                  // Update the store tracker with out the current storeName
                  stores: task.stores,
                  // Allow rerun from now on - this overwrites the timeout
                  nextRun: _addDate(0);
                }
              });
            }

          } if (result === ''+result) {
            // We got an error, push the storeName back in
            task.stores.push(storeName);
            // Update
            tasks.update({ _id: task._id }, {
              $set: {
                // Update the stores
                stores: task.stores,
                // Have the failed task sleep a bit before retry
                nextRun: _addDate( _config.sleep )
              }
            });
          }

        } else {
          // This task exeeded its timeout - we dont touch anything

        }

        // Allow the worker to start up a new task
        // This task has ended
        self.isRunning--;
        done();
      }); // EO task handler

    } else {
      done(null);
    }


  } else {
    // XXX: can this happen in js?
    self.isRunning--;
  }

};

FS.FileWorker.prototype.taskHandler = function(task, storeName, done) {
  var self = this;

  // Init the fileObj
  var fileObj = new FS.File({ _id: task.fileId, collectionName: collectionName });

  // Find the current expiration date - tasks must be created after this date
  var expirationDate = _addDate(-_config.expire);

  // Make sure file is mounted and task created after expiration date
  if (fileObj.isMounted() && task.createdAt > expirationDate) {

    // Store the file
    self.storeFile(fileObj, storeName, done);

  } else {
    // This task failed permently since the file is not mounted on a
    // collection or is expired, so we remove it from tempstore and the task
    // This task will self destruct
    done(null);
  }

};

/**
 * FS.FileWorker.prototype.storeFile
 * @param {FS.File} fileObj
 * @param {string} storeName Name of store
 * @param {function} done callback takes string on error message
 *
 * Streams file from TempStore to a storage adapter, reports done or error
 *
 * > It will not error if storeName is not found
 */
FS.FileWorker.prototype.storeFile = function(fileObj, storeName, done) {

  // Look up the storage
  var storage = fileObj.collection.storesLookup[storeName];

  if (storage) {

    // Then write some data to it
    var writeStream = storage.adapter.createWriteStream(fileObj);

    writeStream.safeOn('error', function(err) {
      done('Write error');
    });

    writeStream.safeOn('stored', function() {
      // The file was stored
      done();
    });

    // We have a valid task and mounted file
    TempStore.createReadStream(fileObj).pipe();

  } else {
    // We dont have a store by that name - we dont retry
    done();
  }

};

/**
 * FS.FileWorker.removeTask
 * @param {FS.File} fileObj
 *
 * Removes the file from task queue and TempStore!
 */
FS.FileWorker.removeTask = function(fileObj) {
  // Remove all tasks on this file
  tasks.remove({ fileId: task.fileId });

  // Remove from tempstore
  TempStore.removeFile(fileObj);
};

/**
 * FS.FileWorker.Config
 * @param {object} options configuration object
 * @param {number} options.timeout time before a task is considered time outed
 * @param {number} options.expire a task will be remove from the system at this point
 * @param {number} options.limit limit of file workers allowed processing task
 * @param {number} options.sleep if a task failes it will be banned to sleep a while before retry
 *
 * Use this function to configure the behaviour of the FileWorker
 */
FS.FileWorker.Config = function(options) {
  // Extend the _config object
  _.extend(_config, options);

  // Check that the timeout is not less than the terminate
  if (_config.timeout < _config.terminate) {
    throw new Error('FileWorker: The timeout is lower than' +
            'terminate - The task should self terminate before' +
            'timeout!');
  }

  // Check that the timeout is not longer than the expiration
  if (_config.expire < _config.timeout) {
    throw new Error('FileWorker: The expire is lower than' +
            'timeout - The task should timeout before' +
            'it actually expires');
  }

  // Check that the sleep is not longer than the expiration
  if (_config.expire < _config.sleep) {
    throw new Error('FileWorker: The expire is lower than' +
            'sleep - The task should sleep longer' +
            'than the actual expire');
  }

  // Check that limit is 1 or more
  if (_config.limit < 1) {
    throw new Error('FileWorker: limit must be 1 or greater');
  }
};

FS.FileWorker.resetServerTasks = function(nodeId) {
  // First we make sure that there are no tasks assigned to this worker
  // we do this blindly

  // We fail tasks when resetting and therefor set them to sleep
  var nextRun = _addDate(_config.sleep);

  // DB call update
  tasks.update({
    // Selector
    $or: [
      // Select tasks with the nodeId and workerId
      { nodeId: nodeId },
    ]
  }, {
    // Modifier
    // Unset worker reference
    $unset: { nodeId: '', workerId: '' },
    // Set the pause until
    $set: { nextRun: nextRun },
    // increase runCount
    $inc: { runCount: 1}
  }, {
    // Options
    multi: true // This should affect all
  });
};

/**
 * FS.FileWorker.requestTask
 * @param {string} workerId
 *
 * @returns {null | task}
 */
FS.FileWorker.requestTask = function(workerId) {
  // Requesting a task is tricky if we have multiple worker nodes running
  // The first thing we do is set priority by runCount - so tasks failing
  // the most gets low priority
  //
  // 1. We seek out a fitting task
  //
  // return null if no tasks found
  //
  // 2. We try to reserve the task
  //
  // Try again if failed to reserve a task
  //
  // 3. Return the reserved task
  //
  // We dont have a max retry on this - we expect either null or a task to be
  // returned at some point

  var now = new Date();

  // 1. DB call
  // To avoid big aggregations we do a findOne as a lucky guess
  var task = tasks.findOne({
    // Select task that is no longer paused, we dont care that an other
    // worker is assigned - it must have failed
    nextRun: { $lt: now }
  }, {
    // Order by runCount - we cant do this in an update...
    sort: {
      runCount: 1
    }
  });

  // If no task found then fail request - we are done for now
  if (!task) {
    return null;
  }

  // 2. DB call
  // Next we select and update one task with:
  var affected = tasks.update({
    $and: [
      // We try to request the task
      { _id: task._id },
      // nextRun has passed, why? We could have a worker that snapped
      // the task from us, and we should not conflict
      { nextRun: { $lt: now } },
    ]
  }, {
  // and set { nodeId: _config.nodeId, workerId: workerId }
    $set: {
      // Set the reference to the current worker
      nodeId: _config.nodeId,
      workerId: workerId,
      // And set the timeout - allowing other workers after timeout
      nextRun: _addDate( _config.timeout )
    },
    $inc: {
      // We increase failure count if the last worker failed to release task
      runCount: 1
    }
  }, {
    // Just one, its default behaviour but we set it anyway
    multi: false
  });

  // Check if we did get the task
  if (affected !== 1) {
    // One of the other worker nodes got assigned to the task instead
    // We try again until we get a task or no tasks left
    return FS.FileWorker.requestTask(workerId);
  } else {
    // 3. DB call
    // We return the requested task if we found any
    return tasks.findOne({ nodeId: _config.nodeId, workerId: workerId });
  }
};

// Init the FileWorker
Meteor.startup(function() {
  console.log('CFS Worker node id: ' + _config.nodeId);
  // When booting the server we reset all server related tasks
  // If we use a fixed node id we want to have a clean start up
  // when requesting
  FS.FileWorker.resetServerTasks(_config.nodeId);
});


// The storage task is initialized calling the storeFile

// We can add one worker using the direct api - we have to enter a nodeId
// this is for future scaling of the workers, we could have more than one
// worker server spawned. The worker will start pulling tasks and start working
// on the storage task
// FS.FileWorker will initiate with an instance id for this server

// We have a way of initializing workers,
// We can do `worker1 = new FS.FileWorker();`

// We can stop a worker by `worker1.stop();`





/**
 * @public
 * @method FS.FileWorker.storeFile
 * @param {FS.File} File object to store
 * @param {FS.File} Name of store to target
 *
 * This function takes a file object and store name. It adds this as a file
 * worker task.
 * A file worker can then pull a task and handle that.
 */
FS.FileWorker.storeFile = function(fileObj, storeName) {

};






/**
 * @method FS.FileWorker.observe
 * @public
 * @param {FS.Collection} fsCollection
 * @returns {undefined}
 *
 * Sets up observes on the fsCollection to store file copies and delete
 * temp files at the appropriate times.
 */
FS.FileWorker.observe = function(fsCollection) {

  // Initiate observe for finding newly uploaded/added files that need to be stored
  // per store.
  _.each(fsCollection.options.stores, function(store) {
    var storeName = store.name;
    fsCollection.files.find(getReadyQuery(storeName), {
      fields: {
        copies: 0
      }
    }).observe({
      added: function(fsFile) {
        // added will catch fresh files
        FS.debug && console.log("FileWorker ADDED - calling saveCopy", storeName, "for", fsFile._id);
        saveCopy(fsFile, storeName);
      },
      changed: function(fsFile) {
        // changed will catch failures and retry them
        FS.debug && console.log("FileWorker CHANGED - calling saveCopy", storeName, "for", fsFile._id);
        saveCopy(fsFile, storeName);
      }
    });
  });

  // Initiate observe for finding files that have been stored so we can delete
  // any temp files
  fsCollection.files.find(getDoneQuery(fsCollection.options.stores)).observe({
    added: function(fsFile) {
      FS.debug && console.log("FileWorker ADDED - calling deleteChunks for", fsFile._id);
      FS.TempStore.removeFile(fsFile);
    }
  });

  // Initiate observe for catching files that have been removed and
  // removing the data from all stores as well
  fsCollection.files.find().observe({
    removed: function(fsFile) {
      FS.debug && console.log('FileWorker REMOVED - removing all stored data for', fsFile._id);
      //remove from temp store
      FS.TempStore.removeFile(fsFile);
      //delete from all stores
      _.each(fsCollection.options.stores, function(store) {
        store.remove(fsFile, {ignoreMissing: true});
      });
    }
  });
};

/**
 *  @method getReadyQuery
 *  @private
 *  @param {string} storeName - The name of the store to observe
 *
 *  Returns a selector that will be used to identify files that
 *  have been uploaded but have not yet been stored to the
 *  specified store.
 *
 *  {
 *    $where: "this.bytesUploaded === this.size",
 *    chunks: {$exists: true},
 *    'copies.storeName`: null,
 *    'failures.copies.storeName.doneTrying': {$ne: true}
 *  }
 */
function getReadyQuery(storeName) {
  var selector = {
    $where: "this.chunkSum === this.chunkCount"
  };

  selector['copies.' + storeName] = null;
  selector['failures.copies.' + storeName + '.doneTrying'] = {$ne: true};

  return selector;
}

/**
 *  @method getDoneQuery
 *  @private
 *  @param {Object} stores - The stores object from the FS.Collection options
 *
 *  Returns a selector that will be used to identify files where all
 *  stores have successfully save or have failed the
 *  max number of times but still have chunks. The resulting selector
 *  should be something like this:
 *
 *  {
 *    $and: [
 *      {chunks: {$exists: true}},
 *      {
 *        $or: [
 *          {
 *            $and: [
 *              {
 *                'copies.storeName': {$ne: null}
 *              },
 *              {
 *                'copies.storeName': {$ne: false}
 *              }
 *            ]
 *          },
 *          {
 *            'failures.copies.storeName.doneTrying': true
 *          }
 *        ]
 *      },
 *      REPEATED FOR EACH STORE
 *    ]
 *  }
 *
 */
function getDoneQuery(stores) {
  var selector = {
    $and: [
      {chunks: {$exists: true}}
    ]
  };

  // Add conditions for all defined stores
  for (var store in stores) {
    var storeName = store.name;
    var copyCond = {$or: [{$and: []}]};
    var tempCond = {};
    tempCond["copies." + storeName] = {$ne: null};
    copyCond.$or[0].$and.push(tempCond);
    tempCond = {};
    tempCond["copies." + storeName] = {$ne: false};
    copyCond.$or[0].$and.push(tempCond);
    tempCond = {};
    tempCond['failures.copies.' + storeName + '.doneTrying'] = true;
    copyCond.$or.push(tempCond);
    selector.$and.push(copyCond);
  }

  return selector;
}

/**
 * @method saveCopy
 * @private
 * @param {FS.File} fsFile
 * @param {string} storeName
 * @param {Object} options
 * @param {Boolean} [options.overwrite=false] - Force save to the specified store?
 * @returns {undefined}
 *
 * Saves to the specified store. If the
 * `overwrite` option is `true`, will save to the store even if we already
 * have, potentially overwriting any previously saved data. Synchronous.
 */
  var makeSafeCallback = function (callback) {
      // Make callback safe for Meteor code
      return Meteor.bindEnvironment(callback, function(err) { throw err; });
  };

function saveCopy(fsFile, storeName, options) {
  options = options || {};

  var storage = FS.StorageAdapter(storeName);
  if (!storage) {
    throw new Error('No store named "' + storeName + '" exists');
  }

  var targetStream = storage.adapter.createWriteStream(fsFile);

  targetStream.safeOn('error', function(err) {
    // TODO:
    console.log('GOT an error in stream while storeing to SA?');
    throw new err;
  });

  FS.debug && console.log('saving to store ' + storeName);

  // Pipe the temp data into the storage adapter
  FS.TempStore.createReadStream(fsFile).pipe(targetStream);


}
