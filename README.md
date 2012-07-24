# General Attachments Worker

This worker proesses attachments.


## Create a new Worker

Define an object with two functions: _check_ and _process_.

Look at index.js for an example.


## Per Database Configuration

Configuration is done in a worker configuration document inside the target database.
The worker looks at all databases and only process if there exists such a configuration file.

A Worker Configuration File might look like this:

    {
      "_id": "worker-config/attachments",
      "_rev": "9-a653b27246b01cf9204fa9f5dee7cc64",
      "size": "135x135"
    }

You can update the config live so that all future processings will take the new configuration.


## Status Object

The worker updates a status object inside the document.
This makes it supereasy to monitor worker status as well as
it keeps an atomic lock when many workers listen to the same database.
Images are prozessed only once by comparing the attachments _revpos_ property
with the revpos property of the status object.

The status object of the worker looks like this:

    "worker_status": {
      "attachments": {
        "status": "completed",
        "revpos": 160
      }
    }

The status field can be _triggered_, _completed_ or _error_.

The worker status is scoped by the worker name in order to have many workers
processing the same document.


## Running the Worker

To start, this needs either the following environment variables set:

    export HOODIE_SERVER=http://example.org
    npm start


or pass them to the commandline:

    HOODIE_SERVER=http://example.org npm start



## License & Copyright

(c) 2012 Johannes J. Schmidt, null2 GmbH, Berlin

Licensed under the Apache License 2.0.Licensed under the Apache License 2.0.
