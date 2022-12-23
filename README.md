# Kafka Connect & Schema Registry POC

Simple POC to demonstrate use of both Kafka Connect & Confluent Schema Registry

## Requirements

- Confluent Platform Community Version - https://docs.confluent.io/4.0.1/installation/installing_cp.html
- Confluent CLI - https://docs.confluent.io/confluent-cli/current/install.html
- Node.js

## Configuration

To enable Connect to write to a flat file, we need to enable `filestream-connectors`, which can be done by making a symbolic link in `/usr/share/java`, pointing to `/usr/share/filestream-connectors`.  The later should have been included in your installation of Confluent Platform.  You can verify that `/usr/share/java` is the correct plugin directory with the following:

- `confluent local current` - outputs the location of the configuration for currently running confluent services
- `cat <current_dir>/connect/connect.properties` - you should see `plugin.path` at the bottom of the output - default should be `/usr/share/java`, but if it differs, ensure you make the link mentioned above in the directory given here.

## Running

The first thing we need to do is start Kafka, Schema Registry & Connect:

`confluent local services connect start`

This will start all of the services Connect depends upon, and finally Connect.

### Connect Example

The simplest way to see connect in action is to:

- `npm run configure-connect` - Configure the sink connector
- `tail -f /tmp/poc.sink.txt` - watch the sink file for changes (in separate terminal)
- `npm run rspv1` - produce a new signing response message to Kafka

The above should demonstrate Connect writing data from the topic to the file on disk.  Configuring connect to do the same to database requires more configuration, but is in essence the same.  Note that we configure Avro deserializer in `bin/configureConnect.js`.  We could also write serialized Avro data directly if we want to.  In the case of a database, the deserializer is mandatory.

### Schema Registry Example

To see how the registry interact with our applications, do the following:

- `npm start` - Starts server
- `npm run rspv1` - Produce a v1 response to cluster - server and client will output result
- `npx run rspv2` - Produce a v2 response to cluster - server and client will output result

The application will use the schema registry at startup, and will rely on its local schema thereafter.  Please look in `src/index.js`, as there are quite a few comments explaining what's happening at the application level in this setup.

### Logs

If you want to watch the logs for Connect, Kafka, Schema Registry or any other services run via the confluent cli:

`confluent local services <service> log -f`

The `-f` option follows the log stream and is optional.

### Connect Output

Connect will write all of the messages produced to the `signing-response` topic to `/tmp/poc.sink.txt`.  You can watch Connect write to this file with: `tail -f /tmp/poc.sink.txt`.

### Shutdown

`confluent local destroy` - kills all running services & cleans up
