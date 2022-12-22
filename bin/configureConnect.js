#!/usr/bin/env node

async function main() {
  try {
    const res = await fetch(
      'http://localhost:8083/connectors',
      {
        method: 'POST',
        body: JSON.stringify({
          name: 'file-sink-connector',
          config: { 
            'connector.class': 'org.apache.kafka.connect.file.FileStreamSinkConnector',
            'tasks.max': 1,
            'file': '/tmp/poc.sink.txt',
            'topics': 'signing-response',
            'key.converter': 'org.apache.kafka.connect.storage.StringConverter',
            'value.converter': 'io.confluent.connect.avro.AvroConverter',
            'value.converter.schema.registry.url': 'http://localhost:8081',
            'value.converter.schema.registry.subject': 'brave.cbp.SigningResultSet',
          },
        }),
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
        },
      }
    );

    if (!res.ok) {
      console.error('Unable to configure connector:', res.status, res.statusText);
      return 
    }
  
    console.log(res.body);
    const body = await res.json();
    console.log(JSON.stringify(body));
  } catch (e) {
    console.error(e);
  }
}

main();
