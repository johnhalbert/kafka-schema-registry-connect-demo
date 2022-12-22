//
// -----[ Kafka ]-----
//

// 
// Schema definitions
// ==================
//
// These can also be wrapped with `avro.Type.fromSchema`, like we currently do.  Where the schema
// is passed to @kafkajs/confluent-schema-registry, the `.schema` method will provide the original
// schema.
//
// For demo, this is just a plain object.
//

const SIGN_REQUEST_SCHEMA = {
  namespace: "brave.cbp",
  type: "record",
  doc: "Top level request containing the data to be processed, as well as any top level metadata for this message.",
  name: "SigningRequestSet",
  fields: [
    { name: "request_id", type: "string" },
    {
      name: "data",
      type: {
        type: "array",
        items: {
          namespace: "brave.cbp",
          type: "record",
          name: "SigningRequest",
          fields: [
            {
              name: "associated_data",
              type: "bytes",
              doc: "contains METADATA",
            },
            {
              name: "blinded_tokens",
              type: {
                type: "array",
                items: {
                  name: "blinded_token",
                  type: "string",
                  namespace: "brave.cbp",
                },
              },
            },
            { name: "issuer_type", type: "string" },
            { name: "issuer_cohort", type: "int" },
          ],
        },
      },
    },
  ],
};

const SIGN_RESPONSE_SCHEMA = {
  namespace: "brave.cbp",
  type: "record",
  doc: "Top level request containing the data to be processed, as well as any top level metadata for this message.",
  name: "SigningResultSet",
  fields: [
    { name: "request_id", type: "string" },
    {
      name: "data",
      type: {
        type: "array",
        items: {
          namespace: "brave.cbp",
          type: "record",
          name: "SigningResult",
          fields: [
            {
              name: "signed_tokens",
              type: {
                type: "array",
                items: {
                  name: "signed_token",
                  type: "string",
                },
              },
            },
            { name: "public_key", type: "string" },
            { name: "proof", type: "string" },
            {
              name: "status",
              type: {
                name: "SigningResultStatus",
                type: "enum",
                symbols: ["ok", "invalid_issuer", "error"],
              },
            },
            {
              name: "associated_data",
              type: "bytes",
              doc: "contains METADATA",
            },
          ],
        },
      },
    },
  ],
};

// 
// Registry client setup
// =====================
//
// Luckily, in JavaScript KafkaJS, which we use, provides a client for the schema registry.
//

const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry');
const SCHEMA_REGISTRY_URL = 'http://localhost:8081';
const registry = new SchemaRegistry({ host: SCHEMA_REGISTRY_URL });

//
// Producer & Consumer Setup
// =========================
//
// Nothing new here. Expecting Kafka to be available locally configured in the usual way.
//

const { Kafka } = require('kafkajs');
const KAFKA_BROKER_URL = 'localhost:9092';
const kafka = new Kafka({
  clientId: 'request-producer',
  brokers: [KAFKA_BROKER_URL], 
});
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'response-consumer' });

async function initKafka() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'signing-response' });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const decoded = await decodeResponse(message.value);
      console.log(JSON.stringify(decoded));
    },
  });
}

// 
// Encoding & Decoding Data
// ========================
//
// We have two options here for encoding this data:
//
// 1. Encode using the latest version of the schema:
//
//   ```
//     const id = await registry.getLatestSchemaId('signing-request-value');
//   ```
//
// 2. Encode using the schema we know about at build/compile time:
//
//   ```
//     const id = await registry.getRegistryIdBySchema(
//       'signing-request-value',
//       {
//         type: SchemaType.AVRO,
//         schema: JSON.stringify(SINGING_REQUEST_SCHEMA)
//       }
//     );
//   ```
//
// With Schema Registry, we configure a "subject" (schema) with compatibility rules. Forward,
// Backward or Full.  Each of these can also be set as Transitive, meaning compatibility checks
// are made against all previous versions of the schema rather than just the one prior.
//
// This really informs us, not only about how we expect data to flow through the system, but also
// about convention and policy we should adopt for our applications.
//
// With Forward compatibility, we lose the ability to deserialize prior data.  In that case we
// care just about now and the future.  We also must keep producers up to date with the latest
// version of the schema.  When a new version of the schema is introduced, all producers writing
// that schema must be updated first. This is by convention, we have to enforce this ourselves.
// The reason for this is that consumers, once updated, cannot tolerate older data.  So we must
// ensure no producers will introduce data encoded with a previous schema version. The benefit
// of forward compatibility is that consumers can lag behind, because they're future-proof.  But
// producers cannot or else we risk failing.
//
// With Backward compatibility this is reversed.  Consumers must be updated first before schema
// changes are introduced, as they cannot tolerate data serialized with new schemas.  Producers
// in this case can lag behind, but are less likely to as they express the existence of the new
// schema in the system in the first place.
//
// What does that mean for encoding & decoding data?
//
// First, we should note that in example one, we forego caching, which is bad.  Assuming we've
// planned and implemented processes that satisfy the above considerations, we can always use
// the schema we know about locally at compile/build time.  This is by virtue of having chosen
// compatibility rules and ensured the above strategies are in place.  Once done, we know that
// the version of the schema we have locally, whether we're a consumer or a producer, will work
// for our use case without issue.
//
// Our system should be organized in such a way that schema
// changes are applied in a systematic fashion so that we don't disrupt safe operation of
// services. In a forward compatible system, we will always know about the latest version of the
// schema by convention.  In a backward compatible system, we will also likely know about the
// latest version, but could possibly not depending on our needs.
//
// The main benefits we get from this are:
//
// 1. Caching - producers can check at startup that they have a valid schema, get the appropriate
//              schema id and forget about it.  Encoding can be done safely without overhead.
//
// 2. Sanity - What we see is what we get.  We don't have to think about versioning during
//             development.  We should have processes in place, that guarantee we can forego the
//             overhead required when thinking about the complexities of compatibility and data
//             in motion in our system.  You can get turned around relatively quickly, as there
//             are multiple ways to view the system - from the schema perspective, the data
//             perspective, the encoding perspective, the decoding perspective, etc.  And
//             combinations of these such that it can be confusing easily. We want to avoid this.
//
// Another thing to note is that, for many of our services, we use strongly typed languages. This
// example in JavaScript does not show this, but when we have a type, for example in Typescript
// we would have:
//
// type SigningRequest = {
//   request_id: string;
//   data: {
//     associated_data: Buffer;
//     blinded_tokens: string[];
//     issuer_type: string;
//     issuer_cohort: string;
//   }[]
// };
//
// It's a benefit to us that we can guarantee the current version of our schema and this match,
// and that data can be seraizlied successfully using this type, as well as deserialized into
// this type.
//
// More advanced setups would incorporate a central location for schemas, as well as tooling that
// would provide native types automatically for our applications.  Adoption of a new schema
// version in this type of setup would only require updating the dependency providing the
// schemas, and evaluating the use of that type in our application code to ensure we're using
// fields on the type appropriately (e.g. we ensure we are not accessing a dropped field, for
// example).
//
// And, also worth noting, even in languages that lack strong typing, our code still expresses
// an expectation based on the schema, on fields and the types the values those fields contain.
// So, even in a language lacking strong typing, we still have typing.  The above constraints
// are just left to us to ensure are made correct.
// 
  
async function encodeRequest(request) {
  request.data = request.data.map(d => ({
    ...d,
    associated_data: Buffer.from(JSON.stringify(d.associated_data)),
  }));

  const id = await registry.getRegistryIdBySchema(
    'signing-request-value',
    {
      type: SchemaType.AVRO,
      schema: JSON.stringify(SIGN_REQUEST_SCHEMA)
    }
  );
  return await registry.encode(id, request);
}

function decodeResponse(response) {
  return registry.decode(
    response,
    {
      [SchemaType.AVRO]: {
        readerSchema: SIGN_RESPONSE_SCHEMA,
        subject: 'signing-response-value',
      }
    }
  );
}


// 
// Schema Initialization
// =====================
//
// We initialize the schemas we intend to produce or consume at startup.  If we encounter an
// issue here, we fail fast (in main below) and note the error received.  Changes to schemas
// are only introduced by producers, so it's important they make this check before producing
// any values with the schema.  Consumers should likewise check, especially in a backward
// compatible topic where they will be the first to roll out the change.  We don't want to
// update all consumers only to find out when we roll out the producers that we have an
// incompatible schema version.  For more on this, see above in Encoding & Decoding Data
// section to understand the implications of schema migrations in the context of consumers as
// well as producers.
//
// We _should_ have organized ourselves in such a way that consumers will not encounter failure.
// This is achieved through our development & deployment strategies more so than with the Schema
// Registry.  The Schema Registry just informs us how we should conduct ourselves based on our
// use-case.
//
// By convention, if we have forward compatibility enabled, schema migrations first require all
// producers to update and use the new version of the schema.  In backwards compatibility,
// consumers must update first.
//
// Depending on changes between this schema and previous versions, the registration call could
// fail. If we have a failure here, it's because we've introduced an incompatible change.  If we
// present a previous version of the schema, the Schema Registry will simply respond with the id of
// that version and the call will succeed.  This also highlights why conventions outlined in Encode
// & Decode section are so important.  If reverting to a previous version of the schema, and that
// change will cause failure, the schema registry won't protect us at runtime. The second call
// below ensures that we have a compatible version of the schema locally.
//
// We should also have processes in place to check schema compatibility during development, prior
// to release.  We shouldn't have failures here if we've done things right.  Failures here express
// a failure to take the appropriate steps when implementing changes.  This also highlights the
// usefulness of a central location for schemas.  We can introduce the required changes there
// first, and have automation in place to make checks that the new version is compatible with
// previous version in the way we expect.
// 
// In the Schema Registry, we call schemas "subjects".  Connect, by convention, wants to derive
// subject name as `<topic>-value`, and optionally `<topic>-key` if our key is in a format handled
// by the Schema Registry.  Below you'll see we need to specify the subject, as the @kafkajs
// implementation uses the name provided in the schema itself to derive the subject name.
//
// In general, within the Kafka ecosystem, the convention that I've seen has always been to use
// either `<topic>-value` and `<topic>-key`, or simply `<topic>`.  For best results, we should
// adopt one of these conventions.
//
    
async function registerSchema(schema, subject, compatibility) {
  // This gives us the globally unique id for this schema.
  //
  // I believe the particulars of this implementation is such that you get the global id of the
  // _latest_ version of the schema, even if you've presented an older version of the schema.  If
  // queried directly, however, you would get back the version of the presented schema.  So, if you
  // presented an older version, you'd get a prior id, not the latest.
  //
  // Schema Registry registration endpoint:
  // POST - http://localhost:8081/subjects/<subject>/versions
  const { id } = await registry.register({
    type: SchemaType.AVRO,
    schema: JSON.stringify(schema),
  }, {
    subject,
    compatibility,
  });

  // Because of the above, we should explicitly check if the version of the schema we have is
  // compatible with the latest version.
  const res = await fetch(
    `http://localhost:8081/compatibility/subjects/${subject}/versions`,
    {
      method: 'POST',
      body: JSON.stringify({
        schema: JSON.stringify(schema)
      }),
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
      },
    }
  );

  if (!res.ok)
    throw new Error(`${res.status} - ${res.statusText}`);

  const body = await res.json();
  if (!body.is_compatible)
    throw new Error(`Incompatible schema version for subject ${subject}`);

  return id;
}

//
// -----[ Server ]-----
//

async function createServer() {
  try {
    // Make connections, begin consuming, etc.
    await initKafka();

    // Register schemas
    await registerSchema(SIGN_REQUEST_SCHEMA, 'signing-request-value', 'FORWARD_TRANSITIVE');
    await registerSchema(SIGN_RESPONSE_SCHEMA, 'signing-response-value', 'FORWARD_TRANSITIVE');

    const express = require('express');
    const bodyParser = require('body-parser');
    const app = express();

    app.use(bodyParser.json());
    app.post('/sign-request', async function(req, res) {
      try {
        const key = req.body.request_id;
        const value = await encodeRequest(req.body);

        await producer.send({
          topic: 'signing-request',
          messages: [{ key, value }],
        });
        console.log(JSON.stringify(req.body));
        res.status(200).end();
      } catch (e) {
        console.error(e);
        res.status(500).end();
      }
    });

    app.listen(3000, () => console.log('Listening :3000'));
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
}

createServer();

// 
// Cleanup
//

async function cleanup() {
  // Clean up on exit
  console.log('Closing connections...');
  await producer.disconnect();
  await consumer.disconnect();
  console.log('Bye :)');
  process.exit(0);
}

process.on('beforeExit', cleanup);
process.on('SIGINT', cleanup);
process.on('SIGTERM', cleanup);
