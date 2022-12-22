const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry');
const registry = new SchemaRegistry({ host: 'http://localhost:8081' });

async function encodeResponseV1(response) {
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

  try {
    // See notes in src/index.js for details on registering schemas
    const { id } = await registry.register({
      type: SchemaType.AVRO,
      schema: JSON.stringify(SIGN_RESPONSE_SCHEMA),
    }, {
      subject: 'signing-response-value',
      compatibility: 'FORWARD_TRANSITIVE',
    });

    // Note: we didn't need to map this, necessarily.  In the server we do, we need a buffer to
    // encode fields of type bytes, and we can't represent a buffer in JSON.  Here we're doing
    // this for consistency more than anything.
    response.data = response.data.map(d => ({
      ...d,
      associated_data: Buffer.from(JSON.stringify(d.associated_data)),
    }));

    console.log(`Encoding repsonse schema version ${id}`);
    return registry.encode(id, response);
  } catch (e) {
    console.error(e);
  }
}

async function encodeResponseV2(response) {
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
              { name: "valid_to", type: ["null", "string"], default: null },
              { name: "valid_from", type: ["null", "string"], default: null },
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

  try {
    // See notes in src/index.js for details on registering schemas
    const { id } = await registry.register({
      type: SchemaType.AVRO,
      schema: JSON.stringify(SIGN_RESPONSE_SCHEMA),
    }, {
      subject: 'signing-response-value',
      compatibility: 'FORWARD_TRANSITIVE',
    });

    // Note: we didn't need to map this, necessarily.  In the server we do, we need a buffer to
    // encode fields of type bytes, and we can't represent a buffer in JSON.  Here we're doing
    // this for consistency more than anything.
    response.data = response.data.map(d => ({
      ...d,
      associated_data: Buffer.from(JSON.stringify(d.associated_data)),
    }));

    console.log(`Encoding repsonse schema version ${id}`);
    return registry.encode(id, response);
  } catch (e) {
    console.error(e);
  }
}

module.exports = {
  encodeResponseV1,
  encodeResponseV2,
};
