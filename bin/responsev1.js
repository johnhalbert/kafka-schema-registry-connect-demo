#!/usr/bin/env node

const { encodeResponseV1 } = require('../src/response');
const { produce } = require('../src/kafka');
const { v4: uuid } = require('uuid');

async function main() {
  const response = {
    request_id: uuid(),
    data: [{
      signed_tokens: [],
      public_key: '',
      proof: '',
      status: 'ok',
      associated_data: {},
    }, {
      signed_tokens: [],
      public_key: '',
      proof: '',
      status: 'ok',
      associated_data: {},
    }],
  };

  try {
    const encodedResponse = await encodeResponseV1(response);
    await produce(response.request_id, encodedResponse);
    console.log(JSON.stringify(response));
  } catch (e) {
    console.error(e);
  }
}

main();
