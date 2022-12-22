#!/usr/bin/env node

const { encodeResponseV2 } = require('../src/response');
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
      valid_to: '',
      valid_from: '',
    }, {
      signed_tokens: [],
      public_key: '',
      proof: '',
      status: 'ok',
      associated_data: {},
      valid_to: '',
      valid_from: '',
    }],
  };

  try {
    const encodedResponse = await encodeResponseV2(response);
    await produce(response.request_id, encodedResponse);
    console.log(JSON.stringify(response));
  } catch (e) {
    console.error(e);
  }
}

main();
