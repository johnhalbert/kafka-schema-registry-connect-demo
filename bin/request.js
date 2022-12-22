#!/usr/bin/env node

const { v4: uuid } = require('uuid');

async function main() {
  try {
    const body = JSON.stringify({
      request_id: uuid(),
      data: [{
        associated_data: {},
        blinded_tokens: [],
        issuer_type: '',
        issuer_cohort: 0,
      }, {
        associated_data: {},
        blinded_tokens: [],
        issuer_type: '',
        issuer_cohort: 0,
      }]
    });

    const res = await fetch(
      'http://localhost:3000/sign-request',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body,
      },
    );

    if (!res.ok) {
      console.error(res.status, res.statusText);
    } else {
      console.log(body);
    }
  } catch (e) {
    console.error(e);
  }
}

main();
