import { Actor } from 'apify';

await Actor.init();

try {

  // ──────────────────────────────
  // 1. GET INPUT
  // ──────────────────────────────
  const input          = await Actor.getInput();
  const entries        = input.entries               || '';
  const uploadedFile   = input.uploadedFile          || '';
  const serviceTagName = input.serviceRequestTagName || '';

  console.log('Tag Name:', serviceTagName);
  console.log('Entries provided:', entries ? 'Yes' : 'No');
  console.log('File URL provided:', uploadedFile ? 'Yes' : 'No');

  // ──────────────────────────────
  // 2. BUILD CSV CONTENT
  // ──────────────────────────────
  let csvContent = '';
  let fileName   = '';
  let rowCount   = 0;

  if (entries && entries.trim()) {

    console.log('Processing manual entries...');
    const lines = entries.trim().split('\n').map(l => l.trim()).filter(l => l);

    const validLines = [];
    for (const line of lines) {
      const cols = line.split(',');
      if (cols.length === 3) {
        validLines.push(cols.map(c => c.trim()).join(','));
      } else {
        console.log('Skipping invalid line:', line);
      }
    }

    csvContent = 'first_name,last_name,domain\n' + validLines.join('\n');
    rowCount   = validLines.length;
    fileName   = serviceTagName.replace(/[^a-zA-Z0-9]/g, '_') + '_' + new Date().toISOString().replace(/[:.]/g, '-') + '.csv';

    console.log('Valid rows:', rowCount);
    console.log('CSV preview:\n', csvContent.split('\n').slice(0, 3).join('\n'));

  } else if (uploadedFile && uploadedFile.trim()) {

    console.log('Processing file URL...');
    let csvUrl = uploadedFile.trim();

    if (csvUrl.includes('docs.google.com/spreadsheets')) {
      const match = csvUrl.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
      if (match) {
        csvUrl = `https://docs.google.com/spreadsheets/d/${match[1]}/export?format=csv&gid=0`;
        console.log('Converted URL:', csvUrl);
      }
    }

    const csvRes  = await fetch(csvUrl);
    csvContent    = await csvRes.text();

    const allRows = csvContent.trim().split('\n');
    rowCount      = allRows.length - 1;
    fileName      = serviceTagName.replace(/[^a-zA-Z0-9]/g, '_') + '_' + new Date().toISOString().replace(/[:.]/g, '-') + '.csv';

    console.log('Downloaded rows:', rowCount);
    console.log('CSV preview:\n', allRows.slice(0, 3).join('\n'));

  } else {
    throw new Error('Please provide either manual entries or a file URL!');
  }

  // ──────────────────────────────
  // 3. GET APIFY RUN DETAILS
  // ──────────────────────────────
  const env    = Actor.getEnv();
  const userId = env.userId     || 'unknown';
  const runId  = env.actorRunId || 'unknown';
  const now    = new Date();
  const time   = now.toLocaleString('en-US', {
    year    : 'numeric',
    month   : 'long',
    day     : 'numeric',
    hour    : 'numeric',
    minute  : '2-digit',
    hour12  : true,
    timeZone: 'Asia/Kolkata'
  });

  console.log('User ID:', userId);
  console.log('Run ID :', runId);
  console.log('Time   :', time);

  // ──────────────────────────────
  // 4. CALCULATE COST
  // ──────────────────────────────
  const creditsCost = parseFloat((rowCount * 0.015).toFixed(3));
  console.log('Row count  :', rowCount);
  console.log('Credits cost: $', creditsCost);

  // ──────────────────────────────
  // 5. TRIGGER N8N — STEP 1
  // Uploads CSV, creates Airtable record, submits to Boomerang
  // n8n responds immediately with { request_id, driveLink }
  // via "Respond to Webhook" node (before the Wait node)
  // Timeout: 30 seconds
  // ──────────────────────────────
  console.log('\nStep 1: Triggering n8n waterfall-input...');

  let n8nRes;
  try {
    n8nRes = await fetch(
      'https://n8n-internal.chitlangia.co/webhook/waterfall-input',
      {
        method : 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal : AbortSignal.timeout(30000), // 30 seconds
        body   : JSON.stringify({
          userId,
          runId,
          time,
          serviceTagName,
          rowCount,
          creditsCost,
          csvContent,
          uploadedFile,
          fileName
        })
      }
    );
  } catch (fetchErr) {
    throw new Error(`Step 1 fetch failed: ${fetchErr.message}`);
  }

  console.log('n8n step 1 status:', n8nRes.status);

  // Read body ONCE only
  const n8nText = await n8nRes.text();
  console.log('n8n step 1 raw response:', n8nText);

  if (!n8nRes.ok) {
    throw new Error(`Step 1 failed with status ${n8nRes.status}. Response: ${n8nText.slice(0, 200)}`);
  }

  let n8nData;
  try {
    n8nData = JSON.parse(n8nText);
  } catch (parseErr) {
    throw new Error(`Step 1 JSON parse failed. Raw response: ${n8nText.slice(0, 200)}`);
  }

  console.log('n8n step 1 response:', JSON.stringify(n8nData));

  const request_id = String(n8nData.request_id || '');
  const driveLink  = n8nData.driveLink || '';

  if (!request_id) throw new Error('No request_id returned from n8n step 1!');

  console.log('Request ID :', request_id);
  console.log('Drive Link :', driveLink);

  // ──────────────────────────────
  // 6. POLL BOOMERANG DIRECTLY — STEP 2
  // Polls Boomerang every 2 min until status = Completed
  // Infinite polling — no timeout, runs until done
  // ──────────────────────────────
  console.log('\nStep 2: Polling Boomerang directly for status...');
  console.log('Polling every 2 minutes until Completed...');

  const POLL_INTERVAL_MS = 2 * 60 * 1000; // 2 minutes

  let outputLink    = '';
  let requestStatus = '';
  let attempts      = 0;

  while (true) {

    attempts++;
    console.log(`\nPoll attempt ${attempts}...`);

    let boomerangRes;
    try {
      boomerangRes = await fetch(
        `https://s1.boomerangserver.co.in/webhook/waterfall-request-stats?request_id=${request_id}`,
        {
          method : 'GET',
          signal : AbortSignal.timeout(15000) // 15 sec per poll request
        }
      );
    } catch (fetchErr) {
      console.log(`Poll attempt ${attempts} fetch failed: ${fetchErr.message}, retrying in 2 min...`);
      await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
      continue;
    }

    const boomerangText = await boomerangRes.text();
    console.log(`Boomerang raw response (attempt ${attempts}):`, boomerangText);

    let boomerangData;
    try {
      boomerangData = JSON.parse(boomerangText);
    } catch (e) {
      console.log('Boomerang JSON parse failed, retrying in 2 min...');
      await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
      continue;
    }

    requestStatus = boomerangData.requestStatus || boomerangData.status      || '';
    outputLink    = boomerangData['Output Link'] || boomerangData.webViewLink || boomerangData.outputLink || '';

    console.log(`Status      : ${requestStatus}`);
    console.log(`Output Link : ${outputLink}`);

    if (requestStatus === 'Completed') {
      console.log('Boomerang processing complete!');
      break;
    }

    console.log(`Not completed yet (status: ${requestStatus}), waiting 2 minutes...`);
    await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
  }

  // ──────────────────────────────
  // 7. TRIGGER N8N — STEP 3
  // Sends final outputLink + all details back to n8n
  // n8n output webhook updates Airtable / notifies user
  // Timeout: 30 seconds
  // ──────────────────────────────
  console.log('\nStep 3: Sending output to n8n output webhook...');

  let outputRes;
  try {
    outputRes = await fetch(
      'https://n8n-internal.chitlangia.co/webhook/waterfall-output',
      {
        method : 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal : AbortSignal.timeout(30000), // 30 seconds
        body   : JSON.stringify({
          userId,
          runId,
          time,
          serviceTagName,
          rowCount,
          creditsCost,
          request_id,
          driveInputLink  : driveLink,
          driveOutputLink : outputLink,
          requestStatus
        })
      }
    );

    const outputText = await outputRes.text();
    console.log('n8n step 3 status:', outputRes.status);
    console.log('n8n step 3 response:', outputText);

  } catch (fetchErr) {
    // Don't throw — output is already done, just log warning
    console.log(`Warning: Step 3 fetch failed: ${fetchErr.message}`);
    console.log('Continuing to save output anyway...');
  }

  // ──────────────────────────────
  // 8. SAVE FINAL OUTPUT TO APIFY DATASET
  // ──────────────────────────────
  await Actor.pushData({
    userId,
    runId,
    time,
    serviceTagName,
    rowCount,
    creditsCost,
    request_id,
    driveInputLink  : driveLink,
    driveOutputLink : outputLink,
    requestStatus
  });

  console.log('\n✅ Final output saved!');
  console.log('Request ID   :', request_id);
  console.log('Input Link   :', driveLink);
  console.log('Output Link  :', outputLink);
  console.log('Status       :', requestStatus);

} catch (err) {
  console.log('❌ Error:', err.message);
}

await Actor.exit();
