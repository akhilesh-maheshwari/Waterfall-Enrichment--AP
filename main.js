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

    const csvRes = await fetch(csvUrl);
    csvContent   = await csvRes.text();

    const allRows = csvContent.trim().split('\n');
    rowCount = allRows.length - 1;
    fileName = serviceTagName.replace(/[^a-zA-Z0-9]/g, '_') + '_' + new Date().toISOString().replace(/[:.]/g, '-') + '.csv';

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
  // Uploads CSV, creates Airtable record, submits to boomerang
  // Returns { request_id, driveLink }
  // ──────────────────────────────
  console.log('\nStep 1: Triggering n8n waterfall-input...');

  const n8nRes = await fetch(
    'https://n8n-internal.chitlangia.co/webhook/waterfall-input',
    {
      method : 'POST',
      headers: { 'Content-Type': 'application/json' },
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

  console.log('n8n step 1 status:', n8nRes.status);
  const n8nData = await n8nRes.json();
  console.log('n8n step 1 response:', JSON.stringify(n8nData));

  const request_id = String(n8nData.request_id || '');
  const driveLink   = n8nData.driveLink || '';

  if (!request_id) throw new Error('No request_id returned from n8n step 1!');

  console.log('Request ID :', request_id);
  console.log('Drive Link   :', driveLink);

  // ──────────────────────────────
  // 6. TRIGGER N8N — STEP 2
  // n8n polls boomerang every 2 min internally
  // Responds only when status = Completed
  // Returns { requestId, requestStatus, "Output Link" }
  // NOTE: Set a long timeout — this can take 10-30+ minutes
  // ──────────────────────────────
  console.log('\nStep 2: Triggering n8n polling workflow...');
  console.log('Waiting for Completed status (this may take several minutes)...');

  const pollRes = await fetch(
    'https://n8n-internal.chitlangia.co/webhook/waterfall-status',  // ← REPLACE with your actual polling webhook URL
    {
      method : 'POST',
      headers: { 'Content-Type': 'application/json' },
      body   : JSON.stringify({ request_id: request_id })
    }
  );

  console.log('n8n step 2 status:', pollRes.status);
  const pollData = await pollRes.json();
  console.log('n8n step 2 response:', JSON.stringify(pollData));

  const outputLink     = pollData['Output Link']    || pollData.outputLink    || '';
  const requestStatus  = pollData.requestStatus     || pollData.request_status || '';

  console.log('Request Status :', requestStatus);
  console.log('Output Link    :', outputLink);

  // ──────────────────────────────
  // 7. SAVE FINAL OUTPUT
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
  console.log('Input Link     :', driveLink);
  console.log('Output Link    :', outputLink);
  console.log('Status         :', requestStatus);

} catch (err) {
  console.log('❌ Error:', err.message);
}

await Actor.exit();
