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
  // 5. TRIGGER N8N
  // n8n responds with request_id + driveLink
  // ──────────────────────────────
  console.log('Triggering n8n workflow...');

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

  console.log('n8n trigger status:', n8nRes.status);
  const n8nData = await n8nRes.json();                         // ✅ parse JSON
  console.log('n8n response:', JSON.stringify(n8nData));

  // ✅ extract requestId and driveLink from Airtable record returned by n8n
  const requestId = n8nData.fields?.request_unique_id || n8nData.request_unique_id || '';
  const driveLink = n8nData.fields?.service_request_url || n8nData.service_request_url || '';

  if (!requestId) throw new Error('No request_id returned from n8n!');

  console.log('Request ID :', requestId);
  console.log('Drive Link :', driveLink);

  // ──────────────────────────────
  // 6. POLL STATS WEBHOOK
  // Every 2 min until Completed
  // ──────────────────────────────
  console.log('\nPolling stats webhook every 2 min until Completed...');

  let isCompleted = false;
  let pollCount   = 0;
  let statsResult = {};

  while (!isCompleted) {

    pollCount++;
    console.log(`\n🔄 Poll attempt #${pollCount}...`);

    const statsRes = await fetch(
      `https://s1.boomerangserver.co.in/webhook/waterfall-request-stats?request_id=${requestId}`
    );

    statsResult = await statsRes.json();
    console.log('Request status:', statsResult.request_status);
    console.log('Stats:', JSON.stringify(statsResult));

    if (statsResult.request_status === 'Completed') {
      console.log('✅ Status = Completed!');
      isCompleted = true;
    } else {
      console.log(`⏳ Still "${statsResult.request_status}" — waiting 2 minutes...`);
      await new Promise(resolve => setTimeout(resolve, 120000));
    }
  }

  // ──────────────────────────────
  // 7. CALL OUTPUT WEBHOOK
  // ──────────────────────────────
  console.log('\nCalling output webhook...');

  const outputRes    = await fetch(
    `https://s1.boomerangserver.co.in/webhook/waterfalls-request-output?request_id=${requestId}`
  );
  const outputResult = await outputRes.json();
  console.log('Output response:', JSON.stringify(outputResult));

  // ──────────────────────────────
  // 8. SAVE FINAL OUTPUT
  // ──────────────────────────────
  await Actor.pushData({
    userId,
    runId,
    time,
    serviceTagName,
    rowCount,
    creditsCost,
    requestId,
    driveInputLink    : driveLink,
    driveOutputLink   : outputResult.webViewLink             || '',
    requestStatus     : statsResult.request_status           || '',
    totalProspects    : statsResult.total_prospects          || '',
    totalEmailFound   : statsResult.total_email_found        || '',
    totalEmailNotFound: statsResult.total_email_not_found    || ''
  });

  console.log('\n✅ Final output saved!');
  console.log('Drive Input Link  :', driveLink);
  console.log('Drive Output Link :', outputResult.webViewLink);
  console.log('Total Prospects   :', statsResult.total_prospects);
  console.log('Total Email Found :', statsResult.total_email_found);

} catch (err) {
  console.log('❌ Error:', err.message);
}

await Actor.exit();
