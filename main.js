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
  // $15 per 1000 inputs = $0.015 per row
  // ──────────────────────────────
  const creditsCost = parseFloat((rowCount * 0.015).toFixed(2));
  console.log('Row count  :', rowCount);
  console.log('Credits cost: $', creditsCost);

  // ──────────────────────────────
  // 5. TRIGGER N8N
  // Send everything to n8n
  // ──────────────────────────────
  console.log('Triggering n8n workflow...');

  const n8nRes = await fetch(
    'https://n8n-internal.chitlangia.co/webhook/waterfall-input', // ← paste your n8n webhook URL here
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
        csvContent,  // ← full CSV data for Google Drive
        fileName     // ← file name for Google Drive
      })
    }
  );

  console.log('n8n trigger status:', n8nRes.status);
  const n8nText = await n8nRes.text();
  console.log('n8n response:', n8nText);

  if (n8nRes.status === 200) {
    console.log('✅ n8n triggered successfully! n8n will handle the rest.');
  } else {
    console.log('❌ n8n trigger failed:', n8nText);
  }

} catch (err) {
  console.log('❌ Error:', err.message);
}

await Actor.exit();
