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
  // 3. SAVE CSV TO GOOGLE DRIVE
  // ──────────────────────────────
  console.log('Saving to Google Drive...');
  console.log('File name:', fileName);

  const gasRes = await fetch(
    'https://script.google.com/macros/s/AKfycbyrkTBophapts2XV4ZA2HxmzUgB26wfhcZmm7qAz7wuRckW5suJSENN6GL_G4zeFx7I/exec',
    {
      method  : 'POST',
      redirect: 'follow',
      headers : { 'Content-Type': 'text/plain' },
      body    : JSON.stringify({ csvContent, fileName })
    }
  );

  const gasResult = JSON.parse(await gasRes.text());
  console.log('Drive result:', JSON.stringify(gasResult));

  if (gasResult.status !== 'success') {
    throw new Error('Google Drive error: ' + gasResult.message);
  }

  const driveLink = gasResult.fileLink;
  console.log('Drive link:', driveLink);

  // ──────────────────────────────
  // 4. CALCULATE COST
  // ──────────────────────────────
  const creditsCost = parseFloat((rowCount * 0.015).toFixed(2));
  console.log('Row count:', rowCount);
  console.log('Credits cost:', creditsCost);

  // ──────────────────────────────
  // 5. GET APIFY RUN DETAILS
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
  // 6. SAVE TO AIRTABLE
  // ──────────────────────────────
  console.log('Saving to Airtable...');

  const atRes = await fetch(
    'https://api.airtable.com/v0/appCuadMXrDqpfaDV/tblD3UXc3tYW0mOdT',
    {
      method : 'POST',
      headers: {
        'Authorization': 'Bearer pat4bRijwFM7m1t9u.c5fa218d14d840e4180f628656b63c163ce71bd8d01881d971ee96fe2d939dd8',
        'Content-Type' : 'application/json'
      },
      body: JSON.stringify({
        fields: {
          user_unique_id           : userId,
          request_unique_id        : runId,
          time_of_request          : time,
          service_request_tag_name : serviceTagName,
          service_request_size     : rowCount,
          service_cost             : creditsCost,
          service_request_url      : driveLink,
          service_option_1         : 'pro',
          service_name             : 'Waterfall Enrichment',
          request_source           : 'Waterfall_enrichment_AP'
        }
      })
    }
  );

  const atResult = await atRes.json();
  console.log('Airtable response:', JSON.stringify(atResult));

  if (atResult.id) {
    console.log('✅ Airtable record saved! ID:', atResult.id);
  } else {
    console.log('❌ Airtable error:', JSON.stringify(atResult));
  }

  // ──────────────────────────────
  // 7. SEND TO MAIN WEBHOOK
  // ──────────────────────────────
  console.log('Sending to Webhook...');

  const webhookRes = await fetch(
    'https://s1.boomerangserver.co.in/webhook/waterfall-live',
    {
      method : 'POST',
      headers: { 'Content-Type': 'application/json' },
      body   : JSON.stringify({
        request_unique_id        : runId,
        time_of_request          : time,
        service_name             : 'Waterfall Enrichment',
        service_option_1         : 'pro',
        service_request_tag_name : serviceTagName,
        size                     : rowCount,
        service_request_url      : driveLink,
        source                   : 'Waterfall_enrichment_AP'
      })
    }
  );

  console.log('Webhook status:', webhookRes.status);
  const webhookText = await webhookRes.text();
  console.log('Webhook response:', webhookText);

  if (webhookRes.status !== 200) {
    throw new Error('Main webhook error: ' + webhookText);
  }

  console.log('✅ Main webhook sent successfully!');

  // ──────────────────────────────
  // 8. GET REQUEST ID
  // ──────────────────────────────
  const webhookResult = JSON.parse(webhookText);
  const requestId     = webhookResult.request_id || '';

  if (!requestId) {
    throw new Error('⚠️ No request_id found in webhook response!');
  }

  console.log('Request ID:', requestId);

  // ──────────────────────────────
  // 9. POLL STATS WEBHOOK
  // Every 3 sec jab tak "Completed" na aaye
  // ──────────────────────────────
  console.log('Polling stats webhook every 2 min until Completed...');

  let isCompleted = false;
  let pollCount   = 0;

  while (!isCompleted) {

    pollCount++;
    console.log(`\n🔄 Poll attempt #${pollCount}...`);

    const statsRes  = await fetch(
      `https://s1.boomerangserver.co.in/webhook/waterfall-request-stats?request_id=${requestId}`,
      {
        method : 'GET',
        headers: { 'Content-Type': 'application/json' }
      }
    );

    console.log('Stats webhook status:', statsRes.status);
    const statsText = await statsRes.text();
    console.log('Stats webhook response:', statsText);

    if (statsRes.status !== 200) {
      console.log('❌ Stats webhook returned non-200, stopping poll.');
      break;
    }

    const statsResult = JSON.parse(statsText);
    console.log('Request status:', statsResult.request_status);

    if (statsResult.request_status === 'Completed') {
      console.log('✅ Status = Completed! Stopping poll.');
      isCompleted = true;
    } else {
      console.log(`⏳ Still "${statsResult.request_status}" — waiting 2 minitues...`);
      await new Promise(resolve => setTimeout(resolve, 120000));
    }

  }

  // ──────────────────────────────
  // 10. CALL OUTPUT WEBHOOK
  // (only after Completed)
  // ──────────────────────────────
  if (isCompleted) {

    console.log('\nSending to output webhook...');

    const outputRes  = await fetch(
      `https://s1.boomerangserver.co.in/webhook/waterfalls-request-output?request_id=${requestId}`,
      {
        method : 'GET',
        headers: { 'Content-Type': 'application/json' }
      }
    );

    console.log('Output webhook status:', outputRes.status);
    const outputText = await outputRes.text();
    console.log('Output webhook response:', outputText);

    if (outputRes.status === 200) {
      console.log('✅ Output webhook sent successfully!');
    } else {
      console.log('❌ Output webhook error:', outputText);
    }

  } else {
    console.log('⚠️ Skipping output webhook — request did not complete.');
  }

} catch (err) {
  console.log('❌ Error:', err.message);
}

await Actor.exit();
