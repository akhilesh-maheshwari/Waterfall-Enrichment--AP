import { Actor } from 'apify';

await Actor.init();

try {

  // ──────────────────────────────
  // 1. GET INPUT
  // ──────────────────────────────
  const input = await Actor.getInput();

  const firstName          = input.firstName          || '';
  const lastName           = input.lastName           || '';
  const domain             = input.domain             || '';
  const serviceTagName     = input.serviceRequestTagName || '';
  let   csvUrl             = input.uploadedFile || input.fileUrl || input.csvUrl || '';

  console.log('First Name:', firstName);
  console.log('Last Name :', lastName);
  console.log('Domain    :', domain);
  console.log('Tag Name  :', serviceTagName);
  console.log('CSV URL   :', csvUrl);

  // ──────────────────────────────
  // 2. CONVERT GOOGLE SHEETS URL
  // ──────────────────────────────
  if (csvUrl.includes('docs.google.com/spreadsheets')) {
    const match = csvUrl.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
    if (match) {
      csvUrl = `https://docs.google.com/spreadsheets/d/${match[1]}/export?format=csv&gid=0`;
      console.log('Converted URL:', csvUrl);
    }
  }

  // ──────────────────────────────
  // 3. SAVE CSV TO GOOGLE DRIVE
  // ──────────────────────────────
  console.log('Saving CSV to Google Drive...');
  const gasRes = await fetch(
    'https://script.google.com/macros/s/AKfycbyrkTBophapts2XV4ZA2HxmzUgB26wfhcZmm7qAz7wuRckW5suJSENN6GL_G4zeFx7I/exec',
    {
      method  : 'POST',
      redirect: 'follow',
      headers : { 'Content-Type': 'text/plain' },
      body    : JSON.stringify({ firstName, lastName, domain, csvUrl })
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
  // 4. COUNT CSV ROWS
  // ──────────────────────────────
  console.log('Counting rows...');
  const csvRes   = await fetch(csvUrl);
  const csvText  = await csvRes.text();
  const rowCount = csvText.trim().split('\n').length - 1;
  console.log('Row count (minus header):', rowCount);

  // ──────────────────────────────
  // 5. CALCULATE COST
  // ──────────────────────────────
  const creditsCost = parseFloat((rowCount * 0.01).toFixed(2));
  console.log('Credits cost:', creditsCost);

  // ──────────────────────────────
  // 6. GET APIFY RUN DETAILS
  // ──────────────────────────────
  const env    = Actor.getEnv();
  const userId = env.userId     || 'unknown';
  const runId  = env.actorRunId || 'unknown';
  const time   = new Date().toISOString();

  console.log('User ID:', userId);
  console.log('Run ID :', runId);
  console.log('Time   :', time);

  // ──────────────────────────────
  // 7. SAVE TO AIRTABLE
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
          user_unique_id              : userId,
          request_unique_id           : runId,
          time_of_request             : time,
          service_request_tag_name    : serviceTagName,
          service_request_size        : rowCount,
          service_request_credits_cost: creditsCost,
          service_request_url         : driveLink
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

} catch (err) {
  console.log('❌ Error:', err.message);
}

await Actor.exit();
