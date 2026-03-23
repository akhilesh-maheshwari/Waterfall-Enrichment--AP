import { Actor } from 'apify';

await Actor.init();

try {
  // Step 1 - Get input from the form
  const input = await Actor.getInput();

  const firstName = input.firstName || '';
  const lastName = input.lastName || '';
  const domain = input.domain || '';
  let csvUrl = input.uploadedFile || input.fileUrl || input.csvUrl || '';

  console.log('First Name:', firstName);
  console.log('Last Name:', lastName);
  console.log('Domain:', domain);
  console.log('Original URL:', csvUrl);

  // Step 2 - Auto convert Google Sheets URL to CSV export URL
  if (csvUrl.includes('docs.google.com/spreadsheets')) {
    const match = csvUrl.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
    if (match) {
      const sheetId = match[1];
      csvUrl = `https://docs.google.com/spreadsheets/d/${sheetId}/export?format=csv&gid=0`;
      console.log('Converted URL:', csvUrl);
    }
  }

  // Step 3 - Send to Google Apps Script to download and save CSV
  console.log('Sending to Google Apps Script...');

  const gasResponse = await fetch(
    'https://script.google.com/macros/s/AKfycbyrkTBophapts2XV4ZA2HxmzUgB26wfhcZmm7qAz7wuRckW5suJSENN6GL_G4zeFx7I/exec',
    {
      method: 'POST',
      redirect: 'follow',
      headers: { 'Content-Type': 'text/plain' },
      body: JSON.stringify({
        firstName: firstName,
        lastName: lastName,
        domain: domain,
        csvUrl: csvUrl
      })
    }
  );

  const gasText = await gasResponse.text();
  const gasResult = JSON.parse(gasText);

  console.log('Google Drive result:', JSON.stringify(gasResult));

  if (gasResult.status !== 'success') {
    throw new Error('Google Drive error: ' + gasResult.message);
  }

  console.log('CSV saved to Drive:', gasResult.fileLink);

  // Step 4 - Count rows from CSV
  console.log('Counting CSV rows...');
  const csvResponse = await fetch(csvUrl);
  const csvText = await csvResponse.text();
  const allRows = csvText.trim().split('\n');
  const rowCount = allRows.length - 1; // minus header row
  console.log('Total rows (minus header):', rowCount);

  // Step 5 - Calculate cost ($0.01 per row)
  const creditsCost = rowCount * 0.01;
  console.log('Credits cost:', creditsCost);

  // Step 6 - Get Apify run details
  const runId = Actor.getEnv().actorRunId || 'unknown';
  const userId = Actor.getEnv().userId || 'unknown';
  const actorId = Actor.getEnv().actorId || 'Waterfall Enrichment';
  const timeOfRequest = new Date().toISOString();

  console.log('Run ID:', runId);
  console.log('User ID:', userId);

  // Step 7 - Save to Airtable
  console.log('Saving to Airtable...');

  const airtableResponse = await fetch(
    'https://api.airtable.com/v0/appCuadMXrDqpfaDV/tblD3UXc3tYW0mOdT',
    {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer pat4bRijwFM7m1t9u.c5fa218d14d840e4180f628656b63c163ce71bd8d01881d971ee96fe2d939dd8',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        fields: {
          user_unique_id: userId,
          request_unique_id: runId,
          time_of_request: timeOfRequest,
          service_request_tag_name: 'Waterfall Enrichment',
          service_request_size: rowCount,
          service_request_credits_cost: creditsCost,
          service_request_url: gasResult.fileLink
        }
      })
    }
  );

  const airtableResult = await airtableResponse.json();
  console.log('Airtable result:', JSON.stringify(airtableResult));

  if (airtableResult.id) {
    console.log('Saved to Airtable successfully! Record ID:', airtableResult.id);
  } else {
    console.log('Airtable error:', JSON.stringify(airtableResult));
  }

} catch (error) {
  console.log('Actor error:', error.message);
}

await Actor.exit();
