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
  const serviceName    = input.serviceName           || 'Waterfall Enrichment';
  const serviceOption1 = input.serviceOption1        || 'pro';
  const requestSource  = input.requestSource         || 'Waterfall_enrichment_AP';
  const boomerangInputUrl = input.boomerangInputUrl  || 'https://s1.boomerangserver.co.in/webhook/waterfall-live';

  console.log('Tag Name :', serviceTagName);
  console.log('Service  :', serviceName);
  console.log('Entries  :', entries ? 'Yes' : 'No');
  console.log('File URL :', uploadedFile ? 'Yes' : 'No');

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
        validLines.push(line.trim());
      }
    }

    const hasThreeCols = validLines[0] && validLines[0].split(',').length === 3;
    const header = hasThreeCols ? 'first_name,last_name,domain' : 'url';

    csvContent = header + '\n' + validLines.join('\n');
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

  console.log('User ID :', userId);
  console.log('Run ID  :', runId);
  console.log('Time    :', time);

  // ──────────────────────────────
  // 4. CALCULATE COST
  // ──────────────────────────────
  const creditsCost = parseFloat((rowCount * 0.015).toFixed(3));
  console.log('Row count    :', rowCount);
  console.log('Credits cost : $', creditsCost);

  // ──────────────────────────────
  // 5. STEP 1 — TRIGGER WORKFLOW 1
  //    Setup folders, batches, NocoDB
  // ──────────────────────────────
  console.log('\n════════════════════════════════════');
  console.log('Step 1 : Setting up master & batches');
  console.log('════════════════════════════════════');

  let wf1Res;
  try {
    wf1Res = await fetch(
      'https://n8n-internal.chitlangia.co/webhook/11fd4929-f376-40f8-9d6f-71f1b3587b3d',
      {
        method : 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal : AbortSignal.timeout(60000),
        body   : JSON.stringify({
          userId,
          runId,
          time,
          serviceTagName,
          rowCount,
          creditsCost,
          csvContent,
          uploadedFile,
          fileName,
          boomerangInputUrl,
          service_option_1 : serviceOption1,
          service_name     : serviceName,
          request_source   : requestSource
        })
      }
    );
  } catch (fetchErr) {
    throw new Error(`Step 1 failed: ${fetchErr.message}`);
  }

  const wf1Text = await wf1Res.text();
  if (!wf1Res.ok) throw new Error(`Step 1 error ${wf1Res.status}: ${wf1Text.slice(0, 200)}`);

  let wf1Data;
  try {
    wf1Data = JSON.parse(wf1Text);
  } catch (e) {
    throw new Error(`Step 1 JSON parse failed: ${wf1Text.slice(0, 200)}`);
  }

  const request_unique_id = wf1Data.request_unique_id || '';
  const masterFileUrl     = wf1Data.masterFileUrl     || '';
  const total_batches     = parseInt(wf1Data.total_batches || '0');
  const batchFolderId     = wf1Data.batchFolderId     || '';

  if (!request_unique_id) throw new Error('No request_unique_id returned from Step 1!');

  console.log('✅ Setup complete!');
  console.log('   Request ID    :', request_unique_id);
  console.log('   Master File   :', masterFileUrl);
  console.log('   Total Batches :', total_batches);

  // ──────────────────────────────
  // 6. STEP 2 — PROCESS BATCHES
  //    5 batches at a time
  // ──────────────────────────────
  let completedBatches = 0;
  let round            = 0;
  let allOutputLinks   = [];

  while (true) {

    round++;
    const remaining = total_batches - completedBatches;
    const thisRound = Math.min(5, remaining);

    console.log(`\n════════════════════════════════════`);
    console.log(`Step 2 : Round ${round} — ${thisRound} batch(es)`);
    console.log(`         Completed : ${completedBatches}/${total_batches}`);
    console.log(`         Remaining : ${remaining}`);
    console.log(`════════════════════════════════════`);

    // ── 2a. Get pending batches & send to Boomerang ──
    let wf2Res;
    try {
      wf2Res = await fetch(
        'https://n8n-internal.chitlangia.co/webhook/2d274972-e90d-4f14-bb58-57b7ea40abdf',
        {
          method : 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal : AbortSignal.timeout(60000),
          body   : JSON.stringify({
            request_unique_id,
            batchFolderId,
            userId,
            runId,
            time,
            serviceTagName,
            rowCount,
            creditsCost,
            boomerangInputUrl,
            service_option_1 : serviceOption1,
            service_name     : serviceName,
            request_source   : requestSource
          })
        }
      );
    } catch (fetchErr) {
      throw new Error(`Step 2 Round ${round} failed: ${fetchErr.message}`);
    }

    const wf2Text = await wf2Res.text();
    console.log('n8n step 2 status:', wf2Res.status);
    console.log('n8n step 2 raw response:', wf2Text);

    if (!wf2Res.ok) throw new Error(`Step 2 error ${wf2Res.status}: ${wf2Text.slice(0, 200)}`);

    // Handle empty response
    if (!wf2Text || wf2Text.trim() === '') {
      console.log('✅ No more pending batches. All done!');
      break;
    }

    let wf2Data;
    try {
      wf2Data = JSON.parse(wf2Text);
    } catch (e) {
      console.log('Step 2 response not JSON, exiting loop.');
      break;
    }

    console.log('n8n step 2 response:', JSON.stringify(wf2Data));

    const batchJobs = wf2Data.batchJobs || [];

    if (batchJobs.length === 0) {
      console.log('✅ No more pending batches. All done!');
      break;
    }

    // ── 2b. Poll Boomerang stats for each batch ──
    console.log(`\nPolling Boomerang status every 30 seconds...`);

    const POLL_INTERVAL_MS = 2 * 60 * 1000;
    const batchResults = [];

    for (const job of batchJobs) {

      const { request_id, driveInputLink, batch_number, nocodb_id } = job;

      console.log(`\n  ⏳ Batch ${batch_number} — Polling (request_id: ${request_id})...`);

      let requestStatus  = '';
      let emailFound     = 0;
      let emailNotFound  = 0;
      let pollAttempts   = 0;

      while (true) {

        pollAttempts++;

        let statsRes;
        try {
          statsRes = await fetch(
            `https://s1.boomerangserver.co.in/webhook/waterfall-request-stats?request_id=${request_id}`,
            { method: 'GET', signal: AbortSignal.timeout(15000) }
          );
        } catch (fetchErr) {
          console.log(`  Batch ${batch_number} poll ${pollAttempts} failed: ${fetchErr.message}, retrying...`);
          await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
          continue;
        }

        const statsText = await statsRes.text();
        let statsData;
        try {
          statsData = JSON.parse(statsText);
        } catch (e) {
          console.log(`  Batch ${batch_number} stats parse failed, retrying...`);
          await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
          continue;
        }

        requestStatus = statsData.request_status || statsData.requestStatus || statsData.status || '';
        emailFound    = statsData.total_email_found    || 0;
        emailNotFound = statsData.total_email_not_found || 0;

        console.log(`  Batch ${batch_number} | Status: ${requestStatus || 'Pending'} | Found: ${emailFound} | Not Found: ${emailNotFound}`);

        if (requestStatus === 'Completed') {
          console.log(`  ✅ Batch ${batch_number} complete!`);
          break;
        }

        await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
      }

      // ── 2c. Call waterfall-output webhook ──
      const boomerangOutputUrl = `https://s1.boomerangserver.co.in/webhook/waterfalls-request-output?request_id=${request_id}`;

      let outputLink = '';
      try {
        const outputRes = await fetch(
          'https://n8n-internal.chitlangia.co/webhook/waterfall-output-copy',
          {
            method : 'POST',
            headers: { 'Content-Type': 'application/json' },
            signal : AbortSignal.timeout(60000),
            body   : JSON.stringify({
              userId,
              runId,
              time,
              serviceTagName,
              rowCount      : job.batch_size || rowCount,
              creditsCost,
              request_id,
              requestStatus,
              driveInputLink,
              boomerangOutputUrl,
              nocodb_id,
              batch_number,
              request_unique_id,
              batchFolderId
            })
          }
        );

        const outputText = await outputRes.text();
        if (outputRes.ok) {
          try {
            const outputData = JSON.parse(outputText);
            outputLink = outputData['Output Link'] || outputData.outputLink || outputData.driveOutputLink || outputData.webViewLink || '';
          } catch (e) {
            console.log(`  Batch ${batch_number} output parse failed, continuing...`);
          }
        } else {
          console.log(`  Batch ${batch_number} output webhook returned ${outputRes.status}`);
        }
      } catch (fetchErr) {
        console.log(`  Batch ${batch_number} output webhook failed: ${fetchErr.message}`);
      }

      batchResults.push({
        batch_number,
        request_id,
        status        : requestStatus,
        email_found   : emailFound,
        email_not_found: emailNotFound,
        output_url    : outputLink
      });

      allOutputLinks.push(outputLink);
    }

    // ── 2d. Log round results ──
    console.log(`\n✅ Round ${round} Results:`);
    for (const result of batchResults) {
      console.log(`\n   📦 Batch ${result.batch_number}`);
      console.log(`      Request ID     : ${result.request_id}`);
      console.log(`      Status         : ${result.status}`);
      console.log(`      Emails Found   : ${result.email_found}`);
      console.log(`      Emails Missing : ${result.email_not_found}`);
      console.log(`      Output Link    : ${result.output_url}`);
    }

    completedBatches += batchResults.length;

    // Save round to Apify dataset
    await Actor.pushData({
      round,
      request_unique_id,
      completedBatches,
      total_batches,
      batchResults
    });

    if (completedBatches < total_batches) {
      console.log(`\n⏳ ${total_batches - completedBatches} batch(es) remaining. Starting next round...`);
    }
  }

  // ──────────────────────────────
  // 7. FINAL SUMMARY
  // ──────────────────────────────
  console.log('\n════════════════════════════════════');
  console.log('🎉 ALL BATCHES COMPLETED!');
  console.log('════════════════════════════════════');
  console.log('Request ID    :', request_unique_id);
  console.log('Total Batches :', total_batches);
  console.log('\nOutput Links:');
  allOutputLinks.forEach((link, i) => console.log(`  Batch ${i + 1} : ${link}`));
  console.log('════════════════════════════════════');

  await Actor.pushData({
    status           : 'completed',
    request_unique_id,
    total_batches,
    masterFileUrl,
    allOutputLinks
  });

} catch (err) {
  console.log('❌ Error:', err.message);
}

await Actor.exit();
