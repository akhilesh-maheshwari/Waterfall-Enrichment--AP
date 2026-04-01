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
  const boomerangStatUrl  = input.boomerangStatUrl   || 'https://s1.boomerangserver.co.in/webhook/waterfall-request-stats';

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
        validLines.push(cols.map(c => c.trim()).join(',') + ',');
      } else if (cols.length === 4) {
        validLines.push(cols.map(c => c.trim()).join(','));
      } else {
        validLines.push(line.trim());
      }
    }

    const hasNameCols = validLines[0] && validLines[0].split(',').length >= 3;
    const header = hasNameCols ? 'first_name,last_name,organization_website_url,email' : 'url';

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

  await Actor.charge({ eventName: serviceOption1, count: rowCount });

  // ──────────────────────────────
  // 5. STEP 1 — TRIGGER WORKFLOW 1
  // ──────────────────────────────
  console.log('\n════════════════════════════════════');
  console.log('Step 1 : Setting up master & batches');
  console.log('════════════════════════════════════');

  let wf1Res;
  try {
    wf1Res = await fetch(
      'https://frontend.boomerangserver.co.in/webhook/11fd4929-f376-40f8-9d6f-71f1b3587b3d',
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
  console.log('n8n step 1 status  :', wf1Res.status);
  console.log('n8n step 1 response:', wf1Text);

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
  const batch_id          = wf1Data.batch_id          || '';

  if (!request_unique_id) throw new Error('No request_unique_id returned from Step 1!');

  console.log('\n✅ Step 1 Complete!');
  console.log('   Request ID    :', request_unique_id);
  console.log('   Master File   :', masterFileUrl);
  console.log('   Total Batches :', total_batches);

  // ──────────────────────────────
  // 6. STEP 2 — PROCESS BATCHES
  // ──────────────────────────────
  let completedBatches = 0;
  let round            = 0;
  let allOutputLinks   = [];
  let allBatchResults  = [];

  const getNextBatchJobs = async () => {
    try {
      const wf2Res = await fetch(
        'https://frontend.boomerangserver.co.in/webhook/2d274972-e90d-4f14-bb58-57b7ea40abdf',
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
      const wf2Text = await wf2Res.text();
      console.log('n8n step 2 status  :', wf2Res.status);
      console.log('n8n step 2 response:', wf2Text);
      if (!wf2Text || wf2Text.trim() === '') return null;
      const wf2Data = JSON.parse(wf2Text);
      return wf2Data.batchJobs || null;
    } catch (err) {
      console.log('❌ No response, please try again.');
      return null;
    }
  };

  let batchJobs = await getNextBatchJobs();

  while (!batchJobs || batchJobs.length === 0) {
    console.log('⏳ No slots available (backend full). Waiting 2 mins before retry...');
    await new Promise(r => setTimeout(r, 2 * 60 * 1000));
    batchJobs = await getNextBatchJobs();
  }

  if (batchJobs && batchJobs.length > 0) {

    while (true) {

      round++;
      console.log(`\n════════════════════════════════════`);
      console.log(`Step 2 : Round ${round} — ${batchJobs.length} batch(es)`);
      console.log(`         Completed : ${completedBatches}/${total_batches}`);
      console.log(`         Remaining : ${total_batches - completedBatches}`);
      console.log(`════════════════════════════════════`);

      console.log(`\n  Sending ${batchJobs.length} batches to n8n for status checking...`);

      const batchStatusResults = await Promise.all(
        batchJobs.map(async (job) => {
          const { request_id, driveInputLink, batch_number } = job;
          console.log(`  ⏳ Batch ${batch_number} — Polling status (request_id: ${request_id})...`);

          const maxAttempts  = 10;
          const pollInterval = 180000;

          for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
              const statusRes = await fetch(
                'https://frontend.boomerangserver.co.in/webhook/batch-status-copy',
                {
                  method : 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  signal : AbortSignal.timeout(120000),
                  body   : JSON.stringify({
                    request_id,
                    batch_number,
                    driveInputLink,
                    request_unique_id,
                    batchFolderId,
                    boomerangStatUrl,
                    userId,
                    runId,
                    time,
                    serviceTagName,
                    rowCount   : job.batch_size || rowCount,
                    creditsCost
                  })
                }
              );
              const statusText = await statusRes.text();

              if (statusText.includes('<html>') || statusText.includes('504')) {
                console.log(`  ⚠️ Batch ${batch_number} — 504, retrying (${attempt}/${maxAttempts})...`);
                await new Promise(r => setTimeout(r, pollInterval));
                continue;
              }

              const statusData = JSON.parse(statusText);
              console.log(`  ✅ Batch ${batch_number} status:`, statusData.status);

              if (statusData.status === 'Completed' || statusData.status === 'Failed') {
                return { ...statusData, job };
              }

              console.log(`  🔄 Batch ${batch_number} still processing, attempt ${attempt}/${maxAttempts}. Waiting 3 min...`);
              await new Promise(r => setTimeout(r, pollInterval));

            } catch (err) {
              console.log(`  ⚠️ Batch ${batch_number} poll error (attempt ${attempt}): ${err.message}`);
              await new Promise(r => setTimeout(r, pollInterval));
            }
          }

          console.log(`  ❌ Batch ${batch_number} timed out after ${maxAttempts} attempts.`);
          return { status: 'Failed', job };
        })
      );

      const hasTimeout = batchStatusResults.some(r => r.status === 'GatewayTimeout');
      if (hasTimeout) {
        console.log('\n❌ 504 Gateway Timeout — stopping. Please try again.');
        break;
      }

      const batchResults = [];

      for (const result of batchStatusResults) {
        const { job } = result;
        const { request_id, driveInputLink, batch_number } = job;

        if (result.status !== 'Completed') {
          console.log(`  ⚠️ Batch ${batch_number} did not complete. Skipping output.`);
          batchResults.push({ batch_number, request_id, status: result.status || 'Failed', output_url: '' });
          allOutputLinks.push('');
          continue;
        }

        const boomerangOutputUrl = `https://s1.boomerangserver.co.in/webhook/waterfalls-request-output?request_id=${request_id}`;
        let outputLink = '';

        try {
          const outputRes = await fetch(
            'https://frontend.boomerangserver.co.in/webhook/waterfall-output-copy',
            {
              method : 'POST',
              headers: { 'Content-Type': 'application/json' },
              signal : AbortSignal.timeout(120000),
              body   : JSON.stringify({
                userId,
                runId,
                time,
                serviceTagName,
                rowCount         : job.batch_size || rowCount,
                creditsCost,
                request_id,
                requestStatus    : result.status,
                driveInputLink,
                boomerangOutputUrl,
                batch_number,
                request_unique_id,
                batchFolderId
              })
            }
          );
          const outputText = await outputRes.text();
          console.log(`  Batch ${batch_number} output raw response:`, outputText);
          if (outputRes.ok) {
            try {
              const outputData = JSON.parse(outputText);
              outputLink = outputData['Output Link'] || outputData.outputLink || outputData.driveOutputLink || outputData.webViewLink || '';
            } catch (e) {
              console.log(`  Batch ${batch_number} output parse failed.`);
            }
          } else {
            console.log(`  ❌ No response, please try again.`);
          }
        } catch (fetchErr) {
          console.log(`  ❌ No response, please try again.`);
        }

        batchResults.push({ batch_number, request_id, status: result.status, output_url: outputLink });
        allOutputLinks.push(outputLink);
      }

      console.log(`\n✅ Round ${round} Results:`);
      for (const result of batchResults) {
        console.log(`\n   📦 Batch ${result.batch_number}`);
        console.log(`      Request ID  : ${result.request_id}`);
        console.log(`      Status      : ${result.status}`);
        console.log(`      Output Link : ${result.output_url}`);
      }

      completedBatches += batchResults.length;
      allBatchResults = allBatchResults.concat(batchResults);

      if (completedBatches >= total_batches) {
        const anyFailed = batchResults.some(r => r.status !== 'Completed' || !r.output_url);
        if (anyFailed) {
          console.log('\n⚠️ Some batches did not complete successfully.');
        } else {
          await getNextBatchJobs();
        }
        break;
      }

      console.log(`\n⏳ ${total_batches - completedBatches} batch(es) remaining. Getting next round...`);
      batchJobs = await getNextBatchJobs();

      while (!batchJobs || batchJobs.length === 0) {
        if (completedBatches >= total_batches) {
          console.log('✅ All batches completed.');
          break;
        }
        console.log('⏳ No slots available (backend full). Waiting 2 mins before retry...');
        await new Promise(r => setTimeout(r, 2 * 60 * 1000));
        batchJobs = await getNextBatchJobs();
      }

      if (completedBatches >= total_batches) break;
    }
  }

  // ──────────────────────────────
  // 7. FINAL SUMMARY
  // ──────────────────────────────
  console.log('\n════════════════════════════════════');
  console.log('🎉 ALL BATCHES COMPLETED!');
  console.log('════════════════════════════════════');
  console.log('Run ID        :', runId);
  console.log('Total Batches :', total_batches);
  console.log('\nOutput Links:');
  allOutputLinks.forEach((link, i) => console.log(`  Batch ${i + 1} : ${link || 'Failed'}`));
  console.log('════════════════════════════════════');

  // one row per batch, Output Link as direct URL
  for (const b of allBatchResults) {
    await Actor.pushData({
      run_id        : runId,
      service_name  : serviceName,
      service_tag   : serviceTagName,
      batch_number  : b.batch_number,
      request_id    : b.request_id,
      status        : b.status,
      'Output Link' : b.output_url || 'Failed'
    });
  }

} catch (err) {
  console.log('❌ Error:', err.message);
}

await Actor.exit();
