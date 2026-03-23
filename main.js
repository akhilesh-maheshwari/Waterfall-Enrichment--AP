import { Actor } from 'apify';

await Actor.init();

// Read the input from your form
const input = await Actor.getInput();
const { firstName, lastName, domain, uploadedFile } = input;

console.log(`Starting actor for ${firstName} ${lastName}`);
console.log(`Domain: ${domain}`);
console.log(`File URL: ${uploadedFile}`);

// Send data to Google Apps Script
// which will save the CSV to Google Drive requested_csv folder
console.log('Sending CSV to Google Drive...');

const response = await fetch('https://script.google.com/macros/s/AKfycbyrkTBophapts2XV4ZA2HxmzUgB26wfhcZmm7qAz7wuRckW5suJSENN6GL_G4zeFx7I/exec', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    firstName: firstName,
    lastName: lastName,
    domain: domain,
    fileUrl: uploadedFile
  })
});

const result = await response.json();
console.log('Google Drive response:', JSON.stringify(result));

if (result.status === 'success') {
  console.log('CSV saved successfully to requested_csv folder!');
  console.log('File link:', result.fileLink);
} else {
  console.log('Error saving CSV:', result.message);
}

// Save the result to Apify storage as well
await Actor.pushData({
  firstName,
  lastName,
  domain,
  uploadedFile,
  driveFileLink: result.fileLink || '',
  status: result.status,
  message: result.message
});

console.log('Actor finished successfully!');

await Actor.exit();
