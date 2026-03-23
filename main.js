import { Actor } from 'apify';

await Actor.init();

// Read the input from your form
const input = await Actor.getInput();

const { firstName, lastName, domain, fileUrl } = input;

console.log(`Hello ${firstName} ${lastName}!`);
console.log(`Domain: ${domain}`);
console.log(`File URL: ${fileUrl}`);

// Save the result to Apify storage
await Actor.pushData({
  firstName,
  lastName,
  domain,
  fileUrl,
  message: `Actor ran successfully for ${firstName} ${lastName}`
});

console.log('Actor finished successfully!');

await Actor.exit();
