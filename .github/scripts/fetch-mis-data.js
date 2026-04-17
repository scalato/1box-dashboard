/**
 * 1BOX MIS Data Fetcher
 * 
 * Runs as a GitHub Action to:
 * 1. Get a fresh access token using the refresh token
 * 2. List today's MIS PDF files from OneDrive
 * 3. Download each PDF
 * 4. Send to Claude API for data extraction
 * 5. Save extracted data as JSON (data/latest.json + data/YYYY-MM-DD.json)
 */

const fs = require('fs');
const path = require('path');

const {
  MS_CLIENT_ID,
  MS_CLIENT_SECRET,
  MS_TENANT_ID,
  MS_REFRESH_TOKEN,
  CLAUDE_API_KEY,
  ONEDRIVE_FOLDER
} = process.env;

// Validate env vars
const required = ['MS_CLIENT_ID', 'MS_CLIENT_SECRET', 'MS_TENANT_ID', 'MS_REFRESH_TOKEN', 'CLAUDE_API_KEY'];
for (const key of required) {
  if (!process.env[key]) {
    console.error(`Missing required secret: ${key}`);
    process.exit(1);
  }
}

const TOKEN_URL = `https://login.microsoftonline.com/${MS_TENANT_ID}/oauth2/v2.0/token`;
const GRAPH_BASE = 'https://graph.microsoft.com/v1.0';
const CLAUDE_URL = 'https://api.anthropic.com/v1/messages';

// --- Microsoft Graph Auth ---

async function getAccessToken() {
  console.log('Refreshing Microsoft access token...');
  const body = new URLSearchParams({
    client_id: MS_CLIENT_ID,
    client_secret: MS_CLIENT_SECRET,
    refresh_token: MS_REFRESH_TOKEN,
    grant_type: 'refresh_token',
    scope: 'offline_access Files.Read User.Read'
  });

  const resp = await fetch(TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString()
  });

  const data = await resp.json();
  if (data.error) {
    throw new Error(`Token refresh failed: ${data.error_description || data.error}`);
  }

  // If we got a new refresh token, log it (GitHub Actions can update secrets via API if needed)
  if (data.refresh_token && data.refresh_token !== MS_REFRESH_TOKEN) {
    console.log('NOTE: Microsoft issued a new refresh token. You may need to update the MS_REFRESH_TOKEN secret.');
  }

  console.log('Access token obtained successfully.');
  return data.access_token;
}

// --- OneDrive File Operations ---

async function graphFetch(url, token) {
  const resp = await fetch(url, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Graph API ${resp.status}: ${text}`);
  }
  return resp.json();
}

async function graphFetchBinary(url, token) {
  const resp = await fetch(url, {
    headers: { 'Authorization': `Bearer ${token}` }
  });
  if (!resp.ok) {
    throw new Error(`Graph API binary fetch ${resp.status}`);
  }
  return Buffer.from(await resp.arrayBuffer());
}

async function findFolder(token) {
  const folderName = (ONEDRIVE_FOLDER || '/1Box MIS Reports').replace(/^\//, '');
  console.log(`Looking for OneDrive folder: ${folderName}`);

  const rootData = await graphFetch(`${GRAPH_BASE}/me/drive/root/children`, token);
  const folder = (rootData.value || []).find(f => f.name === folderName);

  if (!folder) {
    throw new Error(`Folder "${folderName}" not found in OneDrive root`);
  }

  console.log(`Found folder: ${folder.name} (${folder.id})`);
  return folder;
}

function getTodayDateString() {
  const now = new Date();
  // Format as YYYY-MM-DD
  return now.toISOString().split('T')[0];
}

function getDateVariants() {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  // Return multiple date format variants that might appear in filenames
  return [
    `${y}-${m}-${d}`,           // 2026-04-17
    `${d}-${m}-${y}`,           // 17-04-2026
    `${d}${m}${y}`,             // 17042026
    `${y}${m}${d}`,             // 20260417
    `${d}-${m.replace(/^0/, '')}-${y}`, // 17-4-2026
  ];
}

async function getTodaysFiles(token, folderId) {
  console.log('Listing files in OneDrive folder...');
  const listData = await graphFetch(
    `${GRAPH_BASE}/me/drive/items/${folderId}/children?$orderby=lastModifiedDateTime desc&$top=100`,
    token
  );

  const allFiles = listData.value || [];
  console.log(`Total files in folder: ${allFiles.length}`);

  const dateVariants = getDateVariants();
  const today = getTodayDateString();

  // Filter for today's PDF files
  const todayPdfs = allFiles.filter(f => {
    if (!f.name.toLowerCase().endsWith('.pdf')) return false;
    // Check if filename contains today's date in any format
    return dateVariants.some(dv => f.name.includes(dv));
  });

  // Also check for files modified today (fallback)
  if (todayPdfs.length === 0) {
    const todayFiles = allFiles.filter(f => {
      if (!f.name.toLowerCase().endsWith('.pdf')) return false;
      const modified = f.lastModifiedDateTime.split('T')[0];
      return modified === today;
    });
    if (todayFiles.length > 0) {
      console.log(`Found ${todayFiles.length} PDFs modified today (by date, not filename)`);
      return todayFiles;
    }
  }

  console.log(`Found ${todayPdfs.length} PDFs matching today's date`);
  return todayPdfs;
}

// --- Claude API ---

const EXTRACTION_PROMPT = `You are a data extraction assistant for 1BOX Self Storage. Extract all data from this MIS (Management Information System) PDF report.

Return ONLY valid JSON with this exact structure (no markdown, no backticks, just raw JSON):
{
  "site": "1BOX SiteName",
  "country": "NL or FR",
  "date": "YYYY-MM-DD",
  "sqm_occupancy_pct": 76.5,
  "occupied_sqm": 1234,
  "total_sqm": 1615,
  "current_rent": 12345.67,
  "rent_per_sqm": 14.50,
  "gpi": 15000.00,
  "max_rent": 18000.00,
  "max_rent_per_sqm": 16.95,
  "mtd_move_ins": 5,
  "mtd_vacates": 3,
  "mtd_net": 2,
  "ytd_move_ins": 45,
  "ytd_vacates": 30,
  "arrears_30_plus": 0,
  "total_units": 250,
  "occupied_units": 200,
  "unit_occupancy_pct": 80.0,
  "insurance_active": 200,
  "insurance_rate_pct": 80.0,
  "insurance_premium": 1500.00,
  "scheme_discounts": 500.00,
  "fixed_discounts": 0,
  "total_discounts": 500.00,
  "discount_pct_of_max": 2.5,
  "revenue_capture_rate_pct": 83.0,
  "potential_rent_gap": 2000.00,
  "accounts_receivable": {
    "outstanding": 0,
    "in_arrears": 0,
    "paid_in_advance": 0,
    "invoiced_not_due": 0
  }
}

Extract as many fields as the PDF contains. Use null for fields not found. Ensure all monetary values are in euros (numbers only, no currency symbols). Percentages should be plain numbers (76.5 not "76.5%").`;

async function extractWithClaude(pdfBuffer, filename) {
  console.log(`Sending ${filename} to Claude for extraction... (${(pdfBuffer.length / 1024).toFixed(0)} KB)`);

  const base64Pdf = pdfBuffer.toString('base64');

  const resp = await fetch(CLAUDE_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': CLAUDE_API_KEY,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 4096,
      messages: [{
        role: 'user',
        content: [
          {
            type: 'document',
            source: {
              type: 'base64',
              media_type: 'application/pdf',
              data: base64Pdf
            }
          },
          {
            type: 'text',
            text: EXTRACTION_PROMPT
          }
        ]
      }]
    })
  });

  if (!resp.ok) {
    const errText = await resp.text();
    throw new Error(`Claude API ${resp.status}: ${errText}`);
  }

  const data = await resp.json();
  const text = data.content
    .filter(b => b.type === 'text')
    .map(b => b.text)
    .join('');

  // Parse the JSON response (strip any markdown fences if present)
  const clean = text.replace(/```json\s*/g, '').replace(/```\s*/g, '').trim();
  const parsed = JSON.parse(clean);
  console.log(`  Extracted: ${parsed.site} â ${parsed.sqm_occupancy_pct}% occ, â¬${parsed.current_rent} rent`);
  return parsed;
}

// --- Main ---

async function main() {
  console.log('=== 1BOX MIS Data Fetch ===');
  console.log(`Date: ${getTodayDateString()}`);

  // Step 1: Get access token
  const token = await getAccessToken();

  // Step 2: Find the OneDrive folder
  const folder = await findFolder(token);

  // Step 3: Get today's files
  const files = await getTodaysFiles(token, folder.id);

  if (files.length === 0) {
    console.log('No MIS PDFs found for today. Exiting.');
    // Don't fail the action â just no data today
    process.exit(0);
  }

  // Step 4: Download and process each PDF (one at a time to respect rate limits)
  const sites = [];
  for (const file of files) {
    try {
      // Download the file
      const downloadUrl = `${GRAPH_BASE}/me/drive/items/${file.id}/content`;
      const pdfBuffer = await graphFetchBinary(downloadUrl, token);

      // Extract with Claude
      const siteData = await extractWithClaude(pdfBuffer, file.name);
      siteData._source_file = file.name;
      siteData._processed_at = new Date().toISOString();
      sites.push(siteData);

      // Rate limit pause between Claude calls
      if (files.indexOf(file) < files.length - 1) {
        console.log('  Pausing 6s for rate limit...');
        await new Promise(r => setTimeout(r, 6000));
      }
    } catch (err) {
      console.error(`  Error processing ${file.name}: ${err.message}`);
      // Continue with other files
    }
  }

  if (sites.length === 0) {
    console.log('No sites were successfully extracted. Exiting.');
    process.exit(1);
  }

  // Step 5: Build the output JSON
  const output = {
    date: getTodayDateString(),
    updated_at: new Date().toISOString(),
    site_count: sites.length,
    files_processed: files.length,
    sites: sites
  };

  // Step 6: Write to data/
  const dataDir = path.join(process.cwd(), 'data');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  const latestPath = path.join(dataDir, 'latest.json');
  const datePath = path.join(dataDir, `${getTodayDateString()}.json`);

  fs.writeFileSync(latestPath, JSON.stringify(output, null, 2));
  fs.writeFileSync(datePath, JSON.stringify(output, null, 2));

  console.log(`\n=== Done ===`);
  console.log(`Processed ${sites.length} sites from ${files.length} PDFs`);
  console.log(`Saved to: data/latest.json and data/${getTodayDateString()}.json`);
  
  // Summary
  sites.forEach(s => {
    console.log(`  ${s.site}: ${s.sqm_occupancy_pct}% occ, â¬${s.current_rent} rent, net ${s.mtd_net > 0 ? '+' : ''}${s.mtd_net}`);
  });
}

main().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
