const TIKA_ENDPOINT = "https://tika-server-1046058817537.ecozeAIRegion.run.app/tika";

const mime = require("mime-types");
const pdfParse = require("pdf-parse");
const otextract = require("office-text-extractor");

// … near top of file, set up one keep-alive agent for all fetches …
const keepAliveAgent = new https.Agent({ keepAlive: true });
const DEEPSEEK_R1 = 'deepseek/deepseek-r1-0528';
const OAI_GPT = 'openai/gpt-oss-120b-maas';
const sleepAI = ms => new Promise(r => setTimeout(r, ms));
const REGION = "ecozeAIRegion";
const TIMEOUT = 3600; // Increased timeout for long polling if needed, though functions usually cap at 540s (9 mins). User might need Gen 2 for up to 60 mins.
const MEM = "2GiB"; // Deep research might return large payloads? Usually standard is fine, but keeping high.

// --- Interactions API Helpers ---
const INTERACTIONS_API_BASE = "https://generativelanguage.googleapis.com/v1beta/interactions";

const SIM_THRESHOLD = 0.80;
const MAX_LOOPS = 20;

const FOLLOWUP_LIMIT = 25;
/* ---------- BoM lines with optional mass ---------- */
const BOM_RE = /.*/;
const searchTools = [{ googleSearch: {} }];
const calcTools = []; // Reserved for future calculator/math tools
const CFSR_EXCLUDE = ["saveURLs-urlUsage", "cf15-RefineCheck", "cf20", "cf48", "saveURLs-urlUsage-batch-1", "saveURLs-urlUsage-batch-2", "saveURLs-urlUsage-batch-3", "saveURLs-urlUsage-batch-4", "saveURLs-urlUsage-batch-5", "saveURLs-urlUsage-batch-6", "saveURLs-urlUsage-batch-7", "apcfSupplierDisclosed-Check", "apcfSupplierDisclosed-initial", "apcfSupplierFinderCheck", "apcfSupplierFinderCheckFinal", "apcfSupplierFinderFactCheck", "apcfSupplierFinderDRFactCheck", "apcfInitial2", "apcfMaterials2", "apcfSupplierDisclosed-initial", "apcfSupplierDisclosed-FC", "cf37-FC", "cf11-EnhanceName", "apcfSupplierAddressFactChecker"];
const VERTEX_REDIRECT_RE = /.*/;

const TOOL_ICON = {
  google_search: "🔍",
  crawlee_crawl: "🕷️",
  python_calculations: "🧮",
  crawlee_map: "🧭",
  tika_extract: "📜",
};

const MEM_SUPPLIER_FINDER_DR = "4GiB";

const TEST_QUEUE_ID = "apcf-ai-testing";
const TEST_QUEUE_INTERVAL_SEC = 30; // 5-minute interval

let testTasksCli, testQueuePath;

const pretty = obj =>
  JSON.stringify(obj, null, 2).slice(0, 50_000);      // log helper

module.exports = {
  TIKA_ENDPOINT,
  keepAliveAgent,
  DEEPSEEK_R1,
  OAI_GPT,
  sleepAI,
  REGION,
  TIMEOUT,
  MEM,
  INTERACTIONS_API_BASE,
  SIM_THRESHOLD,
  MAX_LOOPS,
  FOLLOWUP_LIMIT,
  BOM_RE,
  searchTools,
  calcTools,
  CFSR_EXCLUDE,
  VERTEX_REDIRECT_RE,
  TOOL_ICON,
  MEM_SUPPLIER_FINDER_DR,
  TEST_QUEUE_ID,
  TEST_QUEUE_INTERVAL_SEC,
  pretty
};