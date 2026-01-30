async function extractWithTika(url) {
  try {
    const response = await fetch(`${TIKA_ENDPOINT}`, {
      method: 'PUT',
      headers: { 'Accept': 'text/plain' },
      body: await (await fetch(url)).buffer() // Fetch URL content first, then send to Tika
    });

    if (!response.ok) {
      throw new Error(`Tika failed with status ${response.status}`);
    }
    return await response.text();
  } catch (err) {
    logger.warn(`[extractWithTika] Failed to extract ${url}:`, err.message);
    return "";
  }
}

const URL_FINDER_DECLARATION = {
  name: "urlFinder",
  description: "Tier 2 Agent: Finds a starting URL for a given task using Google Search.",
  parameters: {
    type: "object",
    properties: {
      task: { type: "string", description: "The search task (e.g. 'Find EPD for Cisco C8500')." }
    },
    required: ["task"]
  }
};

const BROWSER_USE_DECLARATION = {
  name: "browserUse",
  description: "Tier 2 Agent: Navigates a URL, or performs its own search, to find specific resource links (PDFs, etc.).",
  parameters: {
    type: "object",
    properties: {
      task: { type: "string", description: "Task (e.g. Find the product carbon footprint of product x)." },
      url: { type: "string", description: "The URL to navigate." }
    },
    required: ["task", "url"]
  }
};

const URL_ANALYSE_DECLARATION = {
  name: "urlAnalyse",
  description: "Tier 2 Agent: Extracts specific information from a URL.",
  parameters: {
    type: "object",
    properties: {
      task: { type: "string", description: "Extraction task (e.g. 'Extract A1-A3 carbon footprint')." },
      url: { type: "string", description: "The URL to analyze." }
    },
    required: ["task", "url"]
  }
};

async function executeUrlFinder({ task }) {
  logger.info(`[UrlFinder] Task: ${task}`);
  const collectedUrls = new Set();

  await runGeminiStream({
    model: 'aiModel',
    user: `Task: ${task}\n\nFind a single best URL preferably to start the research. If you arent sure which is the best URL, return a list of potential URLs where the research task can begin from.`,
    generationConfig: {
//
//
    },
    collectedUrls // Pass the set to collect grounded URLs
  });

  // Extract and clean URLs from metadata
  const unwrappedUrls = [];
  for (const url of collectedUrls) {
    const unwrapped = await unwrapVertexRedirect(url.trim());
    unwrappedUrls.push(unwrapped);
  }

  const cleanUrls = Array.from(new Set(unwrappedUrls));

  if (cleanUrls.length > 0) {
    logger.info(`[UrlFinder] Found ${cleanUrls.length} grounded URLs.`);
    return JSON.stringify(cleanUrls);
  }

  // Fallback: return empty list or message if no URLs found
  return "[]";
}

async function executeBrowserUse({ task, url }) {
  logger.info(`[BrowserUse] Task: ${task}, URL: ${url}`);
  // Use the existing browser-use service wrapper
  return await executeBrowserUseBrowse({ task, urls: [url] });
}

async function executeUrlAnalyse({ task, url }) {
  logger.info(`[UrlAnalyse] Task: ${task}, URL: ${url}`);
  const { answer } = await runGeminiStream({
    model: 'aiModel',
    user: `Analyze this URL: ${url}\n\nTask: ${task}\n\nExtract the requested information.`,
    generationConfig: {
//
//
    }
  });
  return answer;
}

// -----------------------------------------------------------------------------
// 3-TIER ORCHESTRATOR
// -----------------------------------------------------------------------------

/****************************************************************************************
 * 6.  Other $$$
 ****************************************************************************************/


/****************************************************************************************
 * 7.  Other Helper Functions $$$
 ****************************************************************************************/


async function dispatchFunctionCall(call) {
  const name = call.name;
  // The new SDK provides args directly as an object in `call.args`.
  // If for some reason it's missing, we fallback to an empty object.
  const args = call.args || {};

  /* choose emoji; default to 🛠️ if unknown */
  const icon = TOOL_ICON[name] || "🛠️";
  console.log(`*${icon} dispatchFunctionCall ↳ Executing ${name} with`, JSON.stringify(args));

  try {
    // 1. Google Search (Custom Search Engine)
    if (name === "google_search") {
      const result = await googlePseSearch({
        query: args.query,
        num_results: args.num_results || 5,
        include_answer: false
      });
      console.log(`*${icon} TOOL OUTPUT [${name}]: Found ${result.results.length} results.`);
      return result; // Return JSON object directly
    }

    // 2. Crawlee Map (Puppeteer - Sitemapping)
    if (name === "crawlee_map") {
      const result = await crawleeMap({
        url: args.url,
        max_depth: args.max_depth || 1,
        max_breadth: args.max_breadth || 20,
        limit: args.limit || 50,
        select_paths: args.select_paths || [],
        select_domains: args.select_domains || [],
        exclude_paths: args.exclude_paths || [],
        exclude_domains: args.exclude_domains || [],
        allow_external: args.allow_external || false
      });
      console.log(`*${icon} TOOL OUTPUT [${name}]: Mapped ${result.results.length} URLs.`);
      return result;
    }

    // 3. Crawlee Crawl (Puppeteer - Deep Content Extraction)
    if (name === "crawlee_crawl") {
      const result = await crawleeCrawl({
        url: args.url,
        max_depth: args.max_depth || 1,
        max_breadth: args.max_breadth || 10,
        limit: args.limit || 10,
        allow_external: args.allow_external || false
      });
      console.log(`*${icon} TOOL OUTPUT [${name}]: Scraped ${result.results.length} pages.`);
      return result;
    }

    // 4. Tika Extract (Document Parsing)
    if (name === "tika_extract") {
      const result = await runTikaExtract({
        url: args.url,
        query: args.query
      });
      console.log(`*${icon} TOOL OUTPUT [${name}]: Extracted answer based on document.`);
      return { result: result };
    }

    // 5. Python Calculations (Math.js)
    if (name === "python_calculations") {
      const { code } = args;
      // Basic guard-rail: only allow numbers, ops, and safe functions
      if (! /.*/^._,=eEpiPIsqrtlogsinco*tanabsA-Za-z]+$/.test(code)) {
        return { error: "Expression contains unsupported characters" };
      }
      const result = math.evaluate(code);
      console.log(`*${icon} TOOL OUTPUT [${name}]:`, result);
      return { result: String(result) };
    }

  } catch (err) {
    console.error(`*${icon} ${name} failed:`, err);
    return { error: err.message || String(err) };
  }

  return { error: `Unknown function name: ${name}` };
}


async function crawleeMap({
  url,
  max_depth = 1,
  max_breadth = 20,
  limit = 50,
  select_paths = [],
  select_domains = [],
  exclude_paths = [],
  exclude_domains = [],
  allow_external = false,
}) {
  const t0 = Date.now();
  const found = new Set();

  const config = new Configuration({
    persistStorage: false,
    availableMemoryRatio: 0.8,
    storageClientOptions: {
      localDataDirectory: `/tmp/crawlee_map_${Date.now()}_${Math.random()}`
    }
  });

  const crawler = new CheerioCrawler({
    maxRequestsPerCrawl: limit,
    maxConcurrency: 5,
    requestHandlerTimeoutSecs: 30,
    async requestHandler({ request, $, enqueueLinks, log }) {
      log.info(`Processing ${request.url}...`);

      // Add URL to found set
      found.add(request.url);

      // Build glob patterns for filtering
      const matchGlobs = select_paths.length > 0
        ? select_paths.concat(select_domains.map(d => `*://${d}/*`))
        : ['**'];

      // Enqueue links with depth tracking
      await enqueueLinks({
        strategy: allow_external ? 'all' : 'same-domain',
        limit: max_breadth,
        globs: matchGlobs,
        selector: 'a[href]',
        transformRequestFunction: (req) => {
          // Manual depth control
          const currentDepth = request.userData?.depth ?? 0;

          // Skip if we've reached max depth
          if (currentDepth >= max_depth) {
            return false;
          }

          // Track depth for new requests
          req.userData = {
            ...req.userData,
            depth: currentDepth + 1
          };
          return req;
        },
      });
    },
  }, config);


  try {
    // Initialize with depth tracking
    await crawler.run([{
      url: url,
      userData: { depth: 0 }
    }]);
  } catch (err) {
    console.error("[crawleeMap] Crawl failed:", err);
  }

  return {
    base_url: url,
    results: Array.from(found),
    response_time: ((Date.now() - t0) / 1000).toFixed(2)
  };
}

const CRAWLEE_MAP_SCHEMA = {
  type: "function",
  name: "crawlee_map",
  description: "Obtain a sitemap starting from a base URL",
  strict: true,
  parameters: {
    type: "object",
    properties: {
      url: { type: "string", description: "Root URL to begin the mapping" },
      max_depth: { type: "number" },
      max_breadth: { type: "number" },
      limit: { type: "number" },
      instructions: { type: "string" },
      select_paths: { type: "array", items: { type: "string" } },
      select_domains: { type: "array", items: { type: "string" } },
      exclude_paths: { type: "array", items: { type: "string" } },
      exclude_domains: { type: "array", items: { type: "string" } },
      allow_external: { type: "boolean" },
      categories: { type: "array", items: { type: "string" } }
    },
    required: [
      "url", "max_depth", "max_breadth", "limit", "instructions", "select_paths",
      "select_domains", "exclude_paths", "exclude_domains", "allow_external",
      "categories"
    ],
    additionalProperties: false
  }
};

async function crawleeCrawl({
  url,
  max_depth = 1,
  max_breadth = 10, // Lower breadth for full crawling to save memory
  limit = 20,       // Lower global limit because processing text is heavy
  allow_external = false,
}) {
  const t0 = Date.now();
  const found = [];

  // 1. Configure Crawlee for Cloud Functions (Ephemeral /tmp)
  const config = new Configuration({
    persistStorage: false,
    availableMemoryRatio: 0.9, // Aggressive memory usage
    storageClientOptions: {
      localDataDirectory: `/tmp/crawlee_crawl_${Date.now()}_${Math.random()}`
    }
  });

  const crawler = new CheerioCrawler({
    maxRequestsPerCrawl: limit,
    maxConcurrency: 5,
    requestHandlerTimeoutSecs: 30,
    maxCrawlDepth: max_depth,

    async requestHandler({ request, $, enqueueLinks, log }) {
      log.info(`[crawleeCrawl] Processing ${request.url} (depth=${request.crawlDepth})...`);

      try {
        // Extract page title using Cheerio
        const title = $('title').text() || '';

        // Extract text content from body
        const text = $('body').text()
          .replace( /.*/, ' ')
          .trim()
          .slice(0, 50000);

        found.push({
          url: request.url,
          title: title,
          content: text,
        });
      } catch (e) {
        log.warning(`Page processing issue on ${request.url}: ${e.message}`);
        found.push({ url: request.url, error: e.message });
      }

      // Enqueue links for crawling
      await enqueueLinks({
        strategy: allow_external ? 'all' : 'same-domain',
        limit: max_breadth,
        globs: ['**'],
      });
    }
  }, config);


  try {
    await crawler.run([url]);
  } catch (err) {
    console.error("[crawleeCrawl] Run failed:", err);
  }

  return {
    base_url: url,
    results: found,
    response_time: ((Date.now() - t0) / 1000).toFixed(2)
  };
}

const CRAWLEE_CRAWL_SCHEMA = {
  type: "function",
  name: "crawlee_crawl",
  description: "Deeply crawl a website using a headless browser to extract text content. Use this when you need to READ the content of pages, not just find links. This tool renders JavaScript.",
  strict: true,
  parameters: {
    type: "object",
    properties: {
      url: {
        type: "string",
        description: "The root URL to begin scraping content from."
      },
      max_depth: {
        type: "number",
        description: "How deep to crawl. 0 = root page only. 1 = root + direct links. Recommended: 1."
      },
      max_breadth: {
        type: "number",
        description: "Maximum number of links to follow from any single page. Recommended: 5-10."
      },
      limit: {
        type: "number",
        description: "Global hard limit on the number of pages to scrape. Recommended: 10-20."
      },
      allow_external: {
        type: "boolean",
        description: "If true, the crawler will follow links to different domains. Default: false."
      }
    },
    required: ["url", "max_depth", "limit"],
    additionalProperties: false
  }
};

async function productDescription({ productId = null, materialId = null }) {
  logger.info(`[productDescription] Starting productDescription for productId: ${productId}, materialId: ${materialId}`);

  let pmDocRef, pmDocData;
  if (productId) {
    pmDocRef = db.collection("products_new").doc(productId);
  } else if (materialId) {
    pmDocRef = db.collection("materials").doc(materialId);
  } else {
    logger.error("[productDescription] No productId or materialId provided.");
    return;
  }

  const snap = await pmDocRef.get();
  if (!snap.exists) {
    logger.error(`[productDescription] Document not found: ${pmDocRef.path}`);
    return;
  }
  pmDocData = snap.data();
  const pmName = pmDocData.name;
  if (!pmName) {
    logger.error(`[productDescription] Document has no name field: ${pmDocRef.path}`);
    return;
  }

  const model = "aiModel";
  const cfName = "productDescription";

  // --- Step 1: First AI Call (Research) ---
  const sys1 = `Your job is to take in a product name and research the product. You will be constructing a detailed description of what the product is and does, including what it is made from.

!! You MUST use your google search and url context tools to ground your answer on the most up to date information. !!

Output your answer in the exact following format and no other text:
"
Description: 
[the description of the product]
"`;

  const config1 = {
//
//
//
//
//
      includeThoughts: true,
      thinkingLevel: "HIGH"
    }
  };

  const urls1 = new Set();
  const res1 = await runGeminiStream({
    model,
    generationConfig: config1,
    user: pmName,
    collectedUrls: urls1
  });

  const descriptionOriginalRaw = res1.answer.trim();
  // Extract description using regex to handle the specific format requested
  const descMatch1 = descriptionOriginalRaw.match( /.*/);
  const descriptionOriginal = descMatch1 ? descMatch1[1].trim() : descriptionOriginalRaw;

  // Log AI 1 Transaction & Reasoning
  await logAITransaction({
    cfName,
    productId,
    materialId,
    cost: res1.cost,
    totalTokens: res1.totalTokens,
    modelUsed: res1.model
  });
  await logAIReasoning({
    sys: sys1,
    user: pmName,
    thoughts: res1.thoughts,
    answer: res1.answer,
    cloudfunction: cfName,
    productId,
    materialId,
    rawConversation: res1.rawConversation
  });

  // Save URLs for AI 1
  if (urls1.size > 0) {
    await saveURLs({
      urls: Array.from(urls1),
      productId,
      materialId,
      cloudfunction: cfName
    });
  }

  // --- Step 2: Second AI Call (Fact Check) ---
  const sys2 = `Your job is to take in a product name and a description of the product. You must fact check the description and create a new description where necessary.

!! You MUST use your google search and url context tools to ground your answer on the most up to date information. !!

Output your answer in the exact following format and no other text:
"
*pass_or_fail: [Set as "Pass" and no other text if the description doesnt need changing, set as "Fail" if the description needs changing.]
*description: [Set to "..." if the description passed. If the description didnt pass, output the complete new description here.]
"`;

  const prompt2 = "...";

  const config2 = {
//
//
//
//
//
      includeThoughts: true,
      thinkingLevel: "HIGH"
    }
  };

  const urls2 = new Set();
  const res2 = await runGeminiStream({
    model,
    generationConfig: config2,
    user: prompt2,
    collectedUrls: urls2
  });

  // Log AI 2 Transaction & Reasoning
  await logAITransaction({
    cfName,
    productId,
    materialId,
    cost: res2.cost,
    totalTokens: res2.totalTokens,
    modelUsed: res2.model
  });
  await logAIReasoning({
    sys: sys2,
    user: prompt2,
    thoughts: res2.thoughts,
    answer: res2.answer,
    cloudfunction: cfName,
    productId,
    materialId,
    rawConversation: res2.rawConversation
  });

  // Save URLs for AI 2
  if (urls2.size > 0) {
    await saveURLs({
      urls: Array.from(urls2),
      productId,
      materialId,
      cloudfunction: cfName
    });
  }

  // --- Step 3: Process Results and Update Firestore ---
  const ai2Answer = res2.answer.trim();
  const passMatch = ai2Answer.match( /.*/);
  const passOrFail = passMatch ? passMatch[1].trim() : "Fail";

  const descMatch2 = ai2Answer.match( /.*/);
  const descriptionNew = descMatch2 ? descMatch2[1].trim() : "...";

  let descriptionToAppend = "";
  if (passOrFail === "Pass") {
    descriptionToAppend = descriptionOriginal;
  } else {
    descriptionToAppend = descriptionNew;
  }

  // Update existing description field
  const currentDescription = pmDocData.description || "";
  let finalDescription = "";
  if (!currentDescription || currentDescription.trim() === "") {
    finalDescription = descriptionToAppend;
  } else {
    finalDescription = `${currentDescription}\n\n-----\n\n${descriptionToAppend}`;
  }

  await pmDocRef.update({ description: finalDescription });
  logger.info(`[productDescription] Successfully updated description for ${pmDocRef.path}. PassOrFail: ${passOrFail}`);
}