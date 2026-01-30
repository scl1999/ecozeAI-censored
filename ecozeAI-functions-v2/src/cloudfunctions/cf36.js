const { admin, db, logger, onRequest, onMessagePublished, onSchedule, onDocumentCreated, fetch, discoveryEngineClient, tasksClient, pubSubClient } = require('../../config/firebase');
const { REGION, TIMEOUT, MEM, SECRETS, VERTEX_REDIRECT_RE, INTERACTIONS_API_BASE } = require('../../config/constants');
const { getGeminiClient, runGeminiStream, runOpenModelStream, logFullConversation } = require('../../services/ai/gemini');
const { logAITransaction, logAIReasoning, logAITransactionAgent, logAIReasoningWorkItem, logAIReasoningSingle, logAIReasoningBatch, logAIReasoningFinal, logAIReasoningSimple, logAITransactionSimple } = require('../../services/ai/costs');
const { isValidUrl, unwrapVertexRedirect, saveURLs, extractUrlsFromInteraction, harvestUrls, harvestUrlsFromText, generateReasoningString } = require('../../services/ai/urls');
const { createInteraction, getInteraction, parseNDJSON } = require('../../services/ai/interactionsapi');
const { runAIChat, runAIDeepResearch, runPromisesInParallelWithRetry, productDescription, callCF } = require('../../services/ai/aimain');
const { sleep, getFormattedDate } = require('../../utils/time');
const { runWithRetry, runWithRetryI } = require('../../utils/network');
const { parseCFValue, parseNDJSON: parseNDJSONUtil, parseBoM, parseBoMTable, parseBoMTableLegacy, getStepLabel, getFormattedDate: getFormattedDateUtil } = require('../../utils/formatting');
const prompts = require('../../services/ai/prompts');
const { 
  DUPLICATE_SYS, BOM_SYS, BOM_SYS_TIER_N, GO_AGAIN_PROMPT, TAG_GENERATION_SYS, 
  SYS_APCFSF, SYS_MSG_APCFSF, VERIFY_SYS_MSG, SYS_MSG_MPCFFULL_PRO, 
  SYS_MSG_MPCFFULL_CORE, REASONING_SUMMARIZER_SYS_2, MPCFFULL_PRODUCTS_SYS, 
  MPCFFULLNEW_TAG_GENERATION_SYS 
} = prompts;

// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------

// --- Interactions API Helpers (Updated for Streaming) ---



//-----------------------------------------------------------------------------------------------------------------------------------------------------------------

exports.cf36 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf36] Invoked");
  try {
    // 1. Define the start date: 00:00AM UTC 17th November 2025
    // Note: Month is 0-indexed in JS Date (0=Jan, 10=Nov)
    const startDate = new Date(Date.UTC(2025, 10, 17, 0, 0, 0));
    const startTimestamp = admin.firestore.Timestamp.fromDate(startDate);

    logger.info(`[cf36] Querying products created after ${startDate.toISOString()}...`);

    // 2. Query products_new
    const snapshot = await db.collection("products_new")
      .where("createdAt", ">=", startTimestamp)
      .get();

    if (snapshot.empty) {
      logger.info("[cf36] No products found matching the date criteria.");
      res.json("Done - No products found.");
      return;
    }

    logger.info(`[cf36] Found ${snapshot.size} products created after the cutoff.Filtering for supplier_cf...`);

    // 3. Filter for supplier_cf (Double) not equal to 0
    const pDocs = [];
    snapshot.forEach(doc => {
      const data = doc.data();
      // Check if supplier_cf exists and is a number and not 0
      if (typeof data.supplier_cf === 'number' && data.supplier_cf !== 0) {
        pDocs.push(doc.id);
      }
    });

    logger.info(`[cf36] Identified ${pDocs.length} products with valid supplier_cf.`);

    if (pDocs.length === 0) {
      res.json("Done - No products matched supplier_cf criteria.");
      return;
    }

    // 4. Trigger cf38 for all pDocs
    logger.info(`[cf36] Triggering cf38 for ${pDocs.length} products...`);

    const factories = pDocs.map(id => {
      return () => callCF("cf38", { productId: id });
    });

    // Use existing helper for concurrent execution with retries
    await runPromisesInParallelWithRetry(factories);

    logger.info("[cf36] Finished triggering all cf38 calls.");

    // 5. End
    res.json(`Done - Triggered for ${pDocs.length} products.`);

  } catch (err) {
    logger.error("[cf36] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});