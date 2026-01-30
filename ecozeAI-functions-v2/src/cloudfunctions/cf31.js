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

exports.cf31 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf31] Invoked");
  try {
    // 1. Argument Parsing & Document Fetching
    const { productId } = req.body;
    if (!productId) {
      res.status(400).json({ error: "productId is required" });
      return;
    }

    const pRef = db.collection("products_new").doc(productId);
    const pSnap = await pRef.get();
    if (!pSnap.exists) {
      res.status(404).json({ error: `Product ${productId} not found` });
      return;
    }
    const pData = pSnap.data() || {};

    // 2. Gather URLs from sdCF data documents
    const dataSnap = await pRef.collection("pn_data").where("type", "==", "sdCF").get();
    const urls = [];
    if (!dataSnap.empty) {
      dataSnap.forEach(doc => {
        const url = doc.data().url;
        if (url) {
          urls.push(url);
        }
      });
    }
    logger.info(`[cf31] Found ${urls.length} source URLs for product ${productId}.`);

    // 3. Construct AI Prompt
    let query = `
Product or Activity: ${pData.name || "Unknown"}
Carbon footprint / GWP Total (kgCO2e): ${pData.supplier_cf || "Unknown"}

Sources (URLs):
`;
    if (urls.length > 0) {
      query += urls.map((url, i) => `url_${i + 1}: ${url}`).join('\n');
    } else {
      query += "No specific sources found in database. Please search the web for official manufacturer disclosures for the product specified.";
    }

    // 4. AI Call Configuration & Execution
    const SYS_OM = "...";

    const vGenerationConfig = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 24576
      },
    };

    const collectedUrls = new Set();
    const { answer, thoughts, cost, totalTokens, searchQueries, model, rawConversation } = await runGeminiStream({
      model: 'aiModel',//flash
      generationConfig: vGenerationConfig,
      user: query,
      collectedUrls,
    });

    await logAITransaction({
      cfName: 'cf31',
      productId: productId,
      cost: cost,
      totalTokens: totalTokens,
      searchQueries: searchQueries,
      modelUsed: model,
    });

    await logAIReasoning({
      sys: SYS_OM,
      user: query,
      thoughts: thoughts,
      answer: answer,
      cloudfunction: 'cf31',
      productId: productId,
      rawConversation: rawConversation,
    });

    if (collectedUrls.size > 0) {
      await saveURLs({
        urls: Array.from(collectedUrls),
        productId: productId,
        sys: SYS_OM,
        user: query,
        thoughts: thoughts,
        answer: answer,
        cloudfunction: 'cf31',
      });
    }

    // 5. Parse Response and Update Firestore
    const metrics = parseOtherMetrics(answer);
    const updatePayload = {};

    // Conditionally add metrics to the payload if they are valid numbers
    updatePayload.ap_total = metrics.ap_value;
    updatePayload.ep_total = metrics.ep_value;
    updatePayload.adpe_total = metrics.adpe_value;
    updatePayload.gwp_f_total = metrics.gwp_f_value;
    updatePayload.gwp_b_total = metrics.gwp_b_value;
    updatePayload.gwp_l_total = metrics.gwp_l_value;

    if (Object.keys(updatePayload).length > 0) {
      await pRef.update(updatePayload);
      logger.info(`[cf31] Updated product ${productId} with other metrics:`, updatePayload);
    } else {
      logger.warn(`[cf31] No valid metrics were found in the AI response for product ${productId}.`);
    }

    await pRef.update({ apcfOtherMetrics2_done: true });
    // 6. Finalize
    res.json("Done");
  } catch (err) {
    logger.error("[cf31] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});