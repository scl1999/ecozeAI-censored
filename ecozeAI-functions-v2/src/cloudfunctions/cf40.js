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

exports.cf40 = onRequest({
  region: REGION,
  timeoutSeconds: 60, // A short timeout is sufficient
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  try {
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || "";
    if (!productId) {
      res.status(400).json({ error: "productId is required." });
      return;
    }

    const pRef = db.collection("products_new").doc(productId);
    const pSnap = await pRef.get();

    if (!pSnap.exists) {
      res.status(404).json({ error: `Product ${productId} not found.` });
      return;
    }
    const pData = pSnap.data() || {};
    const currentTier = pData.current_tier;

    if (currentTier === undefined || currentTier === null) {
      logger.warn(`[cf40] Product ${productId} has no current_tier set. Ending.`);
      res.json({ status: "skipped", reason: "Product has no current_tier." });
      return;
    }

    // 1. Find all materials for the product (ignoring tier).
    const baseQuery = db.collection("materials")
      .where("linked_product", "==", pRef);

    const allMaterialsSnap = await baseQuery.select("apcfMaterials_done", "updatedAt").get();

    if (allMaterialsSnap.empty) {
      logger.info(`[cf40] No materials found for product ${productId}. Scheduling re-check.`);
      await scheduleNextCheck(productId);
      res.json({ status: "pending", message: "No materials found. Re-checking in 5 minutes." });
      return;
    }

    // 2. Filter for incomplete materials
    const incompleteMaterials = allMaterialsSnap.docs.filter(doc => doc.data().apcfMaterials_done !== true);
    const nmDone = incompleteMaterials.length;

    // {If nmDone > 0}
    if (nmDone > 0) {
      logger.info(`[cf40] Product ${productId} has ${nmDone} incomplete materials.`);

      logger.info(`[cf40] Scheduling re-check.`);
      await scheduleNextCheck(productId);
      res.json({ status: "pending", message: `${nmDone} materials are still processing. Re-checking in 5 minutes.` });
      return;
    }

    // {If nmDone = 0}
    logger.info(`[cf40] All ${allMaterialsSnap.size} materials for product ${productId} are complete.`);
    // 3. Set the product status to "Done" and clear the scheduled flag.
    await pRef.update({
      status: "Done",
      status_check_scheduled: false
    });
    logger.info(`[cf40] Successfully set status to "Done" for product ${productId}.`);
    // 4. End the cloudfunction.
    res.json({ status: "complete", message: "All materials are processed. Product status set to Done." });

  } catch (err) {
    logger.error("[cf40] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});