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
const prompts = "..."('../../services/ai/prompts');
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

exports.cf18 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf18] Invoked");
  try {
    // 1. Argument Parsing and Validation
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || null;
    if (!productId) {
      res.status(400).json({ error: "Provide productId" });
      return;
    }

    // 2. Fetch the product document
    const targetRef = db.collection("products_new").doc(productId);
    const targetSnap = await targetRef.get();
    if (!targetSnap.exists) {
      res.status(404).json({ error: `Document not found` });
      return;
    }
    const targetData = targetSnap.data() || {};
    const prodName = (targetData.name || "").trim();

    // 4. Construct prompts for the main AI call
    const USER_MSG = "...";
    const SYS_MSG = "...";

    // 5. Perform the single AI call
    const vGenerationConfig = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 32768
      },
    };

    const collectedUrls = new Set();

    const { answer, thoughts, cost, totalTokens, searchQueries, model, rawConversation } = await runGeminiStream({
      model: 'aiModel', //pro
      generationConfig: vGenerationConfig,
      user: USER_MSG,
      collectedUrls
    });

    // 6. Log the AI interaction
    await logAITransaction({
      cfName: 'cf18',
      productId: productId,
      cost: cost,
      totalTokens: totalTokens,
      searchQueries: searchQueries,
      modelUsed: model,
    });

    await logAIReasoning({
      sys: SYS_MSG,
      user: USER_MSG,
      thoughts: thoughts,
      answer: answer,
      cloudfunction: 'cf18',
      productId: productId,
      rawConversation: rawConversation,
    });

    if (collectedUrls.size) {
      await saveURLs({
        urls: Array.from(collectedUrls),
        productId,
        pMPCFData: true,
        sys: SYS_MSG,
        user: USER_MSG,
        thoughts: thoughts,
        answer: answer,
        cloudfunction: 'cf18',
      });
    }

    // 7. Parse the AI result
    const aiCalc = parseCfValue(answer);

    // 8. Update Firestore if the result is valid
    if (aiCalc !== null) {
      await targetRef.update({
        cf_full: aiCalc,
      });
      logger.info(`[cf18] 🏁 Firestore update committed for value: ${aiCalc} `);
    } else {
      logger.warn("[cf18] ⚠️ AI did not supply a numeric *cf_value*. No updates made.");
    }

    // 9. Trigger Other Metrics Calculation
    logger.info(`[cf18] Checking if other metrics calculation is needed...`);
    if (targetData.otherMetrics === true) {
      logger.info(`[cf18] otherMetrics flag is true for product ${productId}.Triggering calculation.`);
      await callCF("cf30", {
        productId: productId,
        calculationLabel: "cf18"
      });
    }

    logger.info(`[cf18] Checking if other metrics calculation is needed...`);
    if (targetData.otherMetrics === true) {
      logger.info(`[cf18] otherMetrics flag is true for product ${productId}.Triggering calculation.`);
      await callCF("cf30", {
        productId: productId,
        calculationLabel: "cf18"
      });
    }

    // 10. Finalize the function
    await targetRef.update({
      apcfMPCFFullGeneric_done: true,
      apcfMPCF_done: true, // Also set the main flag to prevent other loops
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    res.json("Done");

  } catch (err) {
    logger.error("[cf18] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});