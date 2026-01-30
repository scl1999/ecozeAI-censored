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

exports.cf32 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf32] Invoked");

  try {
    // --- 0. Argument Validation ---
    const { userId, collectionPN } = req.method === "POST" ? req.body : req.query;

    if (!userId || !collectionPN) {
      res.status(400).send("Error: Missing required arguments. 'userId' and 'collectionPN' are both required.");
      return;
    }

    // --- 1. Find all products in the specified collection ---
    const productsQuery = db.collection('products_new')
      .where('tu_id', '==', userId)
      .where('pn_collection', '==', collectionPN);

    const productsSnapshot = await productsQuery.get();

    if (productsSnapshot.empty) {
      logger.info(`[cf32] No products found for user '${userId}' in collection '${collectionPN}'. No action taken.`);
      res.status(200).send("Success: No products found to delete.");
      return;
    }

    logger.info(`[cf32] Found ${productsSnapshot.size} products to delete.`);

    // --- Loop through each product to delete it and its linked materials ---
    for (const pnDoc of productsSnapshot.docs) {
      logger.info(`[cf32] Processing product ${pnDoc.id}...`);

      // 2. Find all materials linked to this product
      const materialsQuery = db.collection('materials').where('linked_product', '==', pnDoc.ref);
      const materialsSnapshot = await materialsQuery.get();

      // 3. Delete all linked materials and their subcollections
      if (!materialsSnapshot.empty) {
        logger.info(`[cf32] Found ${materialsSnapshot.size} linked materials for product ${pnDoc.id}.`);
        for (const materialDoc of materialsSnapshot.docs) {
          logger.info(`[cf32] --> Deleting material ${materialDoc.id} and its subcollections.`);
          await deleteDocumentAndSubcollections(materialDoc.ref);
        }
      }

      // 4. Delete the product document itself and its subcollections
      logger.info(`[cf32] --> Deleting product ${pnDoc.id} and its subcollections.`);
      await deleteDocumentAndSubcollections(pnDoc.ref);
    }

    // --- 5. End the function ---
    logger.info(`[cf32] Successfully deleted collection '${collectionPN}' for user '${userId}'.`);
    res.status(200).send("Success");

  } catch (err) {
    logger.error("[cf32] Uncaught error during deletion process:", err);
    res.status(500).send("An internal error occurred during the delete operation.");
  }
});

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------

/****************************************************************************************
 * CBAM $$$
 ****************************************************************************************/