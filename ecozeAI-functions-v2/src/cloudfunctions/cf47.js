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

exports.cf47 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM_SUPPLIER_FINDER_DR,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf47] Invoked");

  try {
    const materialId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || null;

    if (!materialId) {
      res.status(400).json({ error: "Missing materialId" });
      return;
    }

    const mRef = db.collection("materials").doc(materialId);
    const mSnap = await mRef.get();

    if (!mSnap.exists) {
      res.status(404).json({ error: `Material ${materialId} not found` });
      return;
    }

    logger.info(`[cf47] Resetting supplier fields for material ${materialId}...`);

    // 1. Reset Fields
    await mRef.update({
      supplier_name: admin.firestore.FieldValue.delete(),
      manufacturer_name: admin.firestore.FieldValue.delete(),
      other_known_suppliers: admin.firestore.FieldValue.delete(),
      supplier_finder_fcr: admin.firestore.FieldValue.delete(),
      supplier_finder_fcr_reasoning: admin.firestore.FieldValue.delete(),
      supplier_finder_retries: admin.firestore.FieldValue.delete(),
      supplier_confidence: admin.firestore.FieldValue.delete(),
      manufacturer_confidence: admin.firestore.FieldValue.delete(),
      supplier_probability_percentage: admin.firestore.FieldValue.delete(),
      manufacturer_probability_percentage: admin.firestore.FieldValue.delete(),
      supplier_estimated: admin.firestore.FieldValue.delete(),
      other_potential_suppliers: admin.firestore.FieldValue.delete(),
      supplier_evidence_rating: admin.firestore.FieldValue.delete(),
      other_suppliers: admin.firestore.FieldValue.delete(),
      apcfSupplierFinder_done: false,
      // Also potentially reset status if it was stuck
      status: "In Progress (Retry)"
    });

    // 1b. Rename existing reasoning logs
    const reasoningRef = mRef.collection("m_reasoning");
    const reasoningSnap = await reasoningRef.get();

    if (!reasoningSnap.empty) {
      const batch = db.batch();
      let updateCount = 0;

      reasoningSnap.forEach(doc => {
        const data = doc.data();
        if (data.cloudfunction && typeof data.cloudfunction === 'string' && data.cloudfunction.startsWith("cf43")) {
          // Check if it already has _initial to avoid double appending if run multiple times
          if (!data.cloudfunction.endsWith("_initial")) {
            batch.update(doc.ref, {
              cloudfunction: data.cloudfunction + "_initial"
            });
            updateCount++;
          }
        }
      });

      if (updateCount > 0) {
        await batch.commit();
        logger.info(`[cf47] Renamed ${updateCount} reasoning logs to *_initial.`);
      }
    }

    logger.info(`[cf47] Fields reset. Triggering cf46...`);

    // 2. Trigger cf46 and wait
    await callCF("cf46", { materialId: materialId });

    logger.info(`[cf47] cf46 finished.`);

    // 3. Set Status to Done
    await mRef.update({
      status: "Done"
    });

    res.json("Done");

  } catch (err) {
    logger.error("[cf47] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});