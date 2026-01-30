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

exports.cf39 = onRequest({
  region: REGION,
  timeoutSeconds: 60,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  try {
    const materialId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || "";
    if (!materialId) {
      res.status(400).json({ error: "materialId is required." });
      return;
    }

    const mRef = db.collection("materials").doc(materialId);
    const mSnap = await mRef.get();

    if (!mSnap.exists) {
      res.status(404).json({ error: `Material ${materialId} not found.` });
      return;
    }
    const mData = mSnap.data() || {};
    const pRef = mData.linked_product;

    // A linked product is necessary to schedule the main checker or set final status
    if (!pRef) {
      logger.error(`[cf39] Material ${materialId} has no linked_product. Cannot proceed.`);
      res.status(400).json({ error: "Material is missing a linked_product reference." });
      return;
    }

    // 1. Find all child materials (m2Docs) that have the current material as a parent.
    const childrenQuery = db.collection("materials").where("parent_material", "==", mRef);
    const childrenSnap = await childrenQuery.select().get();

    // {{If no child materials are found}}
    if (childrenSnap.empty) {
      logger.info(`[cf39] No sub-materials found for parent ${materialId}. Scheduling main status check.`);
      // 2. Schedule the main status checker and 3. End early.
      await scheduleNextCheck(pRef.id);
      res.json({ status: "pending", message: "No sub-materials found. Re-scheduling main status check in 5 minutes." });
      return;
    }

    // {{If child materials are found}}
    // 2. Count how many children have not completed their MPCF calculation.
    const incompleteChildrenSnap = await childrenQuery.where("apcfMPCF_done", "==", false).count().get();
    const nm2Done = incompleteChildrenSnap.data().count;

    // {If nm2Done > 0}
    if (nm2Done > 0) {
      logger.info(`[cf39] Parent ${materialId} has ${nm2Done} incomplete sub-materials. Scheduling main status check.`);
      // 3. Schedule the main status checker and 4. End early.
      await scheduleMainStatusCheck(pRef.id);
      res.json({ status: "pending", message: `${nm2Done} sub-materials are still processing. Re-scheduling main status check in 5 minutes.` });
      return;
    }

    // {If nm2Done = 0}
    logger.info(`[cf39] All ${childrenSnap.size} sub-materials for parent ${materialId} are complete.`);
    // 3. Set the linked product's status to "Done".
    await pRef.update({ status: "Done" });
    logger.info(`[cf39] Successfully set status to "Done" for product ${pRef.id}.`);
    // 4. End the cloudfunction.
    res.json({ status: "complete", message: "All sub-materials are processed. Product status set to Done." });

  } catch (err) {
    logger.error("[cf39] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------

/****************************************************************************************
 * Backend Updates $$$
 ****************************************************************************************/