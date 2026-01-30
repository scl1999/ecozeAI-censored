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

exports.cf27 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  try {
    const materialId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || "";
    if (!materialId) {
      res.status(400).json({ error: "materialId is required" });
      return;
    }

    const mRef = db.collection("materials").doc(materialId);

    // 1. Check if the material already has children.
    const childrenCountSnap = await db.collection("materials")
      .where("parent_material", "==", mRef)
      .count()
      .get();

    if (childrenCountSnap.data().count > 0) {
      const msg = "...";
      logger.info(`[cf27] Skipped: ${msg}`);
      res.json({ status: "skipped", reason: msg });
      return;
    }

    // 2. Check if the material meets the conditions for processing.
    const mSnap = await mRef.get();
    if (!mSnap.exists) {
      res.status(404).json({ error: `Material ${materialId} not found.` });
      return;
    }
    const mData = mSnap.data() || {};

    if (mData.final_tier === true || (mData.tier || 0) > 5) {
      const msg = "...";
      logger.info(`[cf27] Skipped: ${msg}`);
      res.json({ status: "skipped", reason: msg });
      return;
    }

    // 3. Trigger the apcfMaterials2 cloud function for the material.
    logger.info(`[cf27] Triggering cf25 for material ${materialId}.`);
    await callCF("cf25", { materialId });

    // --- Monitoring Loop ---
    logger.info(`[cf27] Starting monitoring loop for children of ${materialId}.`);
    while (true) {
      const mSnapLatest = await mRef.get();
      const originalJobDone = mSnapLatest.data()?.apcfMaterials2_done === true;

      let nmTotal = 0, amfTotal = 0, asaTotal = 0, atcfTotal = 0, asfTotal = 0, ampcfTotal = 0;
      let allSubJobsDone = false;

      if (originalJobDone) {
        const baseQuery = db.collection("materials").where("parent_material", "==", mRef);
        const [
          nmSnap, amfSnap, asaSnap,
          atcfSnap, asfSnap, ampcfSnap
        ] = await Promise.all([
          baseQuery.count().get(),
          baseQuery.where("...", "==", true).count().get(),
          baseQuery.where("...", "==", true).count().get(),
          baseQuery.where("...", "==", true).count().get(),
          baseQuery.where("...", "==", true).count().get(),
          baseQuery.where("...", "==", true).count().get()
        ]);

        nmTotal = nmSnap.data().count;
        amfTotal = amfSnap.data().count;
        asaTotal = asaSnap.data().count;
        atcfTotal = atcfSnap.data().count;
        asfTotal = asfSnap.data().count;
        ampcfTotal = ampcfSnap.data().count;

        if (nmTotal === 0) {
          allSubJobsDone = true;
        } else if (amfTotal === nmTotal && asaTotal === nmTotal && atcfTotal === nmTotal && asfTotal === nmTotal && ampcfTotal === nmTotal) {
          allSubJobsDone = true;
        }
      }

      logger.info("\n=======================================\n");
      logger.info(`New Materials: ${nmTotal}`);
      logger.info(`\nSupplier: ${asfTotal} / ${nmTotal}`);
      logger.info(`Supplier Address: ${asaTotal} / ${nmTotal}`);
      logger.info(`Mass: ${amfTotal} / ${nmTotal}`);
      logger.info(`TransportCF: ${atcfTotal} / ${nmTotal}`);
      logger.info(`MPCF: ${ampcfTotal} / ${nmTotal}`);
      logger.info("\n=======================================\n");

      if (allSubJobsDone) {
        logger.info(`[cf27] All new materials for parent ${materialId} are processed. Ending loop.`);
        break;
      }

      await sleep(5000);
    }

    res.json("Done");

  } catch (err) {
    logger.error("[cf27] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});