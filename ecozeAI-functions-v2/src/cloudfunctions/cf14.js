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

exports.cf14 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  try {
    // --- MODIFIED: Parse both productName and the new tuId argument ---
    const productName = req.method === "POST" ? req.body?.product_name : req.query.product_name;
    const tuId = req.method === "POST" ? req.body?.tuId : req.query.tuId;

    // --- MODIFIED: Validate both arguments ---
    if (!productName || !productName.trim() || !tuId || !tuId.trim()) {
      res.status(400).json({ error: "product_name and tuId are required" });
      return;
    }

    // 1. Trigger the main cf11 function in the background.
    logger.info(`[cf14] Triggering cf11 for product: "${productName}"`);
    callCF("cf11", {
      product_name: productName,
    }).catch(err => {
      // Log errors from the background task but don't stop the monitor.
      logger.error(`[cf14] Background call to cf11 failed:`, err);
    });

    // 6. Loop until the product document is created by cf11.
    let pRef = null;
    const searchTimeout = Date.now() + 180000; // 3-minute timeout to find the doc.
    logger.info(`[cf14] Waiting for product document to be created...`);

    while (Date.now() < searchTimeout) {
      const snap = await db.collection("products_new").where("name", "==", productName).limit(1).get();
      if (!snap.empty) {
        pRef = snap.docs[0].ref;
        logger.info(`[cf14] Found product document: ${pRef.id}`);
        break;
      }
      await sleep(5000); // Wait 5 seconds before checking again.
    }

    if (!pRef) {
      const msg = "..."
      logger.error(`[cf14] ${msg}`);
      res.status(408).json({ error: msg });
      return;
    }

    // --- NEW: Update the newly found document with the tuId ---
    await pRef.update({ tu_id: tuId });
    logger.info(`[cf14] Set tu_id to "${tuId}" for product ${pRef.id}`);


    // --- Main Monitoring Loop ---
    logger.info(`[cf14] Starting monitoring loop for product ${pRef.id}.`);
    while (true) {
      const pSnap = await pRef.get();
      const pData = pSnap.data() || {};

      // 7. Check if the BoM generation and initial analysis are complete.
      const initial2Done = pData.apcfInitial2_done === true;

      let nmTotal = 0, amfTotal = 0, asaTotal = 0, atcfTotal = 0, asfTotal = 0, ampcfTotal = 0;
      let allSubJobsDone = false;

      // 8. If the initial step is done, we can check the status of all materials.
      if (initial2Done) {
        // 9. Fetch progress counts for all materials linked to the product.
        const baseQuery = db.collection("materials").where("linked_product", "==", pRef);
        const [
          nmSnap, amfSnap, asaSnap,
          atcfSnap, asfSnap, ampcfSnap
        ] = await Promise.all([
          baseQuery.count().get(),
          baseQuery.where("apcfMassFinder_done", "==", true).count().get(),
          baseQuery.where("apcfSupplierAddress_done", "==", true).count().get(),
          baseQuery.where("apcfTransportCF_done", "==", true).count().get(),
          baseQuery.where("apcfSupplierFinder_done", "==", true).count().get(),
          baseQuery.where("apcfMPCF_done", "==", true).count().get()
        ]);

        nmTotal = nmSnap.data().count;
        amfTotal = amfSnap.data().count;
        asaTotal = asaSnap.data().count;
        atcfTotal = atcfSnap.data().count;
        asfTotal = asfSnap.data().count;
        ampcfTotal = ampcfSnap.data().count;

        // 11. Check if the loop can end.
        // This happens if no materials were created, or if all materials are fully processed.
        if (nmTotal === 0) {
          allSubJobsDone = true;
        } else if (amfTotal === nmTotal && asaTotal === nmTotal && atcfTotal === nmTotal && asfTotal === nmTotal && ampcfTotal === nmTotal) {
          allSubJobsDone = true;
        }
      }

      // 10. Print the progress to logs.
      logger.info("\n=======================================\n");
      logger.info(`Total Materials: ${nmTotal}`);
      logger.info(`\nSupplier: ${asfTotal} / ${nmTotal}`);
      logger.info(`Supplier Address: ${asaTotal} / ${nmTotal}`);
      logger.info(`Mass: ${amfTotal} / ${nmTotal}`);
      logger.info(`TransportCF: ${atcfTotal} / ${nmTotal}`);
      logger.info(`MPCF: ${ampcfTotal} / ${nmTotal}`);
      logger.info("\n=======================================\n");

      if (allSubJobsDone) {
        logger.info(`[cf14] All materials for product ${pRef.id} are processed. Ending loop.`);
        break;
      }

      await sleep(5000); // Wait 5 seconds before the next check.
    }

    // 12. End the cloud function.
    res.json("Done");

  } catch (err) {
    logger.error("[cf14] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});