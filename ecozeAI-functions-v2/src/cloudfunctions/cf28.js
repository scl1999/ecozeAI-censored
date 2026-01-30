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

exports.cf28 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  try {
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || "";
    if (!productId) {
      res.status(400).json({ error: "productId required" });
      return;
    }

    const pRef = db.collection("products_new").doc(productId);
    const pSnap = await pRef.get();

    if (!pSnap.exists) {
      res.status(404).json({ error: `Product ${productId} not found` });
      return;
    }
    const pData = pSnap.data() || {};
    const ctOriginal = pData.current_tier || 0;

    // 2. Find all material documents that need to be processed.
    const parentRefsSnap = await db.collection("materials")
      .where("parent_material", "!=", null)
      .select("parent_material")
      .get();
    const parentIds = new Set(parentRefsSnap.docs.map(doc => doc.data().parent_material.id));

    const candidatesSnap = await db.collection("materials")
      .where("linked_product", "==", pRef)
      .where("tier", "==", ctOriginal)
      .where("final_tier", "!=", true)
      .get();

    const mDocs = candidatesSnap.docs.filter(doc => !parentIds.has(doc.id));

    if (mDocs.length === 0) {
      const msg = "...";
      logger.info(`[cf28] ${msg}`);
      res.json({ status: "ok", message: msg });
      return;
    }
    logger.info(`[cf28] Found ${mDocs.length} materials at tier ${ctOriginal} to process.`);

    // 3. Increment the product's current_tier.
    const ctNew = ctOriginal + 1;
    await pRef.update({ current_tier: admin.firestore.FieldValue.increment(1) });
    logger.info(`[cf28] Incremented product tier to ${ctNew}.`);

    // 4. Trigger apcfMaterials2 for all identified materials.
    await Promise.all(
      mDocs.map(doc => callCF("cf25", { materialId: doc.id }))
    );
    logger.info(`[cf28] Triggered cf25 for ${mDocs.length} materials.`);

    // 5. Begin the monitoring loop.
    logger.info(`[cf28] Starting monitoring loop...`);
    while (true) {
      const mDocsLatestSnaps = await db.getAll(...mDocs.map(doc => doc.ref));
      const allOriginalJobsDone = mDocsLatestSnaps.every(snap => snap.data()?.apcfMaterials2_done === true);

      let nmTotal = 0, amfTotal = 0, asaTotal = 0, atcfTotal = 0, asfTotal = 0, ampcfTotal = 0;
      let allSubJobsDone = false;

      if (allOriginalJobsDone) {
        const baseQuery = db.collection("materials").where("linked_product", "==", pRef).where("tier", "==", ctNew);
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

        if (nmTotal > 0 && amfTotal === nmTotal && asaTotal === nmTotal && atcfTotal === nmTotal && asfTotal === nmTotal && ampcfTotal === nmTotal) {
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
        logger.info("[cf28] All new materials processed. Ending loop.");
        break;
      }

      await sleep(5000);
    }

    res.json({ status: "ok", message: "New tier processing and monitoring complete." });

  } catch (err) {
    logger.error("[cf28] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});