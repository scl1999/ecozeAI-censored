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

exports.cf2 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf2] Invoked");
  try {
    /******************** 1. Argument validation & Data Fetching ********************/
    const { productId, materialId } = req.body;
    if (!productId || !materialId) {
      res.status(400).json({ error: "Both productId and materialId are required" });
      return;
    }

    const mRef = db.collection("materials").doc(materialId);
    const mSnap = await mRef.get();
    if (!mSnap.exists) {
      res.status(404).json({ error: `Material ${materialId} not found` });
      return;
    }
    const mData = mSnap.data() || {};

    const pRef = db.collection("products_new").doc(productId);
    const pSnap = await pRef.get();
    if (!pSnap.exists) {
      res.status(404).json({ error: `Product ${productId} not found` });
      return;
    }

    // Find all child materials (m2Docs)
    const childMaterialsSnap = await db.collection("materials")
      .where("parent_material", "==", mRef)
      .get();

    /******************** 2. AI Call ********************/
    const SYS_CBAM_MATERIAL_PROCESSING = "...";

    let userQuery = `...`;

    childMaterialsSnap.docs.forEach((doc, i) => {
      const m2Data = doc.data();
      userQuery += `...`;
    });

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

    const { answer, thoughts, cost, totalTokens, searchQueries, model, rawConversation } = await runGeminiStream({
      model: 'aiModel', //pro
      generationConfig: vGenerationConfig,
      user: userQuery,
    });

    await logAITransaction({
      cfName: 'cf2',
      productId: productId,
      materialId: materialId,
      cost: cost,
      totalTokens: totalTokens,
      searchQueries: searchQueries,
      modelUsed: model,
    });

    await logAIReasoning({
      sys: SYS_CBAM_MATERIAL_PROCESSING,
      user: userQuery,
      thoughts: thoughts,
      answer: answer,
      cloudfunction: 'cf2',
      productId: productId,
      materialId: materialId,
      rawConversation: rawConversation,
    });

    /******************** 3. Process & Save AI Response ********************/
    const parsedData = parseProductCBAMProcessing(answer);

    if (!parsedData.processName && parsedData.fuels.length === 0 && parsedData.wastes.length === 0) {
      logger.warn(`[cf2] AI returned no parsable data for material ${materialId}.`);
    } else {
      const payload = {
        material: mRef, // Add reference to the parent material
        name: parsedData.processName,
        cbam_fes: parsedData.fuels,
        cbam_waste_materials: parsedData.wastes,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      };
      await pRef.collection("pn_cbam").add(payload);
      logger.info(`[cf2] Saved CBAM processing data to subcollection for material ${materialId}.`);
    }

    /******************** 4. Finalize ********************/
    res.json("Done");

  } catch (err) {
    logger.error("[cf2] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});