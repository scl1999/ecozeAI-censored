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

exports.cf3 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf3] Invoked");
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

    /******************** 2. AI Call ********************/
    const SYS_CBAM_FINAL_MATERIAL = "...";

    const userQuery = `...`;

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
      cfName: 'cf3',
      productId: productId,
      materialId: materialId,
      cost: cost,
      totalTokens: totalTokens,
      searchQueries: searchQueries,
      modelUsed: model,
    });

    await logAIReasoning({
      sys: SYS_CBAM_FINAL_MATERIAL,
      user: userQuery,
      thoughts: thoughts,
      answer: answer,
      cloudfunction: 'cf3',
      productId: productId,
      materialId: materialId,
      rawConversation: rawConversation,
    });

    /******************** 3. Process & Save AI Response ********************/
    const parsedData = parseProductCBAMProcessing(answer);

    if (!parsedData.processName && parsedData.fuels.length === 0 && parsedData.wastes.length === 0) {
      logger.warn(`[cf3] AI returned no parsable data for final-tier material ${materialId}.`);
    } else {
      const payload = {
        material: mRef,
        name: parsedData.processName,
        cbam_fes: parsedData.fuels.map(f => ({
          name: f.name,
          amount: f.amount,
          amount_unit: f.amount_unit,
          scope: f.scope,
          co2e_kg: f.co2e_kg,
        })),
        cbam_waste_materials: parsedData.wastes.map(w => ({
          material_name: w.material_name,
          amount: w.amount,
          co2e_kg: w.co2e_kg,
        })),
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      };
      await pRef.collection("pn_cbam").add(payload);
      logger.info(`[cf3] Saved CBAM creation data to subcollection for final-tier material ${materialId}.`);
    }

    /******************** 4. Finalize ********************/
    res.json("Done");

  } catch (err) {
    logger.error("[cf3] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------

/****************************************************************************************
 * Other Cloudfunctions $$$
 ****************************************************************************************/