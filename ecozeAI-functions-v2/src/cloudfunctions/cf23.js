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

exports.cf23 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf23] Invoked");

  try {
    /******************** 1. Argument validation ********************/
    const { materialsNewList, productId, materialId } = req.body;
    const entityType = productId ? 'product' : 'material';

    if (!Array.isArray(materialsNewList) || materialsNewList.length === 0 ||
      (!productId && !materialId) || (productId && materialId)) {
      res.status(400).json({ error: "Provide a materialsNewList array and exactly one of productId OR materialId" });
      return;
    }

    /******************** 2. Fetch Parent Doc & Data ********************/
    let parentRef;
    let parentName = "";
    let parentMass = "Unknown";
    let parentSupplyChain = "";
    let parentDescription = "";

    if (productId) {
      parentRef = db.collection("products_new").doc(productId);
      const pSnap = await parentRef.get();
      if (!pSnap.exists) {
        res.status(404).json({ error: `Product ${productId} not found` });
        return;
      }
      const pData = pSnap.data() || {};
      parentName = pData.name || "Unknown";
      parentDescription = pData.description || "No description provided.";
      if (pData.mass && pData.mass_unit) {
        parentMass = `${pData.mass} ${pData.mass_unit}`;
      }
    } else { // materialId must be present
      parentRef = db.collection("materials").doc(materialId);
      const mpSnap = await parentRef.get();
      if (!mpSnap.exists) {
        res.status(404).json({ error: `Material ${materialId} not found` });
        return;
      }
      const mpData = mpSnap.data() || {};
      parentName = mpData.name || "Unknown";
      parentDescription = mpData.description || "No description provided.";
      parentSupplyChain = mpData.product_chain || "";
      if (mpData.mass && mpData.mass_unit) {
        parentMass = `${mpData.mass} ${mpData.mass_unit}`;
      }
    }

    /******************** 3. Build Prompt from BoM ********************/
    const materialDocs = await Promise.all(
      materialsNewList.map(id => db.collection("materials").doc(id).get())
    );

    const materialNameIdMap = new Map();
    const bomLines = materialDocs.map((doc, index) => {
      if (!doc.exists) return "";
      const data = doc.data();
      const name = data.name || "Unknown";
      const description = data.description || "No description provided.";
      const mass = data.mass ?? "Unknown";
      const unit = data.mass_unit || "";

      materialNameIdMap.set(name, doc.id); // Map name to ID for easy updates later

      return `material_${index + 1}_name: ${name}\nmaterial_${index + 1}_description: ${description}\nmaterial_${index + 1}_mass: ${mass}${unit ? ' ' + unit : ''}`;
    }).filter(Boolean).join("\n\n");

    let userPrompt = "...";
    if (parentSupplyChain) {
      userPrompt += `...`;

    /******************** 4. Define System Prompt & AI Call ********************/
    const sysPrompt = "..."

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
      model: 'aiModel', //flash3
      generationConfig: vGenerationConfig,
      user: userPrompt,
      collectedUrls,
    });

    await logAITransaction({
      cfName: 'cf23',
      productId: entityType === 'product' ? productId : parentRef.id,
      materialId,
      cost,
      totalTokens,
      searchQueries,
      modelUsed: model,
    });

    await logAIReasoning({
      sys: sysPrompt,
      user: userPrompt,
      thoughts,
      answer,
      cloudfunction: 'cf23',
      productId,
      materialId,
      rawConversation: rawConversation,
    });

    if (collectedUrls.size) {
      await saveURLs({
        urls: Array.from(collectedUrls),
        productId,
        materialId,
        pMassReviewData: !!productId,
        mMassReviewData: !!materialId,
        sys: sysPrompt,
        user: userPrompt,
        thoughts,
        answer,
        cloudfunction: 'cf23',
      });
    }

    /******************** 5. Process AI Response ********************/
    if (answer.trim().toLowerCase() === "done") {
      logger.info("[cf23] AI confirmed all masses are correct.");
      await parentRef.update({ apcfMassReview_done: true });
      res.json("Done");
      return;
    }

    const corrections = parseMassCorrections(answer);
    logger.info(`[cf23] AI flagged ${corrections.length} material(s) for mass correction.`);

    if (corrections.length > 0) {
      const batch = db.batch();
      for (const correction of corrections) {
        const docIdToUpdate = materialNameIdMap.get(correction.name);
        if (docIdToUpdate && correction.newMass !== null) {
          const docRef = db.collection("materials").doc(docIdToUpdate);
          batch.update(docRef, {
            mass: correction.newMass,
            mass_unit: correction.newUnit,
            massAmendedReasoning: correction.reasoning,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          });
          logger.info(`[cf23] Queued update for material: ${correction.name} (${docIdToUpdate})`);
        } else {
          logger.warn(`[cf23] Could not find material named "${correction.name}" or new mass was invalid.`);
        }
      }
      await batch.commit();
      logger.info("[cf23] Committed all mass corrections.");
    }

    await parentRef.update({ apcfMassReview_done: true, updatedAt: admin.firestore.FieldValue.serverTimestamp() });
    res.json("Done");

  } catch (err) {
    logger.error("[cf23] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});