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

exports.cf1 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf1] Invoked");
  try {
    /******************** 1. Argument validation ********************/
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || "";
    if (!productId) {
      res.status(400).json({ error: "productId is required" });
      return;
    }

    const pRef = db.collection("products_new").doc(productId);
    const pSnap = await pRef.get();
    if (!pSnap.exists) {
      res.status(404).json({ error: `Product ${productId} not found` });
      return;
    }
    const pData = pSnap.data() || {};

    /******************** 2. AI Call: Product CBAM Scope Check ********************/
    const SYS_CBAM_PRODUCT = "...";

    const vGenerationConfigProduct = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 32768
      },
    };

    const { answer: productAnswer, thoughts: productThoughts, cost: productCost, totalTokens: productTokens, searchQueries: productQueries, model: productModel, rawConversation: productRawConvo } = await runGeminiStream({
      model: 'aiModel', //pro
      generationConfig: vGenerationConfigProduct,
      user: userQueryProduct,
    });

    await logAITransaction({
      cfName: 'cf1-ProductScope',
      productId: productId,
      cost: productCost,
      totalTokens: productTokens,
      searchQueries: productQueries,
      modelUsed: productModel
    });

    await logAIReasoning({
      sys: SYS_CBAM_PRODUCT,
      user: userQueryProduct,
      thoughts: productThoughts,
      answer: productAnswer,
      cloudfunction: 'cf1-ProductScope',
      productId: productId,
      rawConversation: productRawConvo
    });

    /******************** 3. Process AI Response & Update Product ********************/
    const productCBAMInfo = parseProductCBAM(productAnswer);

    const updatePayload = {
      cbam_in_scope: productCBAMInfo.inScope,
      cbam_in_scope_reasoning: productCBAMInfo.reasoning,
      cn_code: productCBAMInfo.cnCode,
      cbam_est_cost: productCBAMInfo.estCost,
      carbon_price_paid: productCBAMInfo.carbonPrice,
    };
    await pRef.update(updatePayload);

    if (productCBAMInfo.inScope !== true) {
      logger.info(`[cf1] Product ${productId} is not in scope for CBAM. Ending function.`);
      res.json("Done");
      return;
    }

    /******************** 4. Fetch Materials & Check Their Scope ********************/
    const materialsSnap = await db.collection("materials").where("linked_product", "==", pRef).get();
    if (materialsSnap.empty) {
      logger.info(`[cf1] Product ${productId} is in scope but has no materials. Ending function.`);
      res.json("Done");
      return;
    }

    const materialLines = materialsSnap.docs.map((doc, i) => {
      const data = doc.data();
      return `material_${i + 1}: ${data.name || 'Unknown'}\nmaterial_description_${i + 1}: ${data.description || 'No description'}`;
    }).join("\n\n");

    const SYS_CBAM_MATERIALS = "...";

    const vGenerationConfigMaterials = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 32768
      },
    };

    const { answer: materialAnswer, thoughts: materialThoughts, cost: materialCost, totalTokens: materialTokens, searchQueries: materialQueries, model: materialModel, rawConversation: materialRawConvo } = await runGeminiStream({
      model: 'aiModel', //pro
      generationConfig: vGenerationConfigMaterials,
      user: materialLines,
    });

    await logAITransaction({
      cfName: 'cf1-MaterialScope',
      productId: productId,
      cost: materialCost,
      totalTokens: materialTokens,
      searchQueries: materialQueries,
      modelUsed: materialModel,
    });

    await logAIReasoning({
      sys: SYS_CBAM_MATERIALS,
      user: materialLines,
      thoughts: materialThoughts,
      answer: materialAnswer,
      cloudfunction: 'cf1-MaterialScope',
      productId: productId,
      rawConversation: materialRawConvo,
    });

    /******************** 5. Update In-Scope Materials & Trigger Next Steps ********************/
    const inScopeMaterials = parseMaterialCBAM(materialAnswer);
    if (inScopeMaterials.length === 0) {
      logger.info(`[cf1] No child materials for product ${productId} are in scope for CBAM.`);
      res.json("Done");
      return;
    }

    const nameToDocMap = new Map(materialsSnap.docs.map(doc => [doc.data().name, doc]));
    const batch = db.batch();
    const msDocs = [];

    for (const material of inScopeMaterials) {
      const docToUpdate = nameToDocMap.get(material.name);
      if (docToUpdate) {
        batch.update(docToUpdate.ref, { cn_code: material.cn_code });
        msDocs.push(docToUpdate);
      }
    }
    await batch.commit();
    logger.info(`[cf1] Updated ${msDocs.length} materials with CN codes.`);

    const promises = [callCF("cf4", { productId })];

    for (const doc of msDocs) {
      const data = doc.data();
      const materialId = doc.id;
      if (data.final_tier === true) {
        promises.push(callCF("cf3", { productId, materialId }));
      } else {
        promises.push(callCF("cf2", { productId, materialId }));
      }
    }

    await Promise.all(promises);
    logger.info(`[cf1] All subsequent CBAM functions have been triggered and completed for product ${productId}.`);

    /******************** 6. Finalize ********************/
    res.json("Done");

  } catch (err) {
    logger.error("[cf1] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});