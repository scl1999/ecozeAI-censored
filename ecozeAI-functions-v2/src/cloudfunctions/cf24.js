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

exports.cf24 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {

  /* ───────── Invocation log ───────── */
  const rawArgs = req.method === "POST" ? req.body : req.query;

  /* ───────── Validate materialId ───────── */
  const mId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || "";
  if (!mId.trim()) {
    res.status(400).json({ error: "materialId is required" });
    return;
  }

  const mRef = db.collection("materials").doc(mId);
  let snap = await mRef.get();
  if (!snap.exists) {
    res.status(404).json({ error: `material ${mId} not found` });
    return;
  }

  let data = snap.data() || {};

  logger.info(`[cf24] bootstrap for ${mId}`);

  /* ───────── helper: build supply-chain string ───────── */


  /* ───────── Step 3 - final-tier verdict ───────── */
  if (!data.final_tier) {
    const sys =
      '...';

    const modelUsed = 'openai/gpt-oss-120b-maas'; //gpt-oss-120b
    const vGenerationConfig = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 24576 // Correct budget for pro model
      },
    };

    // Call the refactored helper, which now returns the cost
    const productChain = data.product_chain || data.name; // Get pre-built chain

    // Define the user prompt as a constant BEFORE using it
    const userPromptForVerdict = "..." +
      `Supplier: ${data.supplier_name}\n` +
      `Supply Chain: ${productChain}`;

    const { answer: verdict, thoughts, totalTokens, cost, model, rawConversation } = await runGeminiStream({
      model: modelUsed,
      generationConfig: vGenerationConfig,
      user: userPromptForVerdict, // Use the constant here
    });

    // Call the new, simpler logger with the pre-calculated cost
    const linkedProductId = data.linked_product ? data.linked_product.id : null;
    await logAITransaction({
      cfName: 'cf24',
      productId: linkedProductId,
      materialId: mId,
      cost,
      totalTokens,
      modelUsed: model
    });

    await logAIReasoning({
      sys: sys,
      user: userPromptForVerdict, // And use the same constant here
      thoughts: thoughts,
      answer: verdict,
      cloudfunction: 'cf24',
      materialId: mId,
      rawConversation: rawConversation,
    });

    if (verdict === "Done") {
      await mRef.update({ final_tier: true });
    } else if (verdict === "SoS") {
      await mRef.update({ final_tier: true, software_or_service: true });
    }
  }

  /* ───────── Step 3½ ───────── */
  data = (await mRef.get()).data() || {};
  let pRef = data.linked_product || null;
  let pData = pRef ? (await pRef.get()).data() || {} : {};

  /* ───────── cf42 ───────── */
  /*
  if (
    data.apcfSupplierDisclosedCF_done !== true && // <-- New condition
    data.supplier_name &&
    data.supplier_name !== "Unknown"
  ) {
    await callCF("cf42", { materialId: mId, productId: null })
      .catch(() => { });
    await mRef.update({ apcfSupplierDisclosedCF_done: true }); // <-- New line
  }
  */

  /* ╔═══════════════ PATH B - no loop; run post-loop tasks ═══════════════╗ */
  logger.info("[cf24] allowSearch=false - executing clean-up");

  data = (await mRef.get()).data() || {}; // Refresh data before checks

  /* Supplier address (if missing) */
  if (
    data.apcfSupplierAddress_done !== true &&
    data.supplier_name &&
    data.supplier_name !== "Unknown" &&
    (!data.supplier_address || data.supplier_address === "Unknown")
  ) {
    await callCF("cf41", { materialId: mId, productId: null }).catch(() => { });
    await mRef.update({ apcfSupplierAddress_done: true });
  }

  /******************** Run post-loop tasks in parallel ********************/
  logger.info(`[cf24] Running post-loop tasks for ${mId}`);
  data = (await mRef.get()).data() || {}; // Refresh data once at the start

  const tasksToRun = [];
  const updatePayload = {};

  // Check conditions for cf49
  if (
    data.software_or_service !== true &&
    data.apcfTransportCF_done !== true &&
    data.supplier_name && data.supplier_name !== "Unknown" &&
    data.supplier_address && data.supplier_address !== "Unknown"
  ) {
    logger.info(`[cf24] Queuing cf49 for ${mId}`);
    tasksToRun.push(callCF("cf49", { materialId: mId }).catch(() => { }));
    updatePayload.apcfTransportCF_done = true;
  }

  // Check conditions for cf15
  if (data.apcfMPCF_done !== true) {
    logger.info(`[cf24] Queuing cf15 for ${mId}`);
    tasksToRun.push(callCF("cf15", { materialId: mId }).catch(() => { }));
    updatePayload.apcfMPCF_done = true;
  }

  // Execute all queued cloud function calls in parallel
  if (tasksToRun.length > 0) {
    await Promise.all(tasksToRun);
    logger.info(`[cf24] Completed ${tasksToRun.length} parallel task(s).`);
  }

  // Commit all flag updates to Firestore in a single operation
  if (Object.keys(updatePayload).length > 0) {
    await mRef.update(updatePayload);
    logger.info(`[cf24] Committed flag updates to Firestore.`);
  }

  /* HotSpot Calculations */
  // --- New logic to calculate percentage of parent CF ---
  data = (await mRef.get()).data() || {}; // Re-fetch the latest data
  logger.info(`[cf24] Calculating percentage of parent CF for material ${mId}.`);

  if (data.parent_material) {
    // {If this is true}: The material has a direct parent_material.
    const pmRef = data.parent_material;
    const pmSnap = await pmRef.get();
    if (pmSnap.exists) {
      const pmData = pmSnap.data() || {};
      const estimatedCf = data.estimated_cf;
      const parentCfFull = pmData.cf_full;

      if (typeof estimatedCf === 'number' && typeof parentCfFull === 'number' && parentCfFull !== 0) {
        const popCF = (estimatedCf / parentCfFull) * 100;
        await mRef.update({ percentage_of_p_cf: popCF });
        logger.info(`[cf24] Set percentage_of_p_cf to ${popCF}% based on parent material ${pmRef.id}.`);
      } else {
        logger.warn(`[cf24] Could not calculate percentage for material ${mId}. estimated_cf: ${estimatedCf}, parent cf_full: ${parentCfFull}.`);
      }
    } else {
      logger.warn(`[cf24] Parent material document ${pmRef.id} not found.`);
    }

  } else if (data.linked_product) {
    // {If this is false}: The material is a tier 1 component, linked to a product.
    const pDocRef = data.linked_product;
    const pDocSnap = await pDocRef.get();
    if (pDocSnap.exists) {
      const pDocData = pDocSnap.data() || {};
      const estimatedCf = data.estimated_cf;
      const productCfFull = pDocData.cf_full;

      if (typeof estimatedCf === 'number' && typeof productCfFull === 'number' && productCfFull !== 0) {
        const pop2CF = (estimatedCf / productCfFull) * 100;
        await mRef.update({ percentage_of_p_cf: pop2CF });
        logger.info(`[cf24] Set percentage_of_p_cf to ${pop2CF}% based on linked product ${pDocRef.id}.`);
      } else {
        logger.warn(`[cf24] Could not calculate percentage for material ${mId}. estimated_cf: ${estimatedCf}, product cf_full: ${productCfFull}.`);
      }
    } else {
      logger.warn(`[cf24] Linked product document ${pDocRef.id} not found.`);
    }
  } else {
    logger.info(`[cf24] No parent_material or linked_product found for material ${mId}. Skipping percentage calculation.`);
  }

  await mRef.update({
    completed_cf: true,
    apcfMaterials_done: true,
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  });

  res.json({ status: "ok", materialId: mId });
});