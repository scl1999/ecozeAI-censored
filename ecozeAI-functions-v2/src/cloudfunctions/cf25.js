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

exports.cf25 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {

  /* ───────── Validate input ───────── */
  const mId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || "";
  if (!mId.trim()) {
    res.status(400).json({ error: "materialId required" });
    return;
  }

  try {
    /* ───────── Load Material & state ───────── */
    const mRef = db.collection("materials").doc(mId);
    let mSnap = await mRef.get();
    if (!mSnap.exists) {
      res.status(404).json({ error: `material ${mId} not found` });
      return;
    }
    let mData = mSnap.data() || {};

    // Set start time
    await mRef.update({ apcfMaterials2StartTime: admin.firestore.FieldValue.serverTimestamp() });

    const productChain = mData.product_chain || '(unknown chain)';

    // Conditionally add the packaging flag to the material name for the prompt
    let materialNameForPrompt = "...".name || "(unknown material)";
    if (mData.includePackaging === true) {
      materialNameForPrompt += " (Include Packaging)";
    }

    let initialPrompt = "...";
    if (mData.mass && mData.mass_unit) {
      initialPrompt += `Material Weight/Mass: ${mData.mass} ${mData.mass_unit}\n`;
    }
    initialPrompt +=
      `Supplier: ${mData.supplier_name}\n` +
      `Description: ${mData.description || 'No description provided.'}\n` +
      `Supply-chain: ${productChain}`;

    // --- START: New conditional logic to find and add peer materials ---
    let peerMaterialsSnap;

    // CASE 1: mDoc is a Tier N material (it has a parent_material)
    if (mData.parent_material) {
      logger.info(`[cf25] Tier N material detected. Searching for peers with parent: ${mData.parent_material.id}`);
      peerMaterialsSnap = await db.collection("materials")
        .where("parent_material", "==", mData.parent_material)
        .get();
    }
    // CASE 2: mDoc is a Tier 1 material (parent_material is unset)
    else if (mData.linked_product) {
      logger.info(`[cf25] Tier 1 material detected. Searching for peers linked to product: ${mData.linked_product.id}`);
      peerMaterialsSnap = await db.collection("materials")
        .where("tier", "==", 1)
        .where("linked_product", "==", mData.linked_product)
        .get();
    }

    // If the query ran and found documents, format them for the prompt
    if (peerMaterialsSnap && !peerMaterialsSnap.empty) {
      const peerLines = [];
      let i = 1;
      for (const peerDoc of peerMaterialsSnap.docs) {
        if (peerDoc.id === mId) {
          continue;
        }
        const peerData = peerDoc.data() || {};
        const massString = (peerData.mass && peerData.mass_unit) ? `${peerData.mass} ${peerData.mass_unit}` : 'Unknown';
        peerLines.push(
          `material_${i}_name: ${peerData.name || 'Unknown'}`,
          `material_${i}_mass/weight: ${massString}`,
          `material_${i}_supplier_name: ${peerData.supplier_name || 'Unknown'}`,
          `material_${i}_description: ${peerData.description || 'No description provided.'}`
        );
        i++;
      }

      if (peerLines.length > 0) {
        initialPrompt += "\n\nPeer Materials:\n" + peerLines.join('\n');
      }
    }

    let existingHistory;
    try {
      existingHistory = JSON.parse(mData.z_ai_history || "[]");
    } catch {
      existingHistory = [];
    }

    const collectedUrls = new Set();
    const modelUsed = 'aiModel';
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

    const foundMaterials = new Set();
    const { finalAnswer, history, cost, totalTokens, searchQueries, model, rawConversation, logForReasoning } = await runChatLoop({
      model: modelUsed,
      generationConfig: vGenerationConfig,
      initialPrompt,
      followUpPrompt: GO_AGAIN_PROMPT,
      maxFollowUps: FOLLOWUP_LIMIT,
      existingHistory,
      collectedUrls,
      onTurnComplete: async (history, latestAnswer) => {
        const materials = parseBom(latestAnswer);
        materials.forEach(m => {
          if (m.mat) foundMaterials.add(m.mat.trim().toLowerCase());
        });
        await mRef.update({ apcfIMaterialsNum: foundMaterials.size });
        logger.info(`[cf25] Incremental material count: ${foundMaterials.size}`);
      }
    });

    await logAITransaction({
      cfName: 'apcfMaterials2',
      productId: mData.linked_product ? mData.linked_product.id : null,
      materialId: mId,
      cost,
      totalTokens,
      searchQueries: searchQueries,
      modelUsed: model
    });

    await logAIReasoning({
      sys: vGenerationConfig.systemInstruction.parts[0].text,
      user: initialPrompt,
      thoughts: logForReasoning,
      answer: finalAnswer,
      cloudfunction: 'apcfMaterials2',
      materialId: mId,
      rawConversation: rawConversation,
    });

    /* ───────── Fact Checker & Merge Logic ───────── */
    logger.info(`[cf25] Running BOM Fact Checker...`);
    let checkedAnswer = "";
    try {
      checkedAnswer = await bomFactChecker(
        vGenerationConfig.systemInstruction.parts[0].text,
        initialPrompt,
        finalAnswer,
        Array.from(collectedUrls)
      );
    } catch (err) {
      logger.error(`[cf25] Fact checker failed. Using original answer.`, err);
    }

    const originalItems = parseBom(finalAnswer);
    const correctedItems = checkedAnswer ? parseBom(checkedAnswer) : [];
    const finalItems = [];

    // Map corrections by index for easy lookup
    const correctionMap = new Map();
    correctedItems.forEach(item => {
      if (item.index) correctionMap.set(item.index, item);
    });

    for (const original of originalItems) {
      const correction = correctionMap.get(original.index);
      if (correction) {
        // If correction says "N/A" (or similar), we skip this item (delete it)
        // Adjust logic based on prompt instruction: "Or 'N/A' if the PCMI doesnt exist"
        const isNA = (val) => val && typeof val === 'string' && val.toUpperCase() === "N/A";

        if (isNA(correction.mat) || isNA(correction.supp) || isNA(correction.desc) || isNA(correction.mass)) {
          logger.info(`[cf25] Item ${original.index} removed by fact checker.`);
          continue;
        }

        // Apply updates
        const updatedItem = { ...original };
        updatedItem.mat = correction.mat;
        updatedItem.supp = correction.supp;
        updatedItem.desc = correction.desc;
        // logic for mass if needed, but reusing original parse logic for mass object structure might be safer if correction string is differnt
        // helper regex parsed mass/unit. correction object has them.
        updatedItem.mass = correction.mass;
        updatedItem.unit = correction.unit;

        finalItems.push(updatedItem);
        logger.info(`[cf25] Item ${original.index} updated by fact checker.`);
      } else {
        // No correction found, keep original
        finalItems.push(original);
      }
    }


    /* ───────── Process final response and create child materials ───────── */
    let childMaterialsNewList = [];
    for (const p of finalItems) {
      const parentMassInfo = (typeof mData.mass === 'number' && mData.mass_unit)
        ? ` [Mass: ${mData.mass}${mData.mass_unit}]`
        : "";

      const parentInfoParts = [];
      if (mData.supplier_name) {
        parentInfoParts.push(`[Supplier Name: ${mData.supplier_name}]`);
      }

      if (mData.supplier_address && mData.supplier_address !== "Unknown") {
        parentInfoParts.push(`[Manufacturer / Supplier Address: ${mData.supplier_address}]`);
      } else if (mData.country_of_origin && mData.country_of_origin !== "Unknown") {
        if (mData.coo_estimated === true) {
          parentInfoParts.push(`[Estimated Country of Origin: ${mData.country_of_origin}]`);
        } else {
          parentInfoParts.push(`[Country of Origin: ${mData.country_of_origin}]`);
        }
      }
      const parentInfoString = parentInfoParts.length > 0 ? ` ${parentInfoParts.join(' ')}` : "";
      const newProductChain = `${mData.product_chain || ''}${parentInfoString}${parentMassInfo} -> ${p.mat}`;

      const parentPmChain = mData.pmChain || [];
      const newPmChain = [
        ...parentPmChain,
        {
          documentId: mRef.id,
          material_or_product: "Material",
          tier: (mData.tier || 1)
        }
      ];
      const childRef = await db.collection("materials").add({
        name: p.mat,
        supplier_name: p.supp,
        description: p.desc,
        linked_product: mData.linked_product || null,
        parent_material: mRef,
        tier: (mData.tier || 1) + 1,
        mass: p.mass ?? null,
        mass_unit: p.unit,
        estimated_cf: 0,
        total_cf: 0,
        transport_cf: 0,
        apcfIMaterialsNum: 0,
        product_chain: newProductChain,
        pmChain: newPmChain,
        apcfMassFinder_done: false,
        apcfSupplierFinder_done: p.supp.toLowerCase() !== 'unknown',
        apcfMPCF_done: false,
        apcfMFSF_done: false,
        apcfBOM_done: false,
        apcfMaterials_done: false,
        apcfSupplierFinderDR_started: false,
        apcfMassReview_done: false,
        apcfCFReview_done: false,
        software_or_service: false,
        apcfSupplierAddress_done: false,
        apcfTransportCF_done: false,
        apcfSupplierDisclosedCF_done: false,
        apcfSupplierDisclosedCF_done: false,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        child_materials_progress: [
          { cloudfunction: 'cf25', number_done: 0 },
          { cloudfunction: 'cf25', number_done: 0 },
          { cloudfunction: 'cf25', number_done: 0 },
          { cloudfunction: 'cf25', number_done: 0 },
          { cloudfunction: 'cf25', number_done: 0 }
        ]
      });
      logger.info(`[cf25] ➕ Spawned child material ${childRef.id} (“${p.mat}”) tier=${(mData.tier || 1) + 1}`);
      childMaterialsNewList.push(childRef.id);
    }

    if (collectedUrls.size) {
      await saveURLs({
        urls: Array.from(collectedUrls),
        materialId: mId,
        productId: mData.linked_product ? mData.linked_product.id : null,
        mBOMData: true,
        sys: vGenerationConfig.systemInstruction.parts[0].text,
        user: initialPrompt,
        thoughts: logForReasoning,
        answer: finalAnswer,
        cloudfunction: 'apcfMaterials2',
      });
    }

    // Update material count to reflect final list
    await mRef.update({ apcfIMaterialsNum: childMaterialsNewList.length });

    // Persist the final, complete history and archive it.
    await persistHistory({ docRef: mRef, history, loop: (mData.ai_loop || 0) + 1, wipeNow: true });

    // Trigger cf26 asynchronously
    const finalUrl = `https://${REGION}-${process.env.GCP_PROJECT_ID || 'projectId'}.cloudfunctions.net/cf26`;

    logger.info(`[cf25] Triggering cf26...`);

    // Fire and forget fetch
    fetch(finalUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ materialId: mId, childMaterialsList: childMaterialsNewList })
    }).catch(e => logger.warn("[cf25] Async trigger warning (expected if timeout)", e.message));

    res.json({ status: "ok", materialId: mId, triggered: "cf26" });

  } catch (err) {
    logger.error(`[cf25] Uncaught error for material ${mId}:`, err);
    res.status(500).json({ error: String(err) });
  }
});