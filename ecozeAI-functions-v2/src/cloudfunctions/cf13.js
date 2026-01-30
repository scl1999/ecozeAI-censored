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

exports.cf13 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  try {
    const body = req.method === "POST" ? req.body : req.query;
    const productId = body.productId;
    const materialsNewList = body.materialsList || [];

    if (!productId) {
      res.status(400).json({ error: "productId required" });
      return;
    }

    const pRef = db.collection("products_new").doc(productId);

    logger.info(`[cf13] Started for product ${productId} with ${materialsNewList.length} materials.`);

    await verifyMaterialLinks(materialsNewList, pRef);

    await sleep(5000);

    // Step 3: Run initial finders in parallel for all new materials.
    logger.info(`[cf13] Running SupplierFinder and MassFinder concurrently for ${materialsNewList.length} new materials.`);

    // --- START: Conditional Supplier Finder Logic ---
    const drMaterialIds = new Set();
    try {
      const drSnapshot = await db.collection("materials")
        .where("linked_product", "==", pRef)
        .orderBy("createdAt", "asc")
        .limit(60)
        .get();

      drSnapshot.forEach(doc => drMaterialIds.add(doc.id));
      logger.info(`[cf13] Identified ${drMaterialIds.size} materials for Deep Research (First 60).`);
    } catch (err) {
      logger.error(`[cf13] Failed to fetch DR material list: ${err.message}. Defaulting to Standard.`);
    }
    // --- END: Conditional Supplier Finder Logic ---

    // FIX: Using Promise.allSettled to prevent one failure from crashing the entire batch
    const results = await Promise.allSettled(materialsNewList.map(async (mId) => {
      try {
        const mRef = db.collection("materials").doc(mId);
        let data = (await mRef.get()).data() || {};

        // Conditional Call
        if (drMaterialIds.has(mId)) {
          logger.info(`[cf13] Triggering cf46 (Deep Research) for ${mId}`);
          await callCF("cf43", { materialId: mId });
        } else {
          logger.info(`[cf13] Triggering cf43 (Standard) for ${mId}`);
          await callCF("cf43", { materialId: mId });
        }
        await mRef.update({ apcfSupplierFinder_done: true });
        data = (await mRef.get()).data() || {};
        if ((data.supplier_name || "Unknown") === "Unknown") {
          await mRef.update({ final_tier: true });
        }

        const isSoS = data.software_or_service === true;
        const isMassFinderDone = data.apcfMassFinder_done === true;

        if (!isSoS && !isMassFinderDone) {
          try {
            await callCF("cf22", { materialId: mId });
            // cf22 will set its own _done flag
          } catch {
            logger.warn(`[cf13] cf22 for ${mId} failed (ignored)`);
          }
        } else {
          logger.info(`[cf13] Skipping cf22 for ${mId}. SoS: ${isSoS}, Done: ${isMassFinderDone}`);
        }
      } catch (innerErr) {
        logger.error(`[cf13] Error processing material ${mId}:`, innerErr);
        // We swallow the error here so other materials continue
      }
    }));

    // Log results summary
    const failures = results.filter(r => r.status === 'rejected');
    if (failures.length > 0) {
      logger.warn(`[cf13] ${failures.length} materials failed during processing, but batch continued.`);
    }

    logger.info(`[cf13] Finished initial finders.`);
    await pRef.update({
      apcfMFSF_done: true
    });

    // Step 4 & 5: Trigger cf23 and wait.
    logger.info(`[cf13] Triggering cf23 for ${materialsNewList.length} materials.`);
    await callCF("cf23", { materialsNewList: materialsNewList, productId: productId });
    logger.info(`[cf13] cf23 finished.`);

    await pRef.update({
      apcfMassReview_done: true
    });

    // Step 6: Trigger cf24 concurrently for all new materials.
    logger.info(`[cf13] Triggering cf24 concurrently for ${materialsNewList.length} materials.`);

    const matResults = await Promise.allSettled(materialsNewList.map(mId =>
      callCF("cf24", { materialId: mId })
    ));

    const matFailures = matResults.filter(r => r.status === 'rejected');
    if (matFailures.length > 0) {
      logger.warn(`[cf13] ${matFailures.length} child cf24 calls failed.`);
    }

    logger.info(`[cf13] cf24 calls finished.`);

    await pRef.update({
      apcfMaterials_done: true
    });

    logger.info(`[cf13] Calling cf21 for product ${productId}`);
    await callCF("cf21", { productId: productId });
    logger.info(`[cf13] Completed cf21 for ${productId}.`);

    // Step 8: Trigger cf8 and wait.
    logger.info(`[cf13] Triggering cf8 for ${materialsNewList.length} materials.`);
    await callCF("cf8", { materialsNewList: materialsNewList, productId: productId });
    logger.info(`[cf13] cf8 finished.`);

    await pRef.update({
      apcfCFReview_done: true
    });

    // Note: History and URL saving moved to BOM function.

    /* ╔═══════════════ Post-loop clean-up ═══════════════╗ */
    logger.info("[cf13] Loop finished.");

    const matsSnap = await db.collection("materials").where("linked_product", "==", pRef).get();

    /* De-duplicate materials (same logic as before) */
    const groups = {};
    matsSnap.forEach(doc => {
      const key = (doc.get("name") || "").trim().toLowerCase();
      (groups[key] = groups[key] || []).push({
        id: doc.id,
        supplier: (doc.get("supplier_name") || "").trim(),
        ref: doc.ref,
        createdAt: doc.get("createdAt")
      });
    });

    const TEN_MINUTES_MS = 10 * 60 * 1000;

    for (const nameKey of Object.keys(groups)) {
      const nameGroup = groups[nameKey];
      if (nameGroup.length <= 1) continue;

      // Sort the group by creation time
      nameGroup.sort((a, b) => a.createdAt.toMillis() - b.createdAt.toMillis());

      let currentSubgroup = [];
      for (const material of nameGroup) {
        if (currentSubgroup.length === 0) {
          currentSubgroup.push(material);
          continue;
        }

        const firstTimestamp = currentSubgroup[0].createdAt.toMillis();
        const currentTimestamp = material.createdAt.toMillis();

        if (currentTimestamp - firstTimestamp <= TEN_MINUTES_MS) {
          // It's a duplicate within the time window, add it to the current subgroup
          currentSubgroup.push(material);
        } else {
          // Time window exceeded, process the completed subgroup
          if (currentSubgroup.length > 1) {
            const keeper = currentSubgroup[0];
            const toMerge = currentSubgroup.slice(1);
            const altSupp = toMerge.map(d => d.supplier).filter(Boolean);
            const batch = db.batch();

            if (altSupp.length) {
              batch.update(keeper.ref, {
                alternative_suppliers: admin.firestore.FieldValue.arrayUnion(...altSupp)
              });
            }
            toMerge.forEach(d => batch.delete(d.ref));
            await batch.commit();
            logger.info(`[cf13] De-duplicated "${nameKey}" (time-windowed) - kept ${keeper.id}`);
          }
          // Start a new subgroup with the current material
          currentSubgroup = [material];
        }
      }

      // Process the last remaining subgroup after the loop finishes
      if (currentSubgroup.length > 1) {
        const keeper = currentSubgroup[0];
        const toMerge = currentSubgroup.slice(1);
        const altSupp = toMerge.map(d => d.supplier).filter(Boolean);
        const batch = db.batch();

        if (altSupp.length) {
          batch.update(keeper.ref, {
            alternative_suppliers: admin.firestore.FieldValue.arrayUnion(...altSupp)
          });
        }
        toMerge.forEach(d => batch.delete(d.ref));
        await batch.commit();
        logger.info(`[cf13] De-duplicated "${nameKey}" (time-windowed) - kept ${keeper.id}`);
      }
    }

    // --- Aggregate Uncertainty & Finalize ---
    /*
    const uncertaintySnap = await pRef.collection("pn_uncertainty").get();
    let uSum = 0;
 
    if (!uncertaintySnap.empty) {
      uncertaintySnap.forEach(doc => {
        const uncertaintyValue = doc.data().co2e_uncertainty_kgco2e;
        if (typeof uncertaintyValue === 'number' && isFinite(uncertaintyValue)) {
          uSum += uncertaintyValue;
        }
      });
    }
    logger.info(`[cf13] Calculated total uncertainty for product ${pRef.id}: ${uSum}`);
    */

    // Combine final updates
    const finalUpdatePayload = {
      status: "Done",
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      apcfInitial2_done: true,
      apcfInitial2EndTime: admin.firestore.FieldValue.serverTimestamp(),
      //total_uncertainty: uSum,
    };

    // Conditionally aggregate other metrics
    const finalProductData = (await pRef.get()).data() || {};
    if (finalProductData.otherMetrics === true) {
      logger.info(`[cf13] otherMetrics flag is true for ${pRef.id}. Aggregating totals.`);
      const metricsSnap = await pRef.collection("pn_otherMetrics").get();

      const totals = { ap_total: 0, ep_total: 0, adpe_total: 0, gwp_f_total: 0, gwp_b_total: 0, gwp_l_total: 0 };
      const fieldsToSum = [
        { from: 'ap_value', to: 'ap_total' }, { from: 'ep_value', to: 'ep_total' },
        { from: 'adpe_value', to: 'adpe_total' }, { from: 'gwp_f_value', to: 'gwp_f_total' },
        { from: 'gwp_b_value', to: 'gwp_b_total' }, { from: 'gwp_l_value', to: 'gwp_l_total' },
      ];

      if (!metricsSnap.empty) {
        metricsSnap.forEach(doc => {
          const data = doc.data();
          fieldsToSum.forEach(field => {
            if (typeof data[field.from] === 'number' && isFinite(data[field.from])) {
              totals[field.to] += data[field.from];
            }
          });
        });
      }
      logger.info(`[cf13] Calculated totals for ${pRef.id}:`, totals);
      Object.assign(finalUpdatePayload, totals);
    }

    await pRef.update(finalUpdatePayload);

    res.json({ status: "ok", docId: productId });
  } catch (err) {
    logger.error("[cf13] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});