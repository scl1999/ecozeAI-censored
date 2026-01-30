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

exports.cf6 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf6] Invoked");

  let pDocRef; // Define here to be accessible in catch block

  try {
    // 1. Argument Parsing and Doc Fetching
    const { productNames, eDocId } = req.body; // Changed from productName to productNames
    if (!Array.isArray(productNames) || productNames.length === 0 || !eDocId) {
      res.status(400).json({ error: "productNames (as a non-empty array) and eDocId are required." });
      return;
    }
    logger.info(`[cf6] Invoked for eDoc ${eDocId} with ${productNames.length} new products.`);

    const eDocRef = db.collection("eaief_inputs").doc(eDocId);
    const eDocSnap = await eDocRef.get();
    if (!eDocSnap.exists) {
      res.status(404).json({ error: `eaief_inputs document ${eDocId} not found.` });
      return;
    }
    const eDocData = eDocSnap.data() || {};

    pDocRef = eDocData.product; // Assign to the outer scope variable
    if (!pDocRef) {
      res.status(404).json({ error: `Original product not linked in eDoc ${eDocId}.` });
      return;
    }

    const pSnap = await pDocRef.get();
    if (!pSnap.exists) {
      res.status(404).json({ error: `Original product ${pDocRef.id} not found.` });
      return;
    }
    const pData = pSnap.data() || {};

    // Set original product status to "In-Progress"
    await pDocRef.update({ status: "In-Progress" });
    logger.info(`[cf6] Set original product ${pDocRef.id} status to In-Progress.`);

    // 2. Create and Process the New Product Documents in a loop
    const includePackagingValue = pData.includePackaging === true;
    const shouldCalculateOtherMetrics = eDocData.otherMetrics === true;

    const newProductIds = [];
    const allDocumentsForDatastore = [];
    const creationBatch = db.batch();

    for (const productName of productNames) {
      const newProductRef = db.collection("products_new").doc();
      const newProductPayload = {
        name: productName,
        official_cf_available: false,
        ef_name: eDocData.productName_input,
        ef_pn: true,
        eai_ef_inputs: [eDocData.productName_input],
        eai_ef_docs: [eDocRef],
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        includePackaging: includePackagingValue,
      };

      creationBatch.set(newProductRef, newProductPayload);
      newProductIds.push(newProductRef.id);
      allDocumentsForDatastore.push({
        id: newProductRef.id,
        structData: { name: productName, ef_pn: true },
      });
    }

    await creationBatch.commit();
    logger.info(`[cf6] Created ${newProductIds.length} new product documents.`);

    // --- Start Processing Pipeline for all new products in parallel ---
    logger.info(`[cf6] Triggering cf37 for ${newProductIds.length} new products...`);
    const sdcfReviewFactories = newProductIds.map(id => () => callCF("cf37", { productId: id }));
    await runPromisesInParallelWithRetry(sdcfReviewFactories);
    logger.info(`[cf6] Finished cf37 for all new products.`);

    if (shouldCalculateOtherMetrics) {
      logger.info(`[cf6] Triggering cf31 for ${newProductIds.length} new products...`);
      const otherMetricsFactories = newProductIds.map(id => () => callCF("cf31", { productId: id }));
      await runPromisesInParallelWithRetry(otherMetricsFactories);
      logger.info(`[cf6] Finished cf31 for all new products.`);
    }

    logger.info(`[cf6] Triggering cf11 for ${newProductIds.length} new products...`);
    const initialFactories = newProductIds.map(id => {
      return () => {
        const initialPayload = { productId: id };
        if (shouldCalculateOtherMetrics) {
          initialPayload.otherMetrics = true;
        }
        return callCF("cf11", initialPayload);
      };
    });
    await runPromisesInParallelWithRetry(initialFactories);
    logger.info(`[cf6] Finished cf11 for all new products.`);

    // --- Poll for completion of all new products ---
    const MAX_POLL_MINUTES = 55;
    const POLLING_INTERVAL_MS = 30000;
    const startTime = Date.now();
    logger.info(`[cf6] Polling for completion of ${newProductIds.length} new products...`);

    while (Date.now() - startTime < MAX_POLL_MINUTES * 60 * 1000) {
      const chunks = [];
      for (let i = 0; i < newProductIds.length; i += 30) {
        chunks.push(newProductIds.slice(i, i + 30));
      }
      const chunkPromises = chunks.map(chunk => db.collection("products_new").where(admin.firestore.FieldPath.documentId(), 'in', chunk).get());
      const allSnapshots = await Promise.all(chunkPromises);
      const allDocs = allSnapshots.flatMap(snapshot => snapshot.docs);
      const completedCount = allDocs.filter(doc => doc.data().apcfInitial_done === true).length;

      logger.info(`[cf6] Polling: ${completedCount}/${newProductIds.length} done.`);
      if (completedCount === newProductIds.length) {
        logger.info(`[cf6] All new products finished processing.`);
        break;
      }
      await sleep(POLLING_INTERVAL_MS);
    }
    if (Date.now() - startTime >= MAX_POLL_MINUTES * 60 * 1000) {
      logger.warn(`[cf6] Polling timed out for new products. Proceeding with recalculation anyway.`);
    }

    // --- Add new products to Vertex AI Search Datastore ---
    if (allDocumentsForDatastore.length > 0) {
      logger.info(`[cf6] Adding ${allDocumentsForDatastore.length} docs to Vertex AI Search.`);
      const datastorePath = 'projects/projectId/locations/global/collections/default_collection/dataStores/ecoze-ai-products-datastore_1755024362755';
      try {
        const [operation] = await discoveryEngineClient.importDocuments({
          parent: `${datastorePath}/branches/0`,
          inlineSource: { documents: allDocumentsForDatastore },
        });
        await operation.promise();
        logger.info(`[cf6] Datastore import completed.`);
      } catch (err) {
        logger.error(`[cf6] Failed to import new documents:`, err);
      }
    }
    // --- End Processing Pipeline ---

    // 3. Recalculate Averages for the *Entire* Sample Set
    logger.info(`[cf6] Recalculating averages for the entire sample set in eDoc ${eDocId}.`);

    let pmDocsSnap = await db.collection('products_new')
      .where('eai_ef_docs', 'array-contains', eDocRef)
      .get();
    logger.info(`[cf6] Recalculating averages. Found ${pmDocsSnap.size} total products linked to ${eDocId}.`);

    // --- Run Deletion/Filtering Logic ---
    if (!pmDocsSnap.empty) {
      logger.info(`[cf6] Filtering ${pmDocsSnap.size} products for data quality...`);
      const checks = pmDocsSnap.docs.map(async doc => {
        if (doc.id === pDocRef.id) return null; // Don't delete the original product

        const data = doc.data();
        const hasStandards = Array.isArray(data.sdcf_standards) && data.sdcf_standards.length > 0;
        const sdcfDataSnap = await doc.ref.collection('pn_data')
          .where('type', '==', 'sdCF').limit(1).get();
        const hasSdcfData = !sdcfDataSnap.empty;

        if (!hasStandards || !hasSdcfData) {
          logger.info(`[cf6] Marking product ${doc.id} for deletion (missing SDCF data).`);
          return doc.ref;
        }
        return null;
      });

      const results = await Promise.all(checks);
      const docsToDeleteRefs = results.filter(ref => ref !== null);

      if (docsToDeleteRefs.length > 0) {
        const deleteBatch = db.batch();
        docsToDeleteRefs.forEach(ref => deleteBatch.delete(ref));
        await deleteBatch.commit();
        logger.info(`[cf6] Deleted ${docsToDeleteRefs.length} products with insufficient data.`);

        pmDocsSnap = await db.collection('products_new')
          .where('eai_ef_docs', 'array-contains', eDocRef)
          .get();
      }
    }
    // --- End Deletion Logic ---

    logger.info(`[cf6] Found ${pmDocsSnap.size} products linked to ${eDocId} after filtering.`);

    let averageCF;
    let finalCf;
    const conversion = eDocData.conversion || 1;

    if (pmDocsSnap.empty) {
      logger.warn(`[cf6] No matching products left after filtering. Averages will be 0.`);
      averageCF = 0;
      finalCf = 0;

      const updatePayload = {
        cf_average: finalCf,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };
      if (shouldCalculateOtherMetrics) {
        updatePayload.ap_total_average = 0;
        updatePayload.ep_total_average = 0;
        updatePayload.adpe_total_average = 0;
        updatePayload.gwp_f_total_average = 0;
        updatePayload.gwp_b_total_average = 0;
        updatePayload.gwp_l_total_average = 0;
      }
      await eDocRef.update(updatePayload);

    } else {
      // --- Start Averaging Logic ---
      const metrics = {
        cf: [], ap: [], ep: [], adpe: [],
        gwp_f_percentages: [], gwp_b_percentages: [], gwp_l_percentages: []
      };

      pmDocsSnap.docs.forEach(doc => {
        const data = doc.data();
        if (typeof data.supplier_cf === 'number' && isFinite(data.supplier_cf)) {
          metrics.cf.push(data.supplier_cf);
        }

        if (shouldCalculateOtherMetrics) {
          if (typeof data.ap_total === 'number' && isFinite(data.ap_total)) metrics.ap.push(data.ap_total);
          if (typeof data.ep_total === 'number' && isFinite(data.ep_total)) metrics.ep.push(data.ep_total);
          if (typeof data.adpe_total === 'number' && isFinite(data.adpe_total)) metrics.adpe.push(data.adpe_total);

          const supplierCf = data.supplier_cf;
          if (typeof supplierCf === 'number' && isFinite(supplierCf) && supplierCf > 0) {
            if (typeof data.gwp_f_total === 'number' && isFinite(data.gwp_f_total)) {
              metrics.gwp_f_percentages.push(data.gwp_f_total / supplierCf);
            }
            if (typeof data.gwp_b_total === 'number' && isFinite(data.gwp_b_total)) {
              metrics.gwp_b_percentages.push(data.gwp_b_total / supplierCf);
            }
            if (typeof data.gwp_l_total === 'number' && isFinite(data.gwp_l_total)) {
              metrics.gwp_l_percentages.push(data.gwp_l_total / supplierCf);
            }
          }
        }
      });

      averageCF = calculateAverage(metrics.cf, true);
      finalCf = averageCF * conversion;

      const eDocUpdatePayload = {
        cf_average: finalCf,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };

      if (shouldCalculateOtherMetrics) {
        eDocUpdatePayload.ap_total_average = calculateAverage(metrics.ap, false) * conversion;
        eDocUpdatePayload.ep_total_average = calculateAverage(metrics.ep, false) * conversion;
        eDocUpdatePayload.adpe_total_average = calculateAverage(metrics.adpe, false) * conversion;

        const avg_gwp_f_percent = calculateAverage(metrics.gwp_f_percentages, false);
        const avg_gwp_b_percent = calculateAverage(metrics.gwp_b_percentages, false);
        const avg_gwp_l_percent = calculateAverage(metrics.gwp_l_percentages, false);

        eDocUpdatePayload.gwp_f_total_average = avg_gwp_f_percent * finalCf;
        eDocUpdatePayload.gwp_b_total_average = avg_gwp_b_percent * finalCf;
        eDocUpdatePayload.gwp_l_total_average = avg_gwp_l_percent * finalCf;
      }

      await eDocRef.update(eDocUpdatePayload);
      logger.info(`[cf6] Updated ${eDocId} with new calculated averages.`);
      // --- End Averaging Logic ---
    }

    const pDocUpdatePayload = {
      cf_full: finalCf,
      cf_full_refined: finalCf,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    await pDocRef.update(pDocUpdatePayload);
    logger.info(`[cf6] Updated original product ${pDocRef.id}: set cf_full_original to ${pDocUpdatePayload.cf_full_original} and new cf_full to ${finalCf}.`);

    // --- Aggregate cost from new EF products to original product ---
    let totalNewCost = 0;
    const newProdRefs = newProductIds.map(id => db.collection("products_new").doc(id));
    const newProdSnaps = await db.getAll(...newProdRefs);

    for (const docSnap of newProdSnaps) {
      if (docSnap.exists) {
        totalNewCost += docSnap.data().totalCost || 0;
      }
    }

    if (totalNewCost > 0) {
      await pDocRef.update({
        totalCost: admin.firestore.FieldValue.increment(totalNewCost)
      });
      logger.info(`[cf6] Incremented original product ${pDocRef.id} totalCost by ${totalNewCost} from ${newProdSnaps.length} new products.`);
    }

    // 4. Set Original Product Status to "Done"
    await pDocRef.update({ status: "Done" });
    logger.info(`[cf6] Set original product ${pDocRef.id} status back to Done.`);

    res.json("Done");

  } catch (err) {
    logger.error("[cf6] Uncaught error:", err);
    if (pDocRef) { // Use the variable from the outer scope
      try {
        await pDocRef.update({ status: "Done" });
        logger.warn(`[cf6] Set original product ${pDocRef.id} status to Done due to error.`);
      } catch (e) {
        logger.error(`[cf6] CRITICAL: Failed to set original product status to Done during error handling:`, e);
      }
    }
    res.status(500).json({ error: String(err) });
  }
});