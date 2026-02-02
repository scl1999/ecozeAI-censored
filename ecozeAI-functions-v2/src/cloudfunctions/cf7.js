//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf7 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf7] Invoked");

  let pDocRef; // Define here to be accessible in catch block

  try {
    // 1. Argument Parsing and Doc Fetching
    const { eDocId, productIds, originalProductName } = req.body;

    if (!eDocId) {
      return res.status(400).json({ error: "eDocId is required." });
    }
    if (!Array.isArray(productIds) || productIds.length === 0) {
      return res.status(400).json({ error: "productIds must be a non-empty array." });
    }
    if (!originalProductName) {
      return res.status(400).json({ error: "originalProductName is required." });
    }
    logger.info(`[cf7] Removing ${productIds.length} product(s) from eDoc ${eDocId}.`);

    const eDocRef = db.collection("eaief_inputs").doc(eDocId);
    const eDocSnap = await eDocRef.get();
    if (!eDocSnap.exists) {
      return res.status(404).json({ error: `eaief_inputs document ${eDocId} not found.` });
    }
    const eDocData = eDocSnap.data() || {};

    pDocRef = eDocData.product; // Assign to the outer scope variable
    if (!pDocRef) {
      return res.status(404).json({ error: `Original product not linked in eDoc ${eDocId}.` });
    }

    // Set original product status to "In-Progress"
    await pDocRef.update({ status: "In-Progress" });
    logger.info(`[cf7] Set original product ${pDocRef.id} status to In-Progress.`);

    // 1. Remove references from the specified products
    const batch = db.batch();
    for (const productId of productIds) {
      const productRef = db.collection("products_new").doc(productId);
      batch.update(productRef, {
        eai_ef_docs: admin.firestore.FieldValue.arrayRemove(eDocRef),
        eai_ef_inputs: admin.firestore.FieldValue.arrayRemove(originalProductName)
      });
    }
    await batch.commit();
    logger.info(`[cf7] Removed eDoc references from ${productIds.length} products.`);

    // 2. Recalculate Averages
    const shouldCalculateOtherMetrics = eDocData.otherMetrics === true;
    const conversion = eDocData.conversion || 1;

    // Fetch all products *still* linked to the eDoc
    const pmDocsSnap = await db.collection('products_new')
      .where('eai_ef_docs', 'array-contains', eDocRef)
      .get();
    logger.info(`[cf7] Recalculating averages based on ${pmDocsSnap.size} remaining products.`);

    let averageCF;
    let finalCf;

    if (pmDocsSnap.empty) {
      logger.warn(`[cf7] No products left in the sample. Averages will be 0.`);
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
      logger.info(`[cf7] Updated ${eDocId} with new recalculated averages.`);
      // --- End Averaging Logic ---
    }

    // Update the original product
    const pSnap = await pDocRef.get();
    const pData = pSnap.data() || {};
    const currentCfFull = pData.cf_full || 0;

    const pDocUpdatePayload = {
      cf_full_refined: finalCf,
      cf_full: finalCf,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    await pDocRef.update(pDocUpdatePayload);
    logger.info(`[cf7] Updated original product ${pDocRef.id}: set cf_full_original to ${currentCfFull} and new cf_full to ${finalCf}.`);

    // 3. Set Original Product Status to "Done"
    await pDocRef.update({ status: "Done" });
    logger.info(`[cf7] Set original product ${pDocRef.id} status back to Done.`);

    res.json("Done");

  } catch (err) {
    logger.error("[cf7] Uncaught error:", err);
    if (pDocRef) {
      await pDocRef.update({ status: "Done" });
      logger.warn(`[cf7] Set original product ${pDocRef.id} status to Done due to error.`);
    }
    res.status(500).json({ error: String(err) });
  }
});