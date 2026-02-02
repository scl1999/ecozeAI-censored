//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf47 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM_SUPPLIER_FINDER_DR,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf47] Invoked");

  try {
    const materialId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || null;

    if (!materialId) {
      res.status(400).json({ error: "Missing materialId" });
      return;
    }

    const mRef = db.collection("materials").doc(materialId);
    const mSnap = await mRef.get();

    if (!mSnap.exists) {
      res.status(404).json({ error: `Material ${materialId} not found` });
      return;
    }

    logger.info(`[cf47] Resetting supplier fields for material ${materialId}...`);

    // 1. Reset Fields
    await mRef.update({
      supplier_name: admin.firestore.FieldValue.delete(),
      manufacturer_name: admin.firestore.FieldValue.delete(),
      other_known_suppliers: admin.firestore.FieldValue.delete(),
      supplier_finder_fcr: admin.firestore.FieldValue.delete(),
      supplier_finder_fcr_reasoning: admin.firestore.FieldValue.delete(),
      supplier_finder_retries: admin.firestore.FieldValue.delete(),
      supplier_confidence: admin.firestore.FieldValue.delete(),
      manufacturer_confidence: admin.firestore.FieldValue.delete(),
      supplier_probability_percentage: admin.firestore.FieldValue.delete(),
      manufacturer_probability_percentage: admin.firestore.FieldValue.delete(),
      supplier_estimated: admin.firestore.FieldValue.delete(),
      other_potential_suppliers: admin.firestore.FieldValue.delete(),
      supplier_evidence_rating: admin.firestore.FieldValue.delete(),
      other_suppliers: admin.firestore.FieldValue.delete(),
      apcfSupplierFinder_done: false,
      // Also potentially reset status if it was stuck
      status: "In Progress (Retry)"
    });

    // 1b. Rename existing reasoning logs
    const reasoningRef = mRef.collection("m_reasoning");
    const reasoningSnap = await reasoningRef.get();

    if (!reasoningSnap.empty) {
      const batch = db.batch();
      let updateCount = 0;

      reasoningSnap.forEach(doc => {
        const data = doc.data();
        if (data.cloudfunction && typeof data.cloudfunction === 'string' && data.cloudfunction.startsWith("cf43")) {
          // Check if it already has _initial to avoid double appending if run multiple times
          if (!data.cloudfunction.endsWith("_initial")) {
            batch.update(doc.ref, {
              cloudfunction: data.cloudfunction + "_initial"
            });
            updateCount++;
          }
        }
      });

      if (updateCount > 0) {
        await batch.commit();
        logger.info(`[cf47] Renamed ${updateCount} reasoning logs to *_initial.`);
      }
    }

    logger.info(`[cf47] Fields reset. Triggering cf46...`);

    // 2. Trigger cf46 and wait
    await callCF("cf46", { materialId: materialId });

    logger.info(`[cf47] cf46 finished.`);

    // 3. Set Status to Done
    await mRef.update({
      status: "Done"
    });

    res.json("Done");

  } catch (err) {
    logger.error("[cf47] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});