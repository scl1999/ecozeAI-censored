//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf40 = onRequest({
  region: REGION,
  timeoutSeconds: 60, // A short timeout is sufficient
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  try {
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || "";
    if (!productId) {
      res.status(400).json({ error: "productId is required." });
      return;
    }

    const pRef = db.collection("products_new").doc(productId);
    const pSnap = await pRef.get();

    if (!pSnap.exists) {
      res.status(404).json({ error: `Product ${productId} not found.` });
      return;
    }
    const pData = pSnap.data() || {};
    const currentTier = pData.current_tier;

    if (currentTier === undefined || currentTier === null) {
      logger.warn(`[cf40] Product ${productId} has no current_tier set. Ending.`);
      res.json({ status: "skipped", reason: "Product has no current_tier." });
      return;
    }

    // 1. Find all materials for the product (ignoring tier).
    const baseQuery = db.collection("materials")
      .where("linked_product", "==", pRef);

    const allMaterialsSnap = await baseQuery.select("apcfMaterials_done", "updatedAt").get();

    if (allMaterialsSnap.empty) {
      logger.info(`[cf40] No materials found for product ${productId}. Scheduling re-check.`);
      await scheduleNextCheck(productId);
      res.json({ status: "pending", message: "No materials found. Re-checking in 5 minutes." });
      return;
    }

    // 2. Filter for incomplete materials
    const incompleteMaterials = allMaterialsSnap.docs.filter(doc => doc.data().apcfMaterials_done !== true);
    const nmDone = incompleteMaterials.length;

    // {If nmDone > 0}
    if (nmDone > 0) {
      logger.info(`[cf40] Product ${productId} has ${nmDone} incomplete materials.`);

      logger.info(`[cf40] Scheduling re-check.`);
      await scheduleNextCheck(productId);
      res.json({ status: "pending", message: `${nmDone} materials are still processing. Re-checking in 5 minutes.` });
      return;
    }

    // {If nmDone = 0}
    logger.info(`[cf40] All ${allMaterialsSnap.size} materials for product ${productId} are complete.`);
    // 3. Set the product status to "Done" and clear the scheduled flag.
    await pRef.update({
      status: "Done",
      status_check_scheduled: false
    });
    logger.info(`[cf40] Successfully set status to "Done" for product ${productId}.`);
    // 4. End the cloudfunction.
    res.json({ status: "complete", message: "All materials are processed. Product status set to Done." });

  } catch (err) {
    logger.error("[cf40] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});