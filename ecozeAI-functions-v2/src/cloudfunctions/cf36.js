//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf36 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf36] Invoked");
  try {
    // 1. Define the start date: 00:00AM UTC 17th November 2025
    // Note: Month is 0-indexed in JS Date (0=Jan, 10=Nov)
    const startDate = new Date(Date.UTC(2025, 10, 17, 0, 0, 0));
    const startTimestamp = admin.firestore.Timestamp.fromDate(startDate);

    logger.info(`[cf36] Querying products created after ${startDate.toISOString()}...`);

    // 2. Query products_new
    const snapshot = await db.collection("products_new")
      .where("createdAt", ">=", startTimestamp)
      .get();

    if (snapshot.empty) {
      logger.info("[cf36] No products found matching the date criteria.");
      res.json("Done - No products found.");
      return;
    }

    logger.info(`[cf36] Found ${snapshot.size} products created after the cutoff.Filtering for supplier_cf...`);

    // 3. Filter for supplier_cf (Double) not equal to 0
    const pDocs = [];
    snapshot.forEach(doc => {
      const data = doc.data();
      // Check if supplier_cf exists and is a number and not 0
      if (typeof data.supplier_cf === 'number' && data.supplier_cf !== 0) {
        pDocs.push(doc.id);
      }
    });

    logger.info(`[cf36] Identified ${pDocs.length} products with valid supplier_cf.`);

    if (pDocs.length === 0) {
      res.json("Done - No products matched supplier_cf criteria.");
      return;
    }

    // 4. Trigger cf38 for all pDocs
    logger.info(`[cf36] Triggering cf38 for ${pDocs.length} products...`);

    const factories = pDocs.map(id => {
      return () => callCF("cf38", { productId: id });
    });

    // Use existing helper for concurrent execution with retries
    await runPromisesInParallelWithRetry(factories);

    logger.info("[cf36] Finished triggering all cf38 calls.");

    // 5. End
    res.json(`Done - Triggered for ${pDocs.length} products.`);

  } catch (err) {
    logger.error("[cf36] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});