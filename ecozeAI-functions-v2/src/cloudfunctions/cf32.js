//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf32 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf32] Invoked");

  try {
    // --- 0. Argument Validation ---
    const { userId, collectionPN } = req.method === "POST" ? req.body : req.query;

    if (!userId || !collectionPN) {
      res.status(400).send("Error: Missing required arguments. 'userId' and 'collectionPN' are both required.");
      return;
    }

    // --- 1. Find all products in the specified collection ---
    const productsQuery = db.collection('products_new')
      .where('tu_id', '==', userId)
      .where('pn_collection', '==', collectionPN);

    const productsSnapshot = await productsQuery.get();

    if (productsSnapshot.empty) {
      logger.info(`[cf32] No products found for user '${userId}' in collection '${collectionPN}'. No action taken.`);
      res.status(200).send("Success: No products found to delete.");
      return;
    }

    logger.info(`[cf32] Found ${productsSnapshot.size} products to delete.`);

    // --- Loop through each product to delete it and its linked materials ---
    for (const pnDoc of productsSnapshot.docs) {
      logger.info(`[cf32] Processing product ${pnDoc.id}...`);

      // 2. Find all materials linked to this product
      const materialsQuery = db.collection('materials').where('linked_product', '==', pnDoc.ref);
      const materialsSnapshot = await materialsQuery.get();

      // 3. Delete all linked materials and their subcollections
      if (!materialsSnapshot.empty) {
        logger.info(`[cf32] Found ${materialsSnapshot.size} linked materials for product ${pnDoc.id}.`);
        for (const materialDoc of materialsSnapshot.docs) {
          logger.info(`[cf32] --> Deleting material ${materialDoc.id} and its subcollections.`);
          await deleteDocumentAndSubcollections(materialDoc.ref);
        }
      }

      // 4. Delete the product document itself and its subcollections
      logger.info(`[cf32] --> Deleting product ${pnDoc.id} and its subcollections.`);
      await deleteDocumentAndSubcollections(pnDoc.ref);
    }

    // --- 5. End the function ---
    logger.info(`[cf32] Successfully deleted collection '${collectionPN}' for user '${userId}'.`);
    res.status(200).send("Success");

  } catch (err) {
    logger.error("[cf32] Uncaught error during deletion process:", err);
    res.status(500).send("An internal error occurred during the delete operation.");
  }
});

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------

/****************************************************************************************
 * CBAM $$$
 ****************************************************************************************/