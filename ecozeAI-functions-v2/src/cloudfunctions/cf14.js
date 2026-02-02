//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf14 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  try {
    // --- MODIFIED: Parse both productName and the new tuId argument ---
    const productName = req.method === "POST" ? req.body?.product_name : req.query.product_name;
    const tuId = req.method === "POST" ? req.body?.tuId : req.query.tuId;

    // --- MODIFIED: Validate both arguments ---
    if (!productName || !productName.trim() || !tuId || !tuId.trim()) {
      res.status(400).json({ error: "product_name and tuId are required" });
      return;
    }

    // 1. Trigger the main cf11 function in the background.
    logger.info(`[cf14] Triggering cf11 for product: "${productName}"`);
    callCF("cf11", {
      product_name: productName,
    }).catch(err => {
      // Log errors from the background task but don't stop the monitor.
      logger.error(`[cf14] Background call to cf11 failed:`, err);
    });

    // 6. Loop until the product document is created by cf11.
    let pRef = null;
    const searchTimeout = Date.now() + 180000; // 3-minute timeout to find the doc.
    logger.info(`[cf14] Waiting for product document to be created...`);

    while (Date.now() < searchTimeout) {
      const snap = await db.collection("products_new").where("name", "==", productName).limit(1).get();
      if (!snap.empty) {
        pRef = snap.docs[0].ref;
        logger.info(`[cf14] Found product document: ${pRef.id}`);
        break;
      }
      await sleep(5000); // Wait 5 seconds before checking again.
    }

    if (!pRef) {
      const msg = "..."
      logger.error(`[cf14] ${msg}`);
      res.status(408).json({ error: msg });
      return;
    }

    // --- NEW: Update the newly found document with the tuId ---
    await pRef.update({ tu_id: tuId });
    logger.info(`[cf14] Set tu_id to "${tuId}" for product ${pRef.id}`);


    // --- Main Monitoring Loop ---
    logger.info(`[cf14] Starting monitoring loop for product ${pRef.id}.`);
    while (true) {
      const pSnap = await pRef.get();
      const pData = pSnap.data() || {};

      // 7. Check if the BoM generation and initial analysis are complete.
      const initial2Done = pData.apcfInitial2_done === true;

      let nmTotal = 0, amfTotal = 0, asaTotal = 0, atcfTotal = 0, asfTotal = 0, ampcfTotal = 0;
      let allSubJobsDone = false;

      // 8. If the initial step is done, we can check the status of all materials.
      if (initial2Done) {
        // 9. Fetch progress counts for all materials linked to the product.
        const baseQuery = db.collection("materials").where("linked_product", "==", pRef);
        const [
          nmSnap, amfSnap, asaSnap,
          atcfSnap, asfSnap, ampcfSnap
        ] = await Promise.all([
          baseQuery.count().get(),
          baseQuery.where("apcfMassFinder_done", "==", true).count().get(),
          baseQuery.where("apcfSupplierAddress_done", "==", true).count().get(),
          baseQuery.where("apcfTransportCF_done", "==", true).count().get(),
          baseQuery.where("apcfSupplierFinder_done", "==", true).count().get(),
          baseQuery.where("apcfMPCF_done", "==", true).count().get()
        ]);

        nmTotal = nmSnap.data().count;
        amfTotal = amfSnap.data().count;
        asaTotal = asaSnap.data().count;
        atcfTotal = atcfSnap.data().count;
        asfTotal = asfSnap.data().count;
        ampcfTotal = ampcfSnap.data().count;

        // 11. Check if the loop can end.
        // This happens if no materials were created, or if all materials are fully processed.
        if (nmTotal === 0) {
          allSubJobsDone = true;
        } else if (amfTotal === nmTotal && asaTotal === nmTotal && atcfTotal === nmTotal && asfTotal === nmTotal && ampcfTotal === nmTotal) {
          allSubJobsDone = true;
        }
      }

      // 10. Print the progress to logs.
      logger.info("\n=======================================\n");
      logger.info(`Total Materials: ${nmTotal}`);
      logger.info(`\nSupplier: ${asfTotal} / ${nmTotal}`);
      logger.info(`Supplier Address: ${asaTotal} / ${nmTotal}`);
      logger.info(`Mass: ${amfTotal} / ${nmTotal}`);
      logger.info(`TransportCF: ${atcfTotal} / ${nmTotal}`);
      logger.info(`MPCF: ${ampcfTotal} / ${nmTotal}`);
      logger.info("\n=======================================\n");

      if (allSubJobsDone) {
        logger.info(`[cf14] All materials for product ${pRef.id} are processed. Ending loop.`);
        break;
      }

      await sleep(5000); // Wait 5 seconds before the next check.
    }

    // 12. End the cloud function.
    res.json("Done");

  } catch (err) {
    logger.error("[cf14] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});