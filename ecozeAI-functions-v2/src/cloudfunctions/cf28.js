//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf28 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  try {
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || "";
    if (!productId) {
      res.status(400).json({ error: "productId required" });
      return;
    }

    const pRef = db.collection("products_new").doc(productId);
    const pSnap = await pRef.get();

    if (!pSnap.exists) {
      res.status(404).json({ error: `Product ${productId} not found` });
      return;
    }
    const pData = pSnap.data() || {};
    const ctOriginal = pData.current_tier || 0;

    // 2. Find all material documents that need to be processed.
    const parentRefsSnap = await db.collection("materials")
      .where("parent_material", "!=", null)
      .select("parent_material")
      .get();
    const parentIds = new Set(parentRefsSnap.docs.map(doc => doc.data().parent_material.id));

    const candidatesSnap = await db.collection("materials")
      .where("linked_product", "==", pRef)
      .where("tier", "==", ctOriginal)
      .where("final_tier", "!=", true)
      .get();

    const mDocs = candidatesSnap.docs.filter(doc => !parentIds.has(doc.id));

    if (mDocs.length === 0) {
      const msg = "...";
      logger.info(`[cf28] ${msg}`);
      res.json({ status: "ok", message: msg });
      return;
    }
    logger.info(`[cf28] Found ${mDocs.length} materials at tier ${ctOriginal} to process.`);

    // 3. Increment the product's current_tier.
    const ctNew = ctOriginal + 1;
    await pRef.update({ current_tier: admin.firestore.FieldValue.increment(1) });
    logger.info(`[cf28] Incremented product tier to ${ctNew}.`);

    // 4. Trigger apcfMaterials2 for all identified materials.
    await Promise.all(
      mDocs.map(doc => callCF("cf25", { materialId: doc.id }))
    );
    logger.info(`[cf28] Triggered cf25 for ${mDocs.length} materials.`);

    // 5. Begin the monitoring loop.
    logger.info(`[cf28] Starting monitoring loop...`);
    while (true) {
      const mDocsLatestSnaps = await db.getAll(...mDocs.map(doc => doc.ref));
      const allOriginalJobsDone = mDocsLatestSnaps.every(snap => snap.data()?.apcfMaterials2_done === true);

      let nmTotal = 0, amfTotal = 0, asaTotal = 0, atcfTotal = 0, asfTotal = 0, ampcfTotal = 0;
      let allSubJobsDone = false;

      if (allOriginalJobsDone) {
        const baseQuery = db.collection("materials").where("linked_product", "==", pRef).where("tier", "==", ctNew);
        const [
          nmSnap, amfSnap, asaSnap,
          atcfSnap, asfSnap, ampcfSnap
        ] = await Promise.all([
          baseQuery.count().get(),
          baseQuery.where("...", "==", true).count().get(),
          baseQuery.where("...", "==", true).count().get(),
          baseQuery.where("...", "==", true).count().get(),
          baseQuery.where("...", "==", true).count().get(),
          baseQuery.where("...", "==", true).count().get()
        ]);

        nmTotal = nmSnap.data().count;
        amfTotal = amfSnap.data().count;
        asaTotal = asaSnap.data().count;
        atcfTotal = atcfSnap.data().count;
        asfTotal = asfSnap.data().count;
        ampcfTotal = ampcfSnap.data().count;

        if (nmTotal > 0 && amfTotal === nmTotal && asaTotal === nmTotal && atcfTotal === nmTotal && asfTotal === nmTotal && ampcfTotal === nmTotal) {
          allSubJobsDone = true;
        }
      }

      logger.info("\n=======================================\n");
      logger.info(`New Materials: ${nmTotal}`);
      logger.info(`\nSupplier: ${asfTotal} / ${nmTotal}`);
      logger.info(`Supplier Address: ${asaTotal} / ${nmTotal}`);
      logger.info(`Mass: ${amfTotal} / ${nmTotal}`);
      logger.info(`TransportCF: ${atcfTotal} / ${nmTotal}`);
      logger.info(`MPCF: ${ampcfTotal} / ${nmTotal}`);
      logger.info("\n=======================================\n");

      if (allSubJobsDone) {
        logger.info("[cf28] All new materials processed. Ending loop.");
        break;
      }

      await sleep(5000);
    }

    res.json({ status: "ok", message: "New tier processing and monitoring complete." });

  } catch (err) {
    logger.error("[cf28] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});