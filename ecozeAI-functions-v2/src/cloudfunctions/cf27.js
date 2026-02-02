//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf27 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  try {
    const materialId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || "";
    if (!materialId) {
      res.status(400).json({ error: "materialId is required" });
      return;
    }

    const mRef = db.collection("materials").doc(materialId);

    // 1. Check if the material already has children.
    const childrenCountSnap = await db.collection("materials")
      .where("parent_material", "==", mRef)
      .count()
      .get();

    if (childrenCountSnap.data().count > 0) {
      const msg = "...";
      logger.info(`[cf27] Skipped: ${msg}`);
      res.json({ status: "skipped", reason: msg });
      return;
    }

    // 2. Check if the material meets the conditions for processing.
    const mSnap = await mRef.get();
    if (!mSnap.exists) {
      res.status(404).json({ error: `Material ${materialId} not found.` });
      return;
    }
    const mData = mSnap.data() || {};

    if (mData.final_tier === true || (mData.tier || 0) > 5) {
      const msg = "...";
      logger.info(`[cf27] Skipped: ${msg}`);
      res.json({ status: "skipped", reason: msg });
      return;
    }

    // 3. Trigger the apcfMaterials2 cloud function for the material.
    logger.info(`[cf27] Triggering cf25 for material ${materialId}.`);
    await callCF("cf25", { materialId });

    // --- Monitoring Loop ---
    logger.info(`[cf27] Starting monitoring loop for children of ${materialId}.`);
    while (true) {
      const mSnapLatest = await mRef.get();
      const originalJobDone = mSnapLatest.data()?.apcfMaterials2_done === true;

      let nmTotal = 0, amfTotal = 0, asaTotal = 0, atcfTotal = 0, asfTotal = 0, ampcfTotal = 0;
      let allSubJobsDone = false;

      if (originalJobDone) {
        const baseQuery = db.collection("materials").where("parent_material", "==", mRef);
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

        if (nmTotal === 0) {
          allSubJobsDone = true;
        } else if (amfTotal === nmTotal && asaTotal === nmTotal && atcfTotal === nmTotal && asfTotal === nmTotal && ampcfTotal === nmTotal) {
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
        logger.info(`[cf27] All new materials for parent ${materialId} are processed. Ending loop.`);
        break;
      }

      await sleep(5000);
    }

    res.json("Done");

  } catch (err) {
    logger.error("[cf27] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});