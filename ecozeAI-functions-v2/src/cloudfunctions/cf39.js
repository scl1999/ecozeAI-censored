//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf39 = onRequest({
  region: REGION,
  timeoutSeconds: 60,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  try {
    const materialId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || "";
    if (!materialId) {
      res.status(400).json({ error: "materialId is required." });
      return;
    }

    const mRef = db.collection("materials").doc(materialId);
    const mSnap = await mRef.get();

    if (!mSnap.exists) {
      res.status(404).json({ error: `Material ${materialId} not found.` });
      return;
    }
    const mData = mSnap.data() || {};
    const pRef = mData.linked_product;

    // A linked product is necessary to schedule the main checker or set final status
    if (!pRef) {
      logger.error(`[cf39] Material ${materialId} has no linked_product. Cannot proceed.`);
      res.status(400).json({ error: "Material is missing a linked_product reference." });
      return;
    }

    // 1. Find all child materials (m2Docs) that have the current material as a parent.
    const childrenQuery = db.collection("materials").where("parent_material", "==", mRef);
    const childrenSnap = await childrenQuery.select().get();

    // {{If no child materials are found}}
    if (childrenSnap.empty) {
      logger.info(`[cf39] No sub-materials found for parent ${materialId}. Scheduling main status check.`);
      // 2. Schedule the main status checker and 3. End early.
      await scheduleNextCheck(pRef.id);
      res.json({ status: "pending", message: "No sub-materials found. Re-scheduling main status check in 5 minutes." });
      return;
    }

    // {{If child materials are found}}
    // 2. Count how many children have not completed their MPCF calculation.
    const incompleteChildrenSnap = await childrenQuery.where("apcfMPCF_done", "==", false).count().get();
    const nm2Done = incompleteChildrenSnap.data().count;

    // {If nm2Done > 0}
    if (nm2Done > 0) {
      logger.info(`[cf39] Parent ${materialId} has ${nm2Done} incomplete sub-materials. Scheduling main status check.`);
      // 3. Schedule the main status checker and 4. End early.
      await scheduleMainStatusCheck(pRef.id);
      res.json({ status: "pending", message: `${nm2Done} sub-materials are still processing. Re-scheduling main status check in 5 minutes.` });
      return;
    }

    // {If nm2Done = 0}
    logger.info(`[cf39] All ${childrenSnap.size} sub-materials for parent ${materialId} are complete.`);
    // 3. Set the linked product's status to "Done".
    await pRef.update({ status: "Done" });
    logger.info(`[cf39] Successfully set status to "Done" for product ${pRef.id}.`);
    // 4. End the cloudfunction.
    res.json({ status: "complete", message: "All sub-materials are processed. Product status set to Done." });

  } catch (err) {
    logger.error("[cf39] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------

/****************************************************************************************
 * Backend Updates $$$
 ****************************************************************************************/