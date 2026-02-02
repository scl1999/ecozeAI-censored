//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf26 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  const body = req.method === "POST" ? req.body : req.query;
  const mId = body.materialId;
  let childMaterialsNewList = body.childMaterialsList || [];

  if (!mId) {
    res.status(400).json({ error: "materialId required" });
    return;
  }

  try {
    const mRef = db.collection("materials").doc(mId);
    let mSnap = await mRef.get();
    if (!mSnap.exists) {
      res.status(404).json({ error: `material ${mId} not found` });
      return;
    }
    let mData = mSnap.data() || {};

    logger.info(`[cf26] Started for material ${mId} with ${childMaterialsNewList.length} children.`);

    await verifyMaterialLinks(childMaterialsNewList, mRef);

    /* ╔═══════════════ De-duplication ═══════════════╗ */
    logger.info("[cf26] Starting de-duplication of child materials.");

    // Re-fetch children to ensure we have the latest set
    const matsSnap = await db.collection("materials").where("parent_material", "==", mRef).get();

    const groups = {};
    matsSnap.forEach(doc => {
      const key = (doc.get("name") || "").trim().toLowerCase();
      (groups[key] = groups[key] || []).push({
        id: doc.id,
        supplier: (doc.get("supplier_name") || "").trim(),
        ref: doc.ref,
        createdAt: doc.get("createdAt")
      });
    });

    const TEN_MINUTES_MS = 10 * 60 * 1000;
    const survivors = [];

    for (const nameKey of Object.keys(groups)) {
      const nameGroup = groups[nameKey];

      nameGroup.sort((a, b) => {
        const tA = a.createdAt ? a.createdAt.toMillis() : 0;
        const tB = b.createdAt ? b.createdAt.toMillis() : 0;
        return tA - tB;
      });

      let currentSubgroup = [];
      for (const material of nameGroup) {
        if (currentSubgroup.length === 0) {
          currentSubgroup.push(material);
          continue;
        }

        const firstTimestamp = currentSubgroup[0].createdAt ? currentSubgroup[0].createdAt.toMillis() : 0;
        const currentTimestamp = material.createdAt ? material.createdAt.toMillis() : 0;

        if (currentTimestamp - firstTimestamp <= TEN_MINUTES_MS) {
          currentSubgroup.push(material);
        } else {
          if (currentSubgroup.length > 1) {
            const keeper = currentSubgroup[0];
            const toMerge = currentSubgroup.slice(1);
            const altSupp = toMerge.map(d => d.supplier).filter(Boolean);
            const batch = db.batch();

            if (altSupp.length) {
              batch.update(keeper.ref, {
                alternative_suppliers: admin.firestore.FieldValue.arrayUnion(...altSupp)
              });
            }
            toMerge.forEach(d => batch.delete(d.ref));
            await batch.commit();
            logger.info(`[cf26] De-duplicated "${nameKey}" (time-windowed) - kept ${keeper.id}`);
            survivors.push(keeper.id);
          } else {
            survivors.push(currentSubgroup[0].id);
          }
          currentSubgroup = [material];
        }
      }

      if (currentSubgroup.length > 1) {
        const keeper = currentSubgroup[0];
        const toMerge = currentSubgroup.slice(1);
        const altSupp = toMerge.map(d => d.supplier).filter(Boolean);
        const batch = db.batch();

        if (altSupp.length) {
          batch.update(keeper.ref, {
            alternative_suppliers: admin.firestore.FieldValue.arrayUnion(...altSupp)
          });
        }
        toMerge.forEach(d => batch.delete(d.ref));
        await batch.commit();
        logger.info(`[cf26] De-duplicated "${nameKey}" (time-windowed) - kept ${keeper.id}`);
        survivors.push(keeper.id);
      } else if (currentSubgroup.length === 1) {
        survivors.push(currentSubgroup[0].id);
      }
    }

    childMaterialsNewList = survivors;
    logger.info(`[cf26] De-duplication complete. ${childMaterialsNewList.length} unique materials remain.`);

    await sleep(5000);

    // Step 3: Run initial finders in parallel for all new child materials.
    logger.info(`[cf26] Running SupplierFinder and MassFinder for ${childMaterialsNewList.length} new materials.`);

    // --- START: Conditional Supplier Finder Logic ---
    const drMaterialIds = new Set();
    {
      try {
        const drSnapshot = await db.collection("materials")
          .where("parent_material", "==", mRef)
          .orderBy("createdAt", "asc")
          .limit(60)
          .get();

        drSnapshot.forEach(doc => drMaterialIds.add(doc.id));
        logger.info(`[cf26] Identified ${drMaterialIds.size} child materials for Deep Research (First 60 of parent).`);
      } catch (err) {
        logger.error(`[cf26] Failed to fetch DR material list: ${err.message}. Defaulting to Standard.`);
      }
    }
    // --- END: Conditional Supplier Finder Logic ---

    // FIX: Using Promise.allSettled
    const results = await Promise.allSettled(childMaterialsNewList.map(async (mId) => {
      try {
        const mRef = db.collection("materials").doc(mId);
        let data = (await mRef.get()).data() || {};

        if (drMaterialIds.has(mId)) {
          logger.info(`[cf26] Triggering cf46 (Deep Research) for ${mId}`);
          await callCF("cf43", { materialId: mId });
        } else {
          logger.info(`[cf26] Triggering cf43 (Standard) for ${mId}`);
          await callCF("cf43", { materialId: mId });
        }
        await mRef.update({ apcfSupplierFinder_done: true });
        data = (await mRef.get()).data() || {};
        if ((data.supplier_name || "Unknown") === "Unknown") {
          await mRef.update({ final_tier: true });
        }

        const isSoS = data.software_or_service === true;
        const isMassFinderDone = data.apcfMassFinder_done === true;

        if (!isSoS && !isMassFinderDone) {
          try {
            await callCF("cf22", { materialId: mId });
          } catch {
            logger.warn(`[cf26] cf22 for ${mId} failed (ignored)`);
          }
        } else {
          logger.info(`[cf26] Skipping cf22 for ${mId}. SoS: ${isSoS}, Done: ${isMassFinderDone}`);
        }
      } catch (innerErr) {
        logger.error(`[cf26] Error processing child material ${mId}:`, innerErr);
      }
    }));

    const failures = results.filter(r => r.status === 'rejected');
    if (failures.length > 0) {
      logger.warn(`[cf26] ${failures.length} child materials failed during processing, but batch continued.`);
    }
    logger.info(`[cf26] Finished initial finders.`);

    // Step 4 & 5: Trigger cf23 and wait.
    logger.info(`[cf26] Triggering cf23 for ${childMaterialsNewList.length} materials.`);
    await callCF("cf23", { materialsNewList: childMaterialsNewList, materialId: mId });
    logger.info(`[cf26] cf23 finished.`);

    // Step 6: Trigger cf24 in parallel for all new child materials.
    logger.info(`[cf26] Triggering cf24 for ${childMaterialsNewList.length} materials.`);
    const matResults = await Promise.allSettled(childMaterialsNewList.map(mId =>
      callCF("cf24", { materialId: mId })
    ));

    const matFailures = matResults.filter(r => r.status === 'rejected');
    if (matFailures.length > 0) {
      logger.warn(`[cf26] ${matFailures.length} child cf24 calls failed.`);
    }
    logger.info(`[cf26] cf24 calls finished.`);

    logger.info(`[cf26] Triggering cf21 for material ${mId} before main BoM loop.`);
    await callCF("cf21", { materialId: mId });
    logger.info(`[cf26] Completed cf21 for ${mId}.`);

    // Step 8: Trigger cf8 and wait.
    logger.info(`[cf26] Triggering cf8 for ${childMaterialsNewList.length} materials.`);
    await callCF("cf8", { materialsNewList: childMaterialsNewList, materialId: mId });
    logger.info(`[cf26] cf8 finished.`);

    // Note: AI History and URL saving was handled in BOM function.

    logger.info(`[cf26] Loop finished for ${mId}.`);

    const linkedProductId = mData.linked_product ? mData.linked_product.id : null;
    if (linkedProductId) {
      logger.info(`[cf26] Scheduling status check for linked product ${linkedProductId}.`);
      await scheduleNextCheck(linkedProductId);
    } else {
      logger.warn(`[cf26] Material ${mId} has no linked product. Cannot schedule status check.`);
    }

    const finalLinkedProductId = mData.linked_product ? mData.linked_product.id : null;
    if (finalLinkedProductId) {
      const pRef = db.collection("products_new").doc(finalLinkedProductId);

      /*
      const uncertaintySnap = await pRef.collection("pn_uncertainty").get();
      let uSum = 0;
 
      if (!uncertaintySnap.empty) {
        uncertaintySnap.forEach(doc => {
          const uncertaintyValue = doc.data().co2e_uncertainty_kgco2e;
          if (typeof uncertaintyValue === 'number' && isFinite(uncertaintyValue)) {
            uSum += uncertaintyValue;
          }
        });
      }
      logger.info(`[cf26] Calculated total uncertainty for parent product ${pRef.id}: ${uSum}`);
      */

      const finalUpdatePayload = {
        status: "Done",
        //total_uncertainty: uSum
      };

      const mpDocSnap = await pRef.get();
      const mpDocData = mpDocSnap.data() || {};
      if (mpDocData.otherMetrics === true) {
        logger.info(`[cf26] otherMetrics flag is true for ${pRef.id}. Aggregating totals.`);
        const metricsSnap = await pRef.collection("pn_otherMetrics").get();

        const totals = { ap_total: 0, ep_total: 0, adpe_total: 0, gwp_f_total: 0, gwp_b_total: 0, gwp_l_total: 0 };
        const fieldsToSum = [
          { from: 'ap_value', to: 'ap_total' }, { from: 'ep_value', to: 'ep_total' },
          { from: 'adpe_value', to: 'adpe_total' }, { from: 'gwp_f_value', to: 'gwp_f_total' },
          { from: 'gwp_b_value', to: 'gwp_b_total' }, { from: 'gwp_l_value', to: 'gwp_l_total' },
        ];

        if (!metricsSnap.empty) {
          metricsSnap.forEach(doc => {
            const data = doc.data();
            fieldsToSum.forEach(field => {
              if (typeof data[field.from] === 'number' && isFinite(data[field.from])) {
                totals[field.to] += data[field.from];
              }
            });
          });
        }
        logger.info(`[cf26] Calculated other metrics totals for ${pRef.id}:`, totals);
        Object.assign(finalUpdatePayload, totals);
      }

      await pRef.update(finalUpdatePayload);
    }

    await mRef.update({
      apcfMaterials2_done: true,
      apcfMaterials2EndTime: admin.firestore.FieldValue.serverTimestamp()
    });
    res.json({ status: "ok", materialId: mId });

  } catch (err) {
    logger.error(`[cf26] Uncaught error for material ${mId}:`, err);
    res.status(500).json({ error: String(err) });
  }
});


//-----------------------------------------------------------------------------------------------------------------------------------------------------------------



//!! Legal Entity Resolution Required: When you name a supplier, you must output the exact legal entity name that appears in the evidence. Do not output a group brand umbrella (“Foxconn”, “LG”, “Samsung”) unless the evidence itself uses that umbrella as the legal counterparty. If the evidence uses an alias/trade name, you must resolve alias→legal entity using a corporate registry/filing (e.g., LEI/OpenCorporates/SEC) and cite it. (Example ambiguity: LG vs LG Display vs LG Innotek; Foxconn vs specific Hon Hai/FIH subsidiary/site.)!!