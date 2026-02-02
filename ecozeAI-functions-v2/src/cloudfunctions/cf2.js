//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf2 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf2] Invoked");
  try {
    /******************** 1. Argument validation & Data Fetching ********************/
    const { productId, materialId } = req.body;
    if (!productId || !materialId) {
      res.status(400).json({ error: "Both productId and materialId are required" });
      return;
    }

    const mRef = db.collection("materials").doc(materialId);
    const mSnap = await mRef.get();
    if (!mSnap.exists) {
      res.status(404).json({ error: `Material ${materialId} not found` });
      return;
    }
    const mData = mSnap.data() || {};

    const pRef = db.collection("products_new").doc(productId);
    const pSnap = await pRef.get();
    if (!pSnap.exists) {
      res.status(404).json({ error: `Product ${productId} not found` });
      return;
    }

    // Find all child materials (m2Docs)
    const childMaterialsSnap = await db.collection("materials")
      .where("parent_material", "==", mRef)
      .get();

    /******************** 2. AI Call ********************/
    const SYS_CBAM_MATERIAL_PROCESSING = "...";

    let userQuery = `...`;

    childMaterialsSnap.docs.forEach((doc, i) => {
      const m2Data = doc.data();
      userQuery += `...`;
    });

    const vGenerationConfig = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 32768
      },
    };

    const { answer, thoughts, cost, totalTokens, searchQueries, model, rawConversation } = await runGeminiStream({
      model: 'aiModel', //pro
      generationConfig: vGenerationConfig,
      user: userQuery,
    });

    await logAITransaction({
      cfName: 'cf2',
      productId: productId,
      materialId: materialId,
      cost: cost,
      totalTokens: totalTokens,
      searchQueries: searchQueries,
      modelUsed: model,
    });

    await logAIReasoning({
      sys: SYS_CBAM_MATERIAL_PROCESSING,
      user: userQuery,
      thoughts: thoughts,
      answer: answer,
      cloudfunction: 'cf2',
      productId: productId,
      materialId: materialId,
      rawConversation: rawConversation,
    });

    /******************** 3. Process & Save AI Response ********************/
    const parsedData = parseProductCBAMProcessing(answer);

    if (!parsedData.processName && parsedData.fuels.length === 0 && parsedData.wastes.length === 0) {
      logger.warn(`[cf2] AI returned no parsable data for material ${materialId}.`);
    } else {
      const payload = {
        material: mRef, // Add reference to the parent material
        name: parsedData.processName,
        cbam_fes: parsedData.fuels,
        cbam_waste_materials: parsedData.wastes,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      };
      await pRef.collection("pn_cbam").add(payload);
      logger.info(`[cf2] Saved CBAM processing data to subcollection for material ${materialId}.`);
    }

    /******************** 4. Finalize ********************/
    res.json("Done");

  } catch (err) {
    logger.error("[cf2] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});