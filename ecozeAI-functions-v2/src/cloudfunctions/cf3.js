//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf3 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf3] Invoked");
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

    /******************** 2. AI Call ********************/
    const SYS_CBAM_FINAL_MATERIAL = "...";

    const userQuery = `...`;

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
      cfName: 'cf3',
      productId: productId,
      materialId: materialId,
      cost: cost,
      totalTokens: totalTokens,
      searchQueries: searchQueries,
      modelUsed: model,
    });

    await logAIReasoning({
      sys: SYS_CBAM_FINAL_MATERIAL,
      user: userQuery,
      thoughts: thoughts,
      answer: answer,
      cloudfunction: 'cf3',
      productId: productId,
      materialId: materialId,
      rawConversation: rawConversation,
    });

    /******************** 3. Process & Save AI Response ********************/
    const parsedData = parseProductCBAMProcessing(answer);

    if (!parsedData.processName && parsedData.fuels.length === 0 && parsedData.wastes.length === 0) {
      logger.warn(`[cf3] AI returned no parsable data for final-tier material ${materialId}.`);
    } else {
      const payload = {
        material: mRef,
        name: parsedData.processName,
        cbam_fes: parsedData.fuels.map(f => ({
          name: f.name,
          amount: f.amount,
          amount_unit: f.amount_unit,
          scope: f.scope,
          co2e_kg: f.co2e_kg,
        })),
        cbam_waste_materials: parsedData.wastes.map(w => ({
          material_name: w.material_name,
          amount: w.amount,
          co2e_kg: w.co2e_kg,
        })),
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      };
      await pRef.collection("pn_cbam").add(payload);
      logger.info(`[cf3] Saved CBAM creation data to subcollection for final-tier material ${materialId}.`);
    }

    /******************** 4. Finalize ********************/
    res.json("Done");

  } catch (err) {
    logger.error("[cf3] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------

/****************************************************************************************
 * Other Cloudfunctions $$$
 ****************************************************************************************/