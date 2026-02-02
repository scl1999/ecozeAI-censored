//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf4 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf4] Invoked");
  try {
    /******************** 1. Argument validation & Data Fetching ********************/
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || "";
    if (!productId) {
      res.status(400).json({ error: "productId is required" });
      return;
    }

    const pRef = db.collection("products_new").doc(productId);
    const pSnap = await pRef.get();
    if (!pSnap.exists) {
      res.status(404).json({ error: `Product ${productId} not found` });
      return;
    }
    const pData = pSnap.data() || {};

    const materialsSnap = await db.collection("materials")
      .where("linked_product", "==", pRef)
      .where("tier", "==", 1)
      .get();

    /******************** 2. AI Call ********************/
    const SYS_CBAM_PRODUCT_PROCESSING = "..."

    let userQuery = `...`;

    materialsSnap.docs.forEach((doc, i) => {
      const mData = doc.data();
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
      cfName: 'cf4',
      productId: productId,
      cost: cost,
      totalTokens: totalTokens,
      searchQueries: searchQueries,
      modelUsed: model,
    });

    await logAIReasoning({
      sys: SYS_CBAM_PRODUCT_PROCESSING,
      user: userQuery,
      thoughts: thoughts,
      answer: answer,
      cloudfunction: 'cf4',
      productId: productId,
      rawConversation: rawConversation,
    });

    /******************** 3. Process & Save AI Response ********************/
    const parsedData = parseProductCBAMProcessing(answer);

    if (!parsedData.processName && parsedData.fuels.length === 0 && parsedData.wastes.length === 0) {
      logger.warn(`[cf4] AI returned no parsable data for product ${productId}.`);
    } else {
      const payload = {
        name: parsedData.processName,
        cbam_fes: parsedData.fuels,
        cbam_waste_materials: parsedData.wastes,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      };
      await pRef.collection("pn_cbam").add(payload);
      logger.info(`[cf4] Saved CBAM processing data to subcollection for product ${productId}.`);
    }

    /******************** 4. Finalize ********************/
    res.json("Done");

  } catch (err) {
    logger.error("[cf4] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});