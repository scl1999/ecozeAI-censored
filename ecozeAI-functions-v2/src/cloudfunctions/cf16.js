//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf16 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf16] Invoked");
  try {
    // 1. Argument Parsing and Validation
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || "";
    if (!productId) {
      res.status(400).json({ error: "productId is required" });
      return;
    }

    // 2. Fetch Product Document
    const pRef = db.collection("products_new").doc(productId);
    const pSnap = await pRef.get();
    if (!pSnap.exists) {
      res.status(404).json({ error: `Product ${productId} not found` });
      return;
    }
    const pData = pSnap.data() || {};
    const productName = pData.name;
    if (!productName) {
      throw new Error(`Product ${productId} has no name field.`);
    }

    // 4. Set up and run the main AI calculation call
    const SYS_MSG = "...";

    const USER_MSG = "...";

    const vGenerationConfig = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 32768,
      },
    };

    const collectedUrls = new Set();

    const { answer, thoughts, cost, totalTokens, searchQueries, model, rawConversation } = await runGeminiStream({
      model: 'aiModel', //pro
      generationConfig: vGenerationConfig,
      user: USER_MSG,
      collectedUrls
    });

    // 5. Log the AI interaction
    await logAITransaction({
      cfName: 'cf16',
      productId: productId,
      cost: cost,
      totalTokens: totalTokens,
      searchQueries: searchQueries,
      modelUsed: model,
    });

    await logAIReasoning({
      sys: SYS_MSG,
      user: USER_MSG,
      thoughts: thoughts,
      answer: answer,
      cloudfunction: 'cf16',
      productId: productId,
      rawConversation: rawConversation,
    });

    if (collectedUrls.size) {
      await saveURLs({
        urls: Array.from(collectedUrls),
        productId,
        pMPCFData: true,
        sys: SYS_MSG,
        user: USER_MSG,
        thoughts: thoughts,
        answer: answer,
        cloudfunction: 'cf16',
      });
    }

    // 6. Parse the AI's response
    const cfValue = parseCfValue(answer);
    // 7. Update the product document in Firestore
    const updatePayload = {
      apcfMPCFFullActivity_done: true,
      apcfMPCF_done: true, // Also set the main flag to prevent other loops
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    if (cfValue !== null) {
      updatePayload.cf_full = cfValue;
      logger.info(`[cf16] Updating product ${ productId } with cf_full: ${ cfValue } `);
    } else {
      logger.warn(`[cf16] AI did not return a valid cf_value for product ${ productId }.`);
    }

    await pRef.update(updatePayload);

    logger.info(`[cf16] Checking if other metrics calculation is needed...`);
    if (pData.otherMetrics === true) {
      logger.info(`[cf16] otherMetrics flag is true for product ${ productId }.Triggering calculation.`);
      await callCF("cf30", {
        productId: productId,
        calculationLabel: "cf16"
      });
    }
    // 8. Finalize and respond
    res.json({ status: "ok", docId: productId });

  } catch (err) {
    logger.error("[cf16] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});