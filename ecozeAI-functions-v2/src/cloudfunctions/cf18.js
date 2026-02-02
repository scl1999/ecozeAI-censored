//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf18 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf18] Invoked");
  try {
    // 1. Argument Parsing and Validation
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || null;
    if (!productId) {
      res.status(400).json({ error: "Provide productId" });
      return;
    }

    // 2. Fetch the product document
    const targetRef = db.collection("products_new").doc(productId);
    const targetSnap = await targetRef.get();
    if (!targetSnap.exists) {
      res.status(404).json({ error: `Document not found` });
      return;
    }
    const targetData = targetSnap.data() || {};
    const prodName = (targetData.name || "").trim();

    // 4. Construct prompts for the main AI call
    const USER_MSG = "...";
    const SYS_MSG = "...";

    // 5. Perform the single AI call
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

  const collectedUrls = new Set();

  const { answer, thoughts, cost, totalTokens, searchQueries, model, rawConversation } = await runGeminiStream({
    model: 'aiModel', //pro
    generationConfig: vGenerationConfig,
    user: USER_MSG,
    collectedUrls
  });

  // 6. Log the AI interaction
  await logAITransaction({
    cfName: 'cf18',
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
    cloudfunction: 'cf18',
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
      cloudfunction: 'cf18',
    });
  }

  // 7. Parse the AI result
  const aiCalc = parseCfValue(answer);

  // 8. Update Firestore if the result is valid
  if (aiCalc !== null) {
    await targetRef.update({
      cf_full: aiCalc,
    });
    logger.info(`[cf18] 🏁 Firestore update committed for value: ${aiCalc} `);
  } else {
    logger.warn("[cf18] ⚠️ AI did not supply a numeric *cf_value*. No updates made.");
  }

  // 9. Trigger Other Metrics Calculation
  logger.info(`[cf18] Checking if other metrics calculation is needed...`);
  if (targetData.otherMetrics === true) {
    logger.info(`[cf18] otherMetrics flag is true for product ${productId}.Triggering calculation.`);
    await callCF("cf30", {
      productId: productId,
      calculationLabel: "cf18"
    });
  }

  logger.info(`[cf18] Checking if other metrics calculation is needed...`);
  if (targetData.otherMetrics === true) {
    logger.info(`[cf18] otherMetrics flag is true for product ${productId}.Triggering calculation.`);
    await callCF("cf30", {
      productId: productId,
      calculationLabel: "cf18"
    });
  }

  // 10. Finalize the function
  await targetRef.update({
    apcfMPCFFullGeneric_done: true,
    apcfMPCF_done: true, // Also set the main flag to prevent other loops
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  });

  res.json("Done");

} catch (err) {
  logger.error("[cf18] Uncaught error:", err);
  res.status(500).json({ error: String(err) });
}
});