//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf31 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf31] Invoked");
  try {
    // 1. Argument Parsing & Document Fetching
    const { productId } = req.body;
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

    // 2. Gather URLs from sdCF data documents
    const dataSnap = await pRef.collection("pn_data").where("type", "==", "sdCF").get();
    const urls = [];
    if (!dataSnap.empty) {
      dataSnap.forEach(doc => {
        const url = doc.data().url;
        if (url) {
          urls.push(url);
        }
      });
    }
    logger.info(`[cf31] Found ${urls.length} source URLs for product ${productId}.`);

    // 3. Construct AI Prompt
    let query = `
Product or Activity: ${pData.name || "Unknown"}
Carbon footprint / GWP Total (kgCO2e): ${pData.supplier_cf || "Unknown"}

Sources (URLs):
`;
    if (urls.length > 0) {
      query += urls.map((url, i) => `url_${i + 1}: ${url}`).join('\n');
    } else {
      query += "No specific sources found in database. Please search the web for official manufacturer disclosures for the product specified.";
    }

    // 4. AI Call Configuration & Execution
    const SYS_OM = "...";

    const vGenerationConfig = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 24576
      },
    };

    const collectedUrls = new Set();
    const { answer, thoughts, cost, totalTokens, searchQueries, model, rawConversation } = await runGeminiStream({
      model: 'aiModel',//flash
      generationConfig: vGenerationConfig,
      user: query,
      collectedUrls,
    });

    await logAITransaction({
      cfName: 'cf31',
      productId: productId,
      cost: cost,
      totalTokens: totalTokens,
      searchQueries: searchQueries,
      modelUsed: model,
    });

    await logAIReasoning({
      sys: SYS_OM,
      user: query,
      thoughts: thoughts,
      answer: answer,
      cloudfunction: 'cf31',
      productId: productId,
      rawConversation: rawConversation,
    });

    if (collectedUrls.size > 0) {
      await saveURLs({
        urls: Array.from(collectedUrls),
        productId: productId,
        sys: SYS_OM,
        user: query,
        thoughts: thoughts,
        answer: answer,
        cloudfunction: 'cf31',
      });
    }

    // 5. Parse Response and Update Firestore
    const metrics = parseOtherMetrics(answer);
    const updatePayload = {};

    // Conditionally add metrics to the payload if they are valid numbers
    updatePayload.ap_total = metrics.ap_value;
    updatePayload.ep_total = metrics.ep_value;
    updatePayload.adpe_total = metrics.adpe_value;
    updatePayload.gwp_f_total = metrics.gwp_f_value;
    updatePayload.gwp_b_total = metrics.gwp_b_value;
    updatePayload.gwp_l_total = metrics.gwp_l_value;

    if (Object.keys(updatePayload).length > 0) {
      await pRef.update(updatePayload);
      logger.info(`[cf31] Updated product ${productId} with other metrics:`, updatePayload);
    } else {
      logger.warn(`[cf31] No valid metrics were found in the AI response for product ${productId}.`);
    }

    await pRef.update({ apcfOtherMetrics2_done: true });
    // 6. Finalize
    res.json("Done");
  } catch (err) {
    logger.error("[cf31] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});