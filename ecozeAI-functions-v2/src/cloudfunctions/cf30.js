//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf30 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf30] Invoked");
  try {
    /******************** 1. Argument validation & Setup ********************/
    const { productId, materialId, calculationLabel } = req.body;
    const entityType = productId ? 'product' : 'material';

    if ((!productId && !materialId) || (productId && materialId) || !calculationLabel) {
      res.status(400).json({ error: "Provide a calculationLabel and exactly one of productId OR materialId" });
      return;
    }

    let query = "";
    let otherMetricsTargetRef = null; // Ref for the final pn_otherMetrics doc
    let materialRefForPayload = null;
    const responseMarker = "Response:";

    /******************** 2. Data Fetching & Prompt Construction ********************/
    if (productId) {
      const pRef = db.collection("products_new").doc(productId);
      otherMetricsTargetRef = pRef;

      const pSnap = await pRef.get();
      if (!pSnap.exists) throw new Error(`Product ${productId} not found`);
      const pData = pSnap.data() || {};

      if (calculationLabel === "cf49") {
        const [reasoningSnap, transportSnap] = await Promise.all([
          pRef.collection("pn_reasoning").where("cloudfunction", "==", "cf49").get(),
          pRef.collection("pn_transport").orderBy("leg").get()
        ]);

        const transportLines = transportSnap.docs.map(doc => {
          const data = doc.data();
          return `leg_${data.leg}_transport_method: ${data.transport_method}\nleg_${data.leg}_distance_km: ${data.distance_km}\nleg_${data.leg}_emissions_kgco2e: ${data.emissions_kgco2e}`;
        }).join("\n\n");

        const reasoningLines = reasoningSnap.docs.map(doc => {
          const original = doc.data().reasoningOriginal || "";
          const index = original.indexOf(responseMarker);
          return index !== -1 ? original.substring(index + responseMarker.length).trim() : original;
        }).join("\n\n---\n\n");

        query = `Emissions Total (kgCO2e): ${pData.transport_cf || 0}\n\nSub Calculation(s):\n${transportLines}\n\nCalculation(s) Reasoning:\n${reasoningLines}`;
      } else { // Handles cf15 and cf21
        const cf_value = ["cf15", "cf16", "cf18"].includes(calculationLabel) ? pData.cf_full || 0 : pData.cf_processing || 0;
        const reasoningSnap = await pRef.collection("pn_reasoning").where("cloudfunction", "==", calculationLabel).orderBy("createdAt", "desc").limit(1).get();
        if (reasoningSnap.empty) throw new Error(`No reasoning doc found for ${calculationLabel} on product ${productId}`);

        const original = reasoningSnap.docs[0].data().reasoningOriginal || "";
        const index = original.indexOf(responseMarker);
        const reasoning = index !== -1 ? original.substring(index + responseMarker.length).trim() : original;

        query = `Emissions Total (kgCO2e): ${cf_value}\n\nCalculation Reasoning:\n${reasoning}`;
      }

    } else { // materialId must be present
      const mRef = db.collection("materials").doc(materialId);
      materialRefForPayload = mRef;

      const mSnap = await mRef.get();
      if (!mSnap.exists) throw new Error(`Material ${materialId} not found`);
      const mData = mSnap.data() || {};

      if (!mData.linked_product) throw new Error(`Material ${materialId} has no linked_product`);
      otherMetricsTargetRef = mData.linked_product;

      if (calculationLabel === "cf49") {
        const [reasoningSnap, transportSnap] = await Promise.all([
          mRef.collection("m_reasoning").where("cloudfunction", "==", "cf49").get(),
          mRef.collection("materials_transport").orderBy("leg").get()
        ]);

        const transportLines = transportSnap.docs.map(doc => {
          const data = doc.data();
          return `leg_${data.leg}_transport_method: ${data.transport_method}\nleg_${data.leg}_distance_km: ${data.distance_km}\nleg_${data.leg}_emissions_kgco2e: ${data.emissions_kgco2e}`;
        }).join("\n\n");

        const reasoningLines = reasoningSnap.docs.map(doc => {
          const original = doc.data().reasoningOriginal || "";
          const index = original.indexOf(responseMarker);
          return index !== -1 ? original.substring(index + responseMarker.length).trim() : original;
        }).join("\n\n---\n\n");

        query = `Emissions Total (kgCO2e): ${mData.transport_cf || 0}\n\nSub Calculation(s):\n${transportLines}\n\nCalculation(s) Reasoning:\n${reasoningLines}`;
      } else { // Handles cf15 and cf21
        const cf_value = ["cf15", "cf16", "cf18"].includes(calculationLabel) ? mData.cf_full || 0 : mData.cf_processing || 0;
        const reasoningSnap = await mRef.collection("m_reasoning").where("cloudfunction", "==", calculationLabel).orderBy("createdAt", "desc").limit(1).get();
        if (reasoningSnap.empty) throw new Error(`No reasoning doc found for ${calculationLabel} on material ${materialId}`);

        const original = reasoningSnap.docs[0].data().reasoningOriginal || "";
        const index = original.indexOf(responseMarker);
        const reasoning = index !== -1 ? original.substring(index + responseMarker.length).trim() : original;

        query = `Emissions Total (kgCO2e): ${cf_value}\n\nCalculation Reasoning:\n${reasoning}`;
      }
    }

    if (!query) {
      throw new Error(`Invalid setup for calculationLabel: ${calculationLabel}`);
    }
    /******************** 3. AI Call & Logging ********************/
    const SYS_UN = "...";

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
      model: 'aiModel', //flash3
      generationConfig: vGenerationConfig,
      user: query,
    });

    await logAITransaction({
      cfName: 'cf30',
      productId: entityType === 'product' ? productId : otherMetricsTargetRef.id,
      materialId: materialId,
      cost,
      totalTokens,
      searchQueries,
      modelUsed: model,
    });

    await logAIReasoning({
      sys: SYS_UN,
      user: query,
      thoughts,
      answer,
      cloudfunction: 'cf30',
      productId: productId,
      materialId: materialId,
      rawConversation: rawConversation,
    });

    /******************** 4. Process AI Response ********************/
    const metrics = parseOtherMetrics(answer);

    /******************** 5. Write to Firestore ********************/
    const payload = {
      cloudfunction: calculationLabel,
      ap_value: metrics.ap_value,
      ep_value: metrics.ep_value,
      adpe_value: metrics.adpe_value,
      gwp_f_value: metrics.gwp_f_value,
      gwp_b_value: metrics.gwp_b_value,
      gwp_l_value: metrics.gwp_l_value,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    if (materialId && materialRefForPayload) {
      payload.material = materialRefForPayload;
    }

    await otherMetricsTargetRef.collection("pn_otherMetrics").add(payload);
    logger.info(`[cf30] Successfully created otherMetrics document in ${otherMetricsTargetRef.path}/pn_otherMetrics`);

    res.json({ status: "ok", doc_created: true });

  } catch (err) {
    logger.error("[cf30] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------


//~~
/****************************************************************************************
 * 8.  API Cloud Functions $$$
 ****************************************************************************************/

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------

/**
 * Gets the full path for the testing queue, creating it if it doesn't exist.
 */