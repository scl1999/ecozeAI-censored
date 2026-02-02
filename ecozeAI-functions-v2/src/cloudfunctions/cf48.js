//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf48 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf48] Invoked");

  try {
    /******************** 1. Argument validation & Setup ********************/
    const { productId, materialId } = req.body;
    if ((!productId && !materialId) || (productId && materialId)) {
      res.status(400).json({ error: "Provide exactly one of productId OR materialId" });
      return;
    }

    let parentRef, reasoningSubcollection, dataSubcollection, reasoningCfName;
    let linkedProductId = null; // For logging purposes if materialId is used

    if (productId) {
      parentRef = db.collection("products_new").doc(productId);
      reasoningSubcollection = "pn_reasoning";
      dataSubcollection = "pn_data";
      reasoningCfName = "apcfInitial2";
    } else { // materialId must be present
      parentRef = db.collection("materials").doc(materialId);
      const mSnap = await parentRef.get();
      if (mSnap.exists) {
        linkedProductId = mSnap.data().linked_product?.id || null;
      }
      reasoningSubcollection = "m_reasoning";
      dataSubcollection = "m_data";
      reasoningCfName = "apcfMaterials2";
    }

    /******************** 2. Data Fetching ********************/
    const parentSnap = await parentRef.get();
    if (!parentSnap.exists) {
      res.status(404).json({ error: "Parent document not found" });
      return;
    }

    // 2a. Fetch the reasoning document
    const reasoningQuery = await parentRef.collection(reasoningSubcollection)
      .where("cloudfunction", "==", reasoningCfName)
      .orderBy("createdAt", "desc")
      .limit(1)
      .get();

    if (reasoningQuery.empty) {
      throw new Error(`No '${reasoningCfName}' reasoning document found.`);
    }
    const rDoc = reasoningQuery.docs[0].data();

    // 2b. Fetch the data source documents
    const dataQuery = await parentRef.collection(dataSubcollection)
      .where("type", "==", "BOM")
      .get();

    if (dataQuery.empty) {
      logger.warn(`[cf48] No 'BOM' type data documents found. Proceeding without URL context.`);
    }

    /******************** 3. Prompt Construction ********************/
    const responseMarker = "Response:";
    const originalReasoning = rDoc.reasoningOriginal || "";
    const reasoningIndex = originalReasoning.indexOf(responseMarker);
    const reasoningText = reasoningIndex !== -1
      ? originalReasoning.substring(reasoningIndex + responseMarker.length).trim()
      : originalReasoning;

    const urlLines = dataQuery.docs.map((doc, i) => {
      const data = doc.data();
      return `url_${i + 1}: ${data.url || "Unknown"}\nurl_used_info_${i + 1}: ${data.info_used || "Unknown"}`;
    }).join("\n\n");

    const query = `AI Reasoning:\n${reasoningText}\n\nURLs:\n${urlLines}`;

    /******************** 4. AI Call & Logging ********************/
    const SYS_MSG = "...";

    const vGenerationConfig = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 24576,
      },
    };

    const { answer, thoughts, cost, totalTokens, searchQueries, model, rawConversation } = await runGeminiStream({
      model: 'aiModel', //flash
      generationConfig: vGenerationConfig,
      user: query,
    });

    await logAITransaction({
      cfName: 'cf48',
      productId: productId || linkedProductId,
      materialId: materialId,
      cost,
      totalTokens,
      searchQueries,
      modelUsed: model,
    });

    await logAIReasoning({
      sys: SYS_MSG,
      user: query,
      thoughts,
      answer,
      cloudfunction: 'cf48',
      productId: productId || linkedProductId,
      materialId: materialId,
      rawConversation,
    });

    /******************** 5. Process AI Response & Update DB ********************/
    const sources = parseSupplierSources(answer);
    if (sources.length === 0) {
      logger.warn("[cf48] AI did not return any parsable supplier sources.");
      res.json("Done");
      return;
    }

    const batch = db.batch();
    for (const source of sources) {
      const materialQuery = await db.collection("materials")
        .where("name", "==", source.name)
        .orderBy("createdAt", "desc")
        .limit(1)
        .get();

      if (materialQuery.empty) {
        logger.warn(`[cf48] Could not find material document for: "${source.name}"`);
        continue;
      }

      const m2DocRef = materialQuery.docs[0].ref;

      const lastIndexSnap = await m2DocRef.collection("m_data")
        .orderBy("index", "desc")
        .limit(1)
        .get();

      const inM = lastIndexSnap.empty ? 0 : (lastIndexSnap.docs[0].data().index || 0);

      const newMDataPayload = {
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        type: "Supplier",
        index: inM + 1,
        info_used: source.info_used,
        url: source.url,
        url_used: true,
      };

      const newDocRef = m2DocRef.collection("m_data").doc();
      batch.set(newDocRef, newMDataPayload);
      logger.info(`[cf48] Queued new 'Supplier' data for material: "${source.name}"`);
    }

    await batch.commit();
    logger.info(`[cf48] Successfully committed ${sources.length} new data documents.`);

    res.json("Done");

  } catch (err) {
    logger.error("[cf48] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------

/**
 * Runs daily at 9am UTC to find overdue prospects and send an email reminder.
 */