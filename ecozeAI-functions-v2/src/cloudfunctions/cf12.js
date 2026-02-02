//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf12 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  try {
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || "";
    if (!productId) {
      res.status(400).json({ error: "productId required" });
      return;
    }
    const pRef = db.collection("products_new").doc(productId);
    const pSnap = await pRef.get();
    if (!pSnap.exists) {
      res.status(404).json({ error: `product ${productId} not found` });
      return;
    }

    // Set start time
    await pRef.update({ apcfInitial2StartTime: admin.firestore.FieldValue.serverTimestamp() });

    logger.info(`[cf12] Calling cf43 for product ${productId}`);
    await callCF("cf43", { productId: productId });

    logger.info(`[cf12] Calling cf41 for product ${productId}`);
    await callCF("cf41", { productId: productId });
    logger.info(`[cf12] cf41 completed for product ${productId}`);

    const pDoc = pSnap.data() || {};
    let productName = pDoc.name || "(unknown product)";
    if (pDoc.includePackaging === true) {
      productName += " (Include Packaging)";
    }

    let existingHistory;
    try {
      existingHistory = JSON.parse(pDoc.z_ai_history || "[]");
    } catch {
      existingHistory = [];
    }

    const collectedUrls = new Set();

    const promptLines = [`Product Name: ${productName}`];
    if (pDoc.mass && pDoc.mass_unit) {
      promptLines.push(`Product Weight/Mass: ${pDoc.mass} ${pDoc.mass_unit}`);
    }
    if (pDoc.manufacturer_name) {
      promptLines.push(`Supplier Name: ${pDoc.manufacturer_name}`);
    }
    if (pDoc.description) {
      promptLines.push(`Description: ${pDoc.description}`);
    }

    const initialPrompt = "...".join('\n');

    logger.info(`[cf12] Starting chat loop for product "${productName}"`);

    const modelUsed = 'aiModel';
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

    const foundMaterials = new Set();
    const { finalAnswer, history, tokens: totalTokens, cost, searchQueries, model, rawConversation, logForReasoning, } = await runChatLoop({
      model: modelUsed,
      generationConfig: vGenerationConfig,
      initialPrompt: initialPrompt,
      followUpPrompt: GO_AGAIN_PROMPT,
      maxFollowUps: FOLLOWUP_LIMIT,
      existingHistory,
      collectedUrls,
      onTurnComplete: async (history, latestAnswer) => {
        const materials = parseBom(latestAnswer);
        materials.forEach(m => {
          if (m.mat) foundMaterials.add(m.mat.trim().toLowerCase());
        });
        await pRef.update({ apcfIMaterialsNum: foundMaterials.size });
        logger.info(`[cf12] Incremental material count: ${foundMaterials.size}`);
      }
    });

    await logAIReasoning({
      sys: BOM_SYS,
      user: initialPrompt,
      thoughts: logForReasoning,
      answer: finalAnswer,
      cloudfunction: 'apcfInitial2', // Kept generic name or specific? Using 'apcfInitial2' as per logic grouping
      productId: productId,
      rawConversation: rawConversation,
    });

    await logAITransaction({
      cfName: 'apcfInitial2',
      productId: productId,
      cost: cost,
      totalTokens: totalTokens,
      searchQueries: searchQueries,
      modelUsed: model
    });

    /* ───────── Fact Checker & Merge Logic ───────── */
    logger.info(`[cf12] Running BOM Fact Checker...`);
    let checkedAnswer = "";
    try {
      checkedAnswer = await bomFactChecker(
        vGenerationConfig.systemInstruction.parts[0].text,
        initialPrompt,
        finalAnswer,
        Array.from(collectedUrls)
      );
    } catch (err) {
      logger.error(`[cf12] Fact checker failed. Using original answer.`, err);
    }

    const originalItems = parseBom(finalAnswer);
    const correctedItems = checkedAnswer ? parseBom(checkedAnswer) : [];
    const finalItems = [];

    // Map corrections by index for easy lookup
    const correctionMap = new Map();
    correctedItems.forEach(item => {
      if (item.index) correctionMap.set(item.index, item);
    });

    for (const original of originalItems) {
      const correction = correctionMap.get(original.index);
      if (correction) {
        // If correction says "N/A" (or similar), we skip this item (delete it)
        const isNA = (val) => val && typeof val === 'string' && val.toUpperCase() === "N/A";

        if (isNA(correction.mat) || isNA(correction.supp) || isNA(correction.desc) || isNA(correction.mass)) {
          logger.info(`[cf12] Item ${original.index} removed by fact checker.`);
          continue;
        }

        // Apply updates
        const updatedItem = { ...original };
        updatedItem.mat = correction.mat;
        updatedItem.supp = correction.supp;
        updatedItem.desc = correction.desc;
        updatedItem.mass = correction.mass;
        updatedItem.unit = correction.unit;

        finalItems.push(updatedItem);
        logger.info(`[cf12] Item ${original.index} updated by fact checker.`);
      } else {
        // No correction found, keep original
        finalItems.push(original);
      }
    }

    const materialsNewList = [];
    for (const p of finalItems) {
      const pDocMassInfo = (pDoc.mass && pDoc.mass_unit) ? ` [${pDoc.mass} ${pDoc.mass_unit}]` : "";
      const pDocCfInfo = (typeof pDoc.supplier_cf === 'number') ? ` [official CF provided by the manufacturer / supplier = ${pDoc.supplier_cf} kgCO2e]` : "";
      const parentInfoParts = [];
      const parentSupplierName = pDoc.manufacturer_name || pDoc.supplier_name;
      if (parentSupplierName) {
        parentInfoParts.push(`[Supplier Name: ${parentSupplierName}]`);
      }

      if (pDoc.supplier_address && pDoc.supplier_address !== "Unknown") {
        parentInfoParts.push(`[Manufacturer / Supplier Address: ${pDoc.supplier_address}]`);
      } else if (pDoc.country_of_origin && pDoc.country_of_origin !== "Unknown") {
        if (pDoc.coo_estimated === true) {
          parentInfoParts.push(`[Estimated Country of Origin: ${pDoc.country_of_origin}]`);
        } else {
          parentInfoParts.push(`[Country of Origin: ${pDoc.country_of_origin}]`);
        }
      }
      const parentInfoString = parentInfoParts.length > 0 ? ` ${parentInfoParts.join(' ')}` : "";
      const newProductChain = `${pDoc.name}${pDocMassInfo}${pDocCfInfo}${parentInfoString} -> ${p.mat}`;

      const newPmChain = [{
        documentId: pRef.id,
        material_or_product: "Product",
        tier: 0
      }];

      const newMatRef = await db.collection("materials").add({
        name: p.mat,
        supplier_name: p.supp,
        description: p.desc,
        linked_product: pRef,
        tier: 1,
        mass: p.mass ?? null,
        mass_unit: p.unit,
        product_chain: newProductChain,
        pmChain: newPmChain,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        estimated_cf: 0,
        total_cf: 0,
        transport_cf: 0,
        apcfIMaterialsNum: 0,
        completed_cf: false,
        final_tier: false,
        software_or_service: false,
        //...
        child_materials_progress: [
          { cloudfunction: 'cf12', number_done: 0 },
          { cloudfunction: 'cf12', number_done: 0 },
          { cloudfunction: 'cf12', number_done: 0 },
          { cloudfunction: 'cf12', number_done: 0 },
          { cloudfunction: 'cf12', number_done: 0 }
        ]
      });
      materialsNewList.push(newMatRef.id);
    }

    logger.info(`[cf12] Scheduled next status check for product ${productId}.`);

    await pRef.update({
      apcfBOM_done: true,
      apcfIMaterialsNum: materialsNewList.length
    });

    // --- MOVED: Logic from end of original function to here to persist AI history ---
    if (collectedUrls.size) {
      await saveURLs({
        urls: Array.from(collectedUrls),
        productId,
        pBOMData: true,
        sys: BOM_SYS,
        user: initialPrompt,
        thoughts: logForReasoning,
        answer: finalAnswer,
        cloudfunction: 'apcfInitial2',
      });
      logger.info(`[cf12] 🔗 Saved ${collectedUrls.size} unique URL(s) to Firestore`);
    }

    await persistHistory({ docRef: pRef, history, loop: (pDoc.ai_loop || 0) + 1, wipeNow: true });
    // --------------------------------------------------------------------------------

    // Trigger cf13 asynchronously
    const finalUrl = `https://${REGION}-${process.env.GCP_PROJECT_ID || 'projectId'}.cloudfunctions.net/cf13`;

    logger.info(`[cf12] Triggering cf13...`);

    // Fire and forget fetch using simple fetch
    fetch(finalUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ productId, materialsList: materialsNewList })
    }).catch(e => logger.warn("[cf12] Async trigger warning (expected if timeout)", e.message));

    res.json({ status: "ok", docId: productId, triggered: "cf13" });

  } catch (err) {
    logger.error("[cf12] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});