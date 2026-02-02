//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf1 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf1] Invoked");
  try {
    /******************** 1. Argument validation ********************/
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

    /******************** 2. AI Call: Product CBAM Scope Check ********************/
    const SYS_CBAM_PRODUCT = "...";

    const vGenerationConfigProduct = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 32768
      },
    };

    const { answer: productAnswer, thoughts: productThoughts, cost: productCost, totalTokens: productTokens, searchQueries: productQueries, model: productModel, rawConversation: productRawConvo } = await runGeminiStream({
      model: 'aiModel', //pro
      generationConfig: vGenerationConfigProduct,
      user: userQueryProduct,
    });

    await logAITransaction({
      cfName: 'cf1-ProductScope',
      productId: productId,
      cost: productCost,
      totalTokens: productTokens,
      searchQueries: productQueries,
      modelUsed: productModel
    });

    await logAIReasoning({
      sys: SYS_CBAM_PRODUCT,
      user: userQueryProduct,
      thoughts: productThoughts,
      answer: productAnswer,
      cloudfunction: 'cf1-ProductScope',
      productId: productId,
      rawConversation: productRawConvo
    });

    /******************** 3. Process AI Response & Update Product ********************/
    const productCBAMInfo = parseProductCBAM(productAnswer);

    const updatePayload = {
      cbam_in_scope: productCBAMInfo.inScope,
      cbam_in_scope_reasoning: productCBAMInfo.reasoning,
      cn_code: productCBAMInfo.cnCode,
      cbam_est_cost: productCBAMInfo.estCost,
      carbon_price_paid: productCBAMInfo.carbonPrice,
    };
    await pRef.update(updatePayload);

    if (productCBAMInfo.inScope !== true) {
      logger.info(`[cf1] Product ${productId} is not in scope for CBAM. Ending function.`);
      res.json("Done");
      return;
    }

    /******************** 4. Fetch Materials & Check Their Scope ********************/
    const materialsSnap = await db.collection("materials").where("linked_product", "==", pRef).get();
    if (materialsSnap.empty) {
      logger.info(`[cf1] Product ${productId} is in scope but has no materials. Ending function.`);
      res.json("Done");
      return;
    }

    const materialLines = materialsSnap.docs.map((doc, i) => {
      const data = doc.data();
      return `material_${i + 1}: ${data.name || 'Unknown'}\nmaterial_description_${i + 1}: ${data.description || 'No description'}`;
    }).join("\n\n");

    const SYS_CBAM_MATERIALS = "...";

    const vGenerationConfigMaterials = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 32768
      },
    };

    const { answer: materialAnswer, thoughts: materialThoughts, cost: materialCost, totalTokens: materialTokens, searchQueries: materialQueries, model: materialModel, rawConversation: materialRawConvo } = await runGeminiStream({
      model: 'aiModel', //pro
      generationConfig: vGenerationConfigMaterials,
      user: materialLines,
    });

    await logAITransaction({
      cfName: 'cf1-MaterialScope',
      productId: productId,
      cost: materialCost,
      totalTokens: materialTokens,
      searchQueries: materialQueries,
      modelUsed: materialModel,
    });

    await logAIReasoning({
      sys: SYS_CBAM_MATERIALS,
      user: materialLines,
      thoughts: materialThoughts,
      answer: materialAnswer,
      cloudfunction: 'cf1-MaterialScope',
      productId: productId,
      rawConversation: materialRawConvo,
    });

    /******************** 5. Update In-Scope Materials & Trigger Next Steps ********************/
    const inScopeMaterials = parseMaterialCBAM(materialAnswer);
    if (inScopeMaterials.length === 0) {
      logger.info(`[cf1] No child materials for product ${productId} are in scope for CBAM.`);
      res.json("Done");
      return;
    }

    const nameToDocMap = new Map(materialsSnap.docs.map(doc => [doc.data().name, doc]));
    const batch = db.batch();
    const msDocs = [];

    for (const material of inScopeMaterials) {
      const docToUpdate = nameToDocMap.get(material.name);
      if (docToUpdate) {
        batch.update(docToUpdate.ref, { cn_code: material.cn_code });
        msDocs.push(docToUpdate);
      }
    }
    await batch.commit();
    logger.info(`[cf1] Updated ${msDocs.length} materials with CN codes.`);

    const promises = [callCF("cf4", { productId })];

    for (const doc of msDocs) {
      const data = doc.data();
      const materialId = doc.id;
      if (data.final_tier === true) {
        promises.push(callCF("cf3", { productId, materialId }));
      } else {
        promises.push(callCF("cf2", { productId, materialId }));
      }
    }

    await Promise.all(promises);
    logger.info(`[cf1] All subsequent CBAM functions have been triggered and completed for product ${productId}.`);

    /******************** 6. Finalize ********************/
    res.json("Done");

  } catch (err) {
    logger.error("[cf1] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});