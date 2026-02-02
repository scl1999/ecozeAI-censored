//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf8 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf8] Invoked");

  try {
    /******************** 1. Argument validation ********************/
    const { materialsNewList, productId, materialId } = req.body;
    const entityType = productId ? 'product' : 'material';

    if (!Array.isArray(materialsNewList) || materialsNewList.length === 0 ||
      (!productId && !materialId) || (productId && materialId)) {
      res.status(400).json({ error: "Provide a materialsNewList array and exactly one of productId OR materialId" });
      return;
    }

    /******************** 2. Fetch Parent Doc & Data ********************/
    let parentRef;
    let parentName = "", parentMass = "Unknown", parentSupplyChain = "", ecf = "Unknown", scf = "", parentDescription = "", picf = "Unknown";

    if (productId) {
      parentRef = db.collection("products_new").doc(productId);
      const pSnap = await parentRef.get();
      if (!pSnap.exists) {
        res.status(404).json({ error: `Product ${productId} not found` });
        return;
      }
      const pData = pSnap.data() || {};
      parentName = pData.name || "Unknown";
      parentDescription = pData.description || "No description provided.";
      if (pData.mass && pData.mass_unit) parentMass = `${pData.mass} ${pData.mass_unit}`;
      if (typeof pData.supplier_cf === 'number') scf = `${pData.supplier_cf} kgCO2e`;
      if (typeof pData.estimated_cf === 'number') ecf = `${pData.estimated_cf} kgCO2e`;
      const cf_full = pData.cf_full || 0;
      const transport_cf = pData.transport_cf || 0;
      const total_cf = cf_full + transport_cf;
      if (total_cf > 0) {
        picf = `${total_cf} kgCO2e`;
      }
    } else { // materialId must be present
      parentRef = db.collection("materials").doc(materialId);
      const mpSnap = await parentRef.get();
      if (!mpSnap.exists) {
        res.status(404).json({ error: `Material ${materialId} not found` });
        return;
      }
      const mpData = mpSnap.data() || {};
      parentName = mpData.name || "Unknown";
      parentDescription = mpData.description || "No description provided.";
      parentSupplyChain = mpData.product_chain || "";
      if (mpData.mass && mpData.mass_unit) parentMass = `${mpData.mass} ${mpData.mass_unit}`;
      if (typeof mpData.estimated_cf === 'number') ecf = `${mpData.estimated_cf} kgCO2e`;
      const cf_full = mpData.cf_full || 0;
      const transport_cf = mpData.transport_cf || 0;
      const total_cf = cf_full + transport_cf;
      if (total_cf > 0) {
        picf = `${total_cf} kgCO2e`;
      }
    }

    /******************** 2b. Fetch Initial CF Reasoning ********************/
    let aiCFFullReasoning = "";
    const reasoningMarker = "Reasoning:";

    try {
      let reasoningQuery;
      if (productId) {
        reasoningQuery = parentRef.collection("pn_reasoning")
          .where("cloudfunction", "==", "cf15")
          .orderBy("createdAt", "desc")
          .limit(1);
      } else { // materialId must be present
        reasoningQuery = parentRef.collection("m_reasoning")
          .where("cloudfunction", "==", "cf15")
          .orderBy("createdAt", "desc")
          .limit(1);
      }

      const reasoningSnap = await reasoningQuery.get();
      if (!reasoningSnap.empty) {
        const reasoningDoc = reasoningSnap.docs[0].data();
        const originalReasoning = reasoningDoc.reasoningOriginal || "";
        const markerIndex = originalReasoning.indexOf(reasoningMarker);
        if (markerIndex !== -1) {
          aiCFFullReasoning = originalReasoning.substring(markerIndex + reasoningMarker.length).trim();
          logger.info(`[cf8] Successfully extracted reasoning from cf15.`);
        }
      } else {
        logger.warn(`[cf8] No 'cf15' reasoning document found.`);
      }
    } catch (error) {
      logger.error("[cf8] Error fetching reasoning document:", error);
    }

    /******************** 3. Build Prompt from BoM ********************/
    const materialDocs = await Promise.all(
      materialsNewList.map(id => db.collection("materials").doc(id).get())
    );

    const materialNameIdMap = new Map();
    const bomLines = materialDocs.map((doc, index) => {
      if (!doc.exists) return "";
      const data = doc.data() || {};
      const name = data.name || "Unknown";
      const description = data.description || "No description provided.";
      const mass = (data.mass && data.mass_unit) ? `${data.mass} ${data.mass_unit}` : "Unknown";
      const calculated_cf = (typeof data.cf_full === 'number') ? `${data.cf_full} kgCO2e` : "Unknown";
      const transport_cf = (typeof data.transport_cf === 'number') ? `${data.transport_cf} kgCO2e` : "Unknown";

      materialNameIdMap.set(name, doc.id);

      const detailLines = [
        `material_${index + 1}_name: ${name}`,
        `material_${index + 1}_description: ${description}`,
        `material_${index + 1}_supplier_name: ${data.supplier_name || 'Unknown'}`,
      ];

      if (data.supplier_address && data.supplier_address !== "Unknown") {
        detailLines.push(`material_${index + 1}_assembly_address: ${data.supplier_address}`);
      } else if (data.country_of_origin && data.country_of_origin !== "Unknown") {
        if (data.coo_estimated === true) {
          detailLines.push(`material_${index + 1}_estimated_coo: ${data.country_of_origin}`);
        } else {
          detailLines.push(`material_${index + 1}_coo: ${data.country_of_origin}`);
        }
      }

      detailLines.push(`material_${index + 1}_mass: ${mass}`);
      detailLines.push(`material_${index + 1}_calculated_cf: ${calculated_cf}`);
      detailLines.push(`material_${index + 1}_transport_cf: ${transport_cf}`);

      return detailLines.join('\n');
    }).filter(Boolean).join("\n\n");

    let userPrompt = "...";
    if (aiCFFullReasoning) {
      userPrompt += `\nParent Initial Calculated CF Reasoning:\n${aiCFFullReasoning}`;
    }
    userPrompt += `\nParent Calculated CF: ${ecf}`;
    if (scf) userPrompt += `\nOfficial Manufacturer Disclosed CF: ${scf}`;
    if (parentSupplyChain) userPrompt += `\nParent Supply Chain: ${parentSupplyChain}`;
    userPrompt += `\nProduct Description: ${parentDescription}`;
    userPrompt += `\n\nBill-of-Materials:\n\n${bomLines}`;

    /******************** 4. Define System Prompt & AI Call ********************/
    const sysPrompt = "...";


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

    const followUpPrompt = "..."
    const collectedUrls = new Set(); // Required for the loop function

    const { finalAnswer, history, cost, tokens: totalTokens, searchQueries, model, rawConversation, logForReasoning } = await runChatLoop({
      model: 'aiModel', //pro
      generationConfig: vGenerationConfig,
      initialPrompt: userPrompt,
      followUpPrompt: followUpPrompt,
      maxFollowUps: 3,
      collectedUrls
    });

    await logAITransaction({
      cfName: 'cf8',
      productId: entityType === 'product' ? productId : parentRef.id,
      materialId,
      cost,
      totalTokens,
      searchQueries,
      modelUsed: model,
    });

    await logAIReasoning({
      sys: sysPrompt,
      user: userPrompt,
      thoughts: logForReasoning,
      answer: finalAnswer,
      cloudfunction: 'cf8',
      productId,
      materialId,
      rawConversation: rawConversation,
    });

    if (collectedUrls.size) {
      await saveURLs({
        urls: Array.from(collectedUrls),
        productId,
        materialId,
        pCFAR: !!productId,
        mCFAR: !!materialId,
        sys: sysPrompt,
        user: userPrompt,
        thoughts: logForReasoning,
        answer: finalAnswer,
        cloudfunction: 'cf8',
      });
    }

    /******************** 5. Process AI Response ********************/
    if (finalAnswer.trim().toLowerCase() === "done") {
      logger.info("[cf8] AI confirmed all CFs are correct.");
      await parentRef.update({ apcfCFReview_done: true });
      res.json("Done");
      return;
    }

    const corrections = parseCFCorrections(finalAnswer); // Use the aggregated final answer
    logger.info(`[cf8] AI flagged ${corrections.length} material(s) for CF correction.`);

    if (corrections.length > 0) {
      const amendPromises = corrections.map(correction => {
        const childMaterialId = materialNameIdMap.get(correction.name);
        if (!childMaterialId) {
          logger.warn(`[cf8] Could not find ID for flagged material: "${correction.name}"`);
          return Promise.resolve();
        }

        const amendArgs = {
          childMaterialId: childMaterialId,
          productId: productId,     // Will be null if materialId was passed
          materialId: materialId,   // Will be null if productId was passed
        };

        // Conditionally add reasoning arguments
        if (correction.ccf_reasoning.toLowerCase() !== 'done') {
          amendArgs.reasoningCCF = correction.ccf_reasoning;
        }
        if (correction.tcf_reasoning.toLowerCase() !== 'done') {
          amendArgs.reasoningTCF = correction.tcf_reasoning;
        }

        // Only trigger the function if there is at least one correction to make
        if (amendArgs.reasoningCCF || amendArgs.reasoningTCF) {
          logger.info(`[cf8] Triggering cf5 for child material ${childMaterialId} `);
          return callCF("cf5", amendArgs)
            .catch(err => {
              // Log the final error after retries, but don't crash Promise.all
              logger.error(`[cf8] call to cf5 for material ${childMaterialId} failed permanently: `, err.message);
            });
        } else {
          logger.warn(`[cf8] Material "${correction.name}" was flagged but both reasoning fields were 'Done'.Skipping amend call.`);
          return Promise.resolve();
        }
      });
      await Promise.all(amendPromises);
      logger.info("[cf8] All cf5 calls have completed.");
    }

    await parentRef.update({ apcfCFReview_done: true, updatedAt: admin.firestore.FieldValue.serverTimestamp() });
    res.json("Done");

  } catch (err) {
    logger.error("[cf8] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});


//-----------------------------------------------------------------------------------------------------------------------------------------------------------------
//~