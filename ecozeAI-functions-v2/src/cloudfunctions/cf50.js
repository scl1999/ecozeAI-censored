//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf50 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf50] Invoked");
  try {
    /******************** 1. Argument validation & Setup ********************/
    const { productId, materialId, calculationLabel } = req.body;
    const entityType = productId ? 'product' : 'material';

    if ((!productId && !materialId) || (productId && materialId) || !calculationLabel) {
      res.status(400).json({ error: "Provide a calculationLabel and exactly one of productId OR materialId" });
      return;
    }

    let cf_value = null;
    let query = "";
    let uncertaintyTargetRef = null; // Ref for the final pn_uncertainty doc
    let materialRefForPayload = null;

    let dataType;
    switch (calculationLabel) {
      case "cf49":
        dataType = "Transport";
        break;
      case "cf15":
        dataType = "mpcf";
        break;
      case "cf21":
        dataType = "mpcfp";
        break;
      default:
        throw new Error(`Invalid calculationLabel for URL lookup: ${calculationLabel}`);
    }

    const responseMarker = "Response:";

    /******************** 1a. Superseded Uncertainty Cleanup ********************/
    if (calculationLabel === "cf21") {
      logger.info(`[cf50] Running cleanup for superseded 'cf15' uncertainty.`);

      let pRefForCleanup;
      let mRefForCleanup = null;
      let pDataForCleanup = {};

      if (productId) {
        pRefForCleanup = db.collection("products_new").doc(productId);
        const pSnap = await pRefForCleanup.get();
        pDataForCleanup = pSnap.data() || {};
      } else { // materialId is present
        mRefForCleanup = db.collection("materials").doc(materialId);
        const mSnap = await mRefForCleanup.get();
        const mData = mSnap.data() || {};
        if (mData.linked_product) {
          pRefForCleanup = mData.linked_product;
          const pSnap = await pRefForCleanup.get();
          pDataForCleanup = pSnap.data() || {};
        }
      }

      // Proceed with deletion only if the cf_processing value exists
      if (pRefForCleanup && typeof pDataForCleanup.cf_processing === 'number') {
        let uncertaintyQuery = pRefForCleanup.collection("pn_uncertainty")
          .where("cloudfunction", "==", "cf15");

        // Add the material constraint depending on which ID was passed
        if (materialId) {
          uncertaintyQuery = uncertaintyQuery.where("material", "==", mRefForCleanup);
        } else {
          uncertaintyQuery = uncertaintyQuery.where("material", "==", null);
        }

        const oldUncertaintySnap = await uncertaintyQuery.get();
        if (!oldUncertaintySnap.empty) {
          const batch = db.batch();
          oldUncertaintySnap.docs.forEach(doc => {
            batch.delete(doc.ref);
          });
          await batch.commit();
          logger.info(`[cf50] Deleted ${oldUncertaintySnap.size} old 'cf15' uncertainty doc(s).`);
        }
      } else {
        logger.warn(`[cf50] Skipping cleanup because cf_processing is not set.`);
      }
    }

    /******************** 2. Data Fetching & Prompt Construction ********************/
    if (productId) {
      const pRef = db.collection("products_new").doc(productId);
      uncertaintyTargetRef = pRef;

      const pSnap = await pRef.get();
      if (!pSnap.exists) throw new Error(`Product ${productId} not found`);
      const pData = pSnap.data() || {};

      const dataSnap = await pRef.collection("pn_data").where("type", "==", dataType).get();
      const urls = dataSnap.docs.map(doc => doc.data().url).filter(Boolean);

      if (calculationLabel === "cf49") {
        cf_value = pData.transport_cf || 0;
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

        query = `Emissions Total (kgCO2e): ${cf_value}\n\nSub Calculation(s):\n${transportLines}\n\nCalculation(s) Reasoning:\n${reasoningLines}`;
      } else { // Handles cf15 and cf21
        cf_value = calculationLabel === "cf15" ? pData.cf_full || 0 : pData.cf_processing || 0;
        const reasoningSnap = await pRef.collection("pn_reasoning").where("cloudfunction", "==", calculationLabel).orderBy("createdAt", "desc").limit(1).get();
        if (reasoningSnap.empty) throw new Error(`No reasoning doc found for ${calculationLabel} on product ${productId}`);

        const original = reasoningSnap.docs[0].data().reasoningOriginal || "";
        const index = original.indexOf(responseMarker);
        const reasoning = index !== -1 ? original.substring(index + responseMarker.length).trim() : original;

        query = `Emissions Total (kgCO2e): ${cf_value}\n\nCalculation Reasoning:\n${reasoning}`;
      }

      if (urls.length > 0) {
        query += `\n\nData Source URLs:\n${urls.join('\n')}`;
      }

      if (pData.est_mass === true) {
        logger.info(`[cf50] Product ${productId} has an estimated mass. Fetching extra reasoning and data.`);

        // 1. & 2. Find and add mass reasoning
        const massReasoningSnap = await pRef.collection("pn_reasoning")
          .where("cloudfunction", "==", "cf33")
          .orderBy("createdAt", "desc")
          .limit(1)
          .get();

        if (!massReasoningSnap.empty) {
          const reasoningDoc = massReasoningSnap.docs[0].data();
          const originalReasoning = reasoningDoc.reasoningOriginal || "";
          const markerIndex = originalReasoning.indexOf(responseMarker);
          if (markerIndex !== -1) {
            const pmassString = originalReasoning.substring(markerIndex + responseMarker.length).trim();
            query += `\n\nMass Reasoning:\n${pmassString}`;
          }
        }

        // 4. & 5. Find and add mass data URLs
        const massDataSnap = await pRef.collection("pn_data")
          .where("type", "==", "Mass")
          .get();

        if (!massDataSnap.empty) {
          const massUrls = massDataSnap.docs.map(doc => doc.data().url).filter(Boolean);
          if (massUrls.length > 0) {
            query += `\n\nMass Data:\n${massUrls.join('\n')}`;
          }
        }
      }

    } else { // materialId must be present
      const mRef = db.collection("materials").doc(materialId);
      materialRefForPayload = mRef;

      const mSnap = await mRef.get();
      if (!mSnap.exists) throw new Error(`Material ${materialId} not found`);
      const mData = mSnap.data() || {};

      if (!mData.linked_product) throw new Error(`Material ${materialId} has no linked_product`);
      uncertaintyTargetRef = mData.linked_product;

      const dataSnap = await mRef.collection("m_data").where("type", "==", dataType).get();
      let urls = dataSnap.docs.map(doc => doc.data().url).filter(Boolean);

      const getCfArUrls = async (docRef, subcollectionName) => {
        const urlSnap = await docRef.collection(subcollectionName).where("type", "==", "CF AR").get();
        if (urlSnap.empty) return [];
        return urlSnap.docs.map(doc => doc.data().url).filter(Boolean);
      };

      // Only add Amend/Review URLs for CF calculations, not transport.
      if (calculationLabel === "cf15" || calculationLabel === "cf21") {
        if (mData.parent_material) {
          // This is a Tier N material, so get URLs from it and its parent material.
          logger.info(`[cf50] Fetching 'CF AR' URLs for Tier N material ${materialId} and its parent.`);
          const mDocCfArUrls = await getCfArUrls(mRef, "m_data");
          const pmDocRef = mData.parent_material;
          const pmDocCfArUrls = await getCfArUrls(pmDocRef, "m_data");
          urls.push(...mDocCfArUrls, ...pmDocCfArUrls);
        } else {
          // This is a Tier 1 material, so get URLs from it and its linked product.
          logger.info(`[cf50] Fetching 'CF AR' URLs for Tier 1 material ${materialId} and its product.`);
          const mDocCfArUrls = await getCfArUrls(mRef, "m_data");
          const pDocRef = mData.linked_product; // Existence already verified
          const pDocCfArUrls = await getCfArUrls(pDocRef, "pn_data");
          urls.push(...mDocCfArUrls, ...pDocCfArUrls);
        }
      }

      if (calculationLabel === "cf49") {
        cf_value = mData.transport_cf || 0;
        const [reasoningSnap, transportSnap, amendReasoningSnap] = await Promise.all([
          mRef.collection("m_reasoning").where("cloudfunction", "==", "cf49").get(),
          mRef.collection("materials_transport").orderBy("leg").get(),
          // START OF NEW LOGIC
          mRef.collection("m_reasoning").where("cloudfunction", "==", "cf5-transport").orderBy("createdAt", "desc").limit(1).get()
          // END OF NEW LOGIC
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

        query = `Emissions Total (kgCO2e): ${cf_value}\n\nSub Calculation(s):\n${transportLines}\n\nCalculation(s) Reasoning:\n${reasoningLines}`;

        // START OF NEW LOGIC
        if (!amendReasoningSnap.empty) {
          const originalAmend = amendReasoningSnap.docs[0].data().reasoningOriginal || "";
          const index = originalAmend.indexOf(responseMarker);
          const atReasoning = index !== -1 ? originalAmend.substring(index + responseMarker.length).trim() : originalAmend;
          if (atReasoning) {
            query += `\n\nCalculation(s) Amendments Reasoning:\n${atReasoning}`;
          }
        }
        // END OF NEW LOGIC

      } else { // Handles cf15 and cf21
        cf_value = calculationLabel === "cf15" ? mData.cf_full || 0 : mData.cf_processing || 0;
        const [reasoningSnap, amendReasoningSnap] = await Promise.all([
          mRef.collection("m_reasoning").where("cloudfunction", "==", calculationLabel).orderBy("createdAt", "desc").limit(1).get(),
          // START OF NEW LOGIC
          calculationLabel === "cf15"
            ? mRef.collection("m_reasoning").where("cloudfunction", "==", "cf5-full").orderBy("createdAt", "desc").limit(1).get()
            : Promise.resolve({ empty: true }) // Don't search for amendments for cf21
          // END OF NEW LOGIC
        ]);

        if (reasoningSnap.empty) throw new Error(`No reasoning doc found for ${calculationLabel} on material ${materialId}`);

        const original = reasoningSnap.docs[0].data().reasoningOriginal || "";
        const index = original.indexOf(responseMarker);
        const reasoning = index !== -1 ? original.substring(index + responseMarker.length).trim() : original;

        query = `Emissions Total (kgCO2e): ${cf_value}\n\nCalculation Reasoning:\n${reasoning}`;

        // START OF NEW LOGIC
        if (!amendReasoningSnap.empty) {
          const originalAmend = amendReasoningSnap.docs[0].data().reasoningOriginal || "";
          const index = originalAmend.indexOf(responseMarker);
          const atReasoning = index !== -1 ? originalAmend.substring(index + responseMarker.length).trim() : originalAmend;
          if (atReasoning) {
            query += `\n\nCalculation(s) Amendments Reasoning:\n${atReasoning}`;
          }
        }
        // END OF NEW LOGIC
      }

      if (urls.length > 0) {
        // Use a Set to ensure URLs are unique before joining
        const uniqueUrls = Array.from(new Set(urls));
        query += `\n\nData Source URLs:\n${uniqueUrls.join('\n')}`;
      }

      if (mData.est_mass === true) {
        logger.info(`[cf50] Material ${materialId} has an estimated mass. Fetching extra reasoning and data.`);

        // 1. & 2. Find and add mass reasoning
        const massReasoningSnap = await mRef.collection("m_reasoning")
          .where("cloudfunction", "==", "cf22")
          .orderBy("createdAt", "desc")
          .limit(1)
          .get();

        if (!massReasoningSnap.empty) {
          const reasoningDoc = massReasoningSnap.docs[0].data();
          const originalReasoning = reasoningDoc.reasoningOriginal || "";
          const markerIndex = originalReasoning.indexOf(responseMarker);
          if (markerIndex !== -1) {
            const massString = originalReasoning.substring(markerIndex + responseMarker.length).trim();
            query += `\n\nMass Reasoning:\n${massString}`;
          }
        }

        // 4. & 5. Find and add mass data URLs
        const massDataSnap = await mRef.collection("m_data")
          .where("type", "==", "Mass")
          .get();

        if (!massDataSnap.empty) {
          const massUrls = massDataSnap.docs.map(doc => doc.data().url).filter(Boolean);
          if (massUrls.length > 0) {
            query += `\n\nMass Data:\n${massUrls.join('\n')}`;
          }
        }
      }

    }

    if (!query) {
      throw new Error(`Invalid calculationLabel: ${calculationLabel}`);
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
        thinkingBudget: 24576,
      },
    };

    const { answer, thoughts, cost, totalTokens, searchQueries, model, rawConversation } = await runGeminiStream({
      model: 'aiModel', //flash
      generationConfig: vGenerationConfig,
      user: query,
    });

    await logAITransaction({
      cfName: 'cf50',
      productId: entityType === 'product' ? productId : uncertaintyTargetRef.id,
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
      cloudfunction: 'cf50',
      productId: productId,
      materialId: materialId,
      rawConversation: rawConversation,
    });

    /******************** 4. Process AI Response & Calculate Uncertainty ********************/
    const scores = parseUncertaintyScores(answer);

    const getBasicUncertaintyFactor = (label) => {
      switch (label) {
        case "cf49": return 2.00;
        case "cf15": case "cf21": return 1.05;
        default: return 1.05;
      }
    };

    const U1 = scores.precision || 1.50;
    const U2 = scores.completeness || 1.20;
    const U3 = scores.temporal || 1.50;
    const U4 = scores.geographical || 1.10;
    const U5 = scores.technological || 2.00;
    const Ub = getBasicUncertaintyFactor(calculationLabel);

    const sumOfSquares =
      Math.pow(Math.log(U1), 2) +
      Math.pow(Math.log(U2), 2) +
      Math.pow(Math.log(U3), 2) +
      Math.pow(Math.log(U4), 2) +
      Math.pow(Math.log(U5), 2) +
      Math.pow(Math.log(Ub), 2);

    const total_uncert = Math.exp(Math.sqrt(sumOfSquares));

    let co2e_uncert_kgco2e = null;
    if (typeof cf_value === 'number' && isFinite(cf_value)) {
      const upperBound = cf_value * total_uncert;
      const lowerBound = cf_value / total_uncert;
      const upperDelta = upperBound - cf_value;
      const lowerDelta = cf_value - lowerBound;
      co2e_uncert_kgco2e = (upperDelta + lowerDelta) / 2;
    }

    /******************** 5. Write to Firestore ********************/
    const payload = {
      co2e_kg: cf_value,
      temporal_rep_score: scores.temporal,
      precision_score: scores.precision,
      completeness_score: scores.completeness,
      geo_rep_score: scores.geographical,
      tech_rep_score: scores.technological,
      uncertainty_reasoning: answer,
      cloudfunction: calculationLabel,
      co2e_uncertainty_kgco2e: co2e_uncert_kgco2e,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    if (materialId && materialRefForPayload) {
      payload.material = materialRefForPayload;
    }

    await uncertaintyTargetRef.collection("pn_uncertainty").add(payload);
    logger.info(`[cf50] Successfully created uncertainty document in ${uncertaintyTargetRef.path}/pn_uncertainty`);

    res.json({ status: "ok", uncertainty_doc_created: true });

  } catch (err) {
    logger.error("[cf50] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});