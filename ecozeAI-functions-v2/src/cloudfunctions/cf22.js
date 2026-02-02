//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf22 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  /* 0. ── validate input ─────────────────────────────────────────────── */
  const materialId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || "";
  try {
    if (!materialId.trim()) {
      res.status(400).json({ error: "Missing materialId" });
      return;
    }

    const mRef = db.collection("materials").doc(materialId);
    const mSnap = await mRef.get();
    if (!mSnap.exists) {
      res.status(404).json({ error: `Material ${materialId} not found` });
      return;
    }
    const mData = mSnap.data() || {};
    const mName = (mData.name || "").trim();
    const tier = mData.tier ?? 1;
    const linkedProductId = mData.linked_product ? mData.linked_product.id : null;

    const collectedUrls = new Set();

    /* 1. ── build product-chain string and add peer context ───────── */
    const productChain = mData.product_chain || '(unknown chain)';

    // Use 'let' to allow for appending the peer materials section
    let USER_PROMPT = "...";

    if (typeof mData.mass === 'number' && mData.mass_unit) {
      USER_PROMPT += `\nProduct Mass: ${mData.mass} ${mData.mass_unit}`;
    }

    // --- START: New conditional logic to find and add peer materials ---
    let peerMaterialsSnap;

    // CASE 1: mDoc is a Tier N material (it has a parent_material)
    // Peers are other materials with the SAME parent_material.
    if (mData.parent_material) {
      logger.info(`[cf22] Tier N material detected. Searching for peers with parent: ${mData.parent_material.id}`);
      peerMaterialsSnap = await db.collection("materials")
        .where("parent_material", "==", mData.parent_material)
        .get();
    }
    // CASE 2: mDoc is a Tier 1 material (parent_material is unset)
    // Peers are other Tier 1 materials linked to the SAME product.
    else if (mData.linked_product) {
      logger.info(`[cf22] Tier 1 material detected. Searching for peers linked to product: ${mData.linked_product.id}`);
      peerMaterialsSnap = await db.collection("materials")
        .where("tier", "==", 1)
        .where("linked_product", "==", mData.linked_product)
        .get();
    }

    // If the query ran and found documents, format them for the prompt
    if (peerMaterialsSnap && !peerMaterialsSnap.empty) {
      const peerLines = [];
      let i = 1;
      for (const peerDoc of peerMaterialsSnap.docs) {
        // IMPORTANT: Exclude the current material from its own peer list
        if (peerDoc.id === materialId) {
          continue;
        }
        const peerData = peerDoc.data() || {};
        peerLines.push(
          `...`
        );
        i++;
      }

      if (peerLines.length > 0) {
        USER_PROMPT += "\n\nPeer Materials:\n" + peerLines.join('\n');
      }
    }
    /* helper - parse mass lines */
    const parseExact = txt => {
      const m = txt.match( /.*/);
      const u = txt.match( /.*/);
      if (!m || !u) return null;

      let valStr = m[1];
      // Logging to debug user report of "0" result
      logger.info(`[cf22] Parsing Mass: raw '${valStr}' unit '${u[1]}'`);

      // Handle comma as decimal if needed, but usually AI follows locale. 
      // User input "0.05" implies dot decimal. 
      // Safe removing of comma if used as thousands separator? 
      // "1,000.0" -> "1000.0". 
      // "0,05" -> "005" (5) if treated as thousands separator.
      // Standardize: If comma exists and dot exists, comma is likely thousands.
      // If only comma exists? European decimal?

      let v = parseFloat(valStr.replace( /.*/, ""));
      if (valStr.includes(',') && !valStr.includes('.') && v >= 100) {
        // Ambiguous. "0,05" -> 5? No, replace makes it 005.
        // If "0,05", we might want 0.05.
        // For now, strict float parse.
      }

      // Explicit logger for final value
      logger.info(`[cf22] Parsed Value: ${v}`);

      return { v: v, unit: u[1].toLowerCase() };
    };
    const parseEst = txt => {
      const m = txt.match( /.*/);
      const u = txt.match( /.*/);
      const r = txt.match( /.*/);
      if (!m || !u || !r) return null;
      return {
        v: parseFloat(m[1].replace( /.*/, "")),
        unit: u[1].toLowerCase(),
        why: r[1].trim()
      };
    };

    /* 2. ── first attempt: exact mass ------------------------------------ */
    const SYS_1 = "...";
    let history = [
      { role: "developer", content: [{ type: "input_text", text: SYS_1 }] },
      { role: "user", content: [{ type: "input_text", text: USER_PROMPT }] }
    ];

    const AUX_QUERY_1 = SYS_1 + `  Product: ${mName} | Chain: ${productChain}`;

    const primaryModel = 'aiModel';
    const secondaryModel = 'aiModel';

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

    const { answer: exactAnswer, thoughts: exactThoughts, cost, flashTks, proTks, searchQueries, modelUsed, rawConversation } = await runGeminiWithModelEscalation({
      primaryModel,
      secondaryModel,
      generationConfig: vGenerationConfig,
      user: USER_PROMPT,
      collectedUrls,
      cloudfunction: 'cf22'
    });

    // NEW: Call the new, simpler logger with pre-calculated values
    await logAITransaction({
      cfName: 'cf22',
      productId: linkedProductId,
      materialId: materialId,
      cost,
      flashTks,
      proTks,
      searchQueries: searchQueries,
      modelUsed: modelUsed,
    });

    await logAIReasoning({
      sys: SYS_1,
      user: USER_PROMPT,
      thoughts: exactThoughts,
      answer: exactAnswer,
      cloudfunction: 'cf22',
      materialId: materialId,
      rawConversation: rawConversation,
    });

    const exact = parseExact(exactAnswer);

    if (exact) {
      await mRef.update({ mass: exact.v, mass_unit: exact.unit, apcfMassFinder_done: true });
      /* stash URLs as “mass data” for this material */
      await saveURLs({
        urls: Array.from(collectedUrls),
        materialId,
        productId: linkedProductId,
        mMassData: true,
        sys: SYS_1,
        user: USER_PROMPT,
        thoughts: exactThoughts,
        answer: exactAnswer,
        cloudfunction: 'cf22',
      });
      const updatedMSnap = await mRef.get();
      const updatedMData = updatedMSnap.data() || {};
      if (updatedMData.linked_product && updatedMData.mass && updatedMData.mass_unit) {
        const pRef = updatedMData.linked_product;
        const pSnap = await pRef.get();
        if (pSnap.exists) {
          const pData = pSnap.data() || {};
          if (pData.mass && pData.mass_unit) {
            const convertToGrams = (mass, unit) => {
              if (typeof mass !== 'number' || !unit) return null;
              const u = unit.toLowerCase();
              if (u === 'g') return mass;
              if (u === 'kg') return mass * 1000;
              if (u === 'mg') return mass / 1000;
              if (u === 'lb' || u === 'lbs') return mass * 453.592;
              if (u === 'oz') return mass * 28.3495;
              return null;
            };

            const mMassGrams = convertToGrams(updatedMData.mass, updatedMData.mass_unit);
            const pMassGrams = convertToGrams(pData.mass, pData.mass_unit);

            if (mMassGrams !== null && pMassGrams !== null && pMassGrams > 0) {
              const percentageOPM = (mMassGrams / pMassGrams) * 100;
              await mRef.update({ percentage_of_p_mass: percentageOPM });
              logger.info(`[cf22] Calculated and saved percentage_of_p_mass: ${percentageOPM.toFixed(2)}% for material ${materialId}`);
            }
          }
        }
      }
      await incrementChildProgress(materialId, 'cf22');
      res.json("Done");
      return;
    }

    /* 3. ── second attempt: estimate ------------------------------------ */
    const SYS_2 = "..."

    const AUX_QUERY_2 = SYS_2 + `...`;

    // 2. DEFINE the generation config
    const vGenerationConfig2 = {
//
//
//
//
      // Add the thinkingConfig object here
//
        includeThoughts: true,    // Set to true to request thinking process
        thinkingBudget: 32768     // The token budget for the model to "think"
      },
    };

    const modelUsedForEstimate = 'aiModel'; //flash3

    // CORRECTED: Use aliasing to prevent redeclaring variables
    const { answer: estAnswer, thoughts: estThoughts, cost: costForEstimate, totalTokens: tokensForEstimate, searchQueries: estSearchQueries, model: modelForEstimate, rawConversation: rawConversation1 } = await runGeminiStream({
      model: modelUsedForEstimate,
      generationConfig: vGenerationConfig2,
      user: USER_PROMPT,
      collectedUrls
    });

    // CORRECTED: Use the new variable names in the logger call
    await logAITransaction({
      cfName: 'cf22',
      productId: linkedProductId,
      materialId: materialId,
      cost: costForEstimate,
      totalTokens: tokensForEstimate,
      searchQueries: estSearchQueries,
      modelUsed: modelForEstimate,
    });

    await logAIReasoning({
      sys: SYS_2,
      user: USER_PROMPT,
      thoughts: estThoughts,
      answer: estAnswer,
      cloudfunction: 'cf22',
      materialId: materialId,
      rawConversation: rawConversation1,
    });

    const est = parseEst(estAnswer);

    if (est) {
      await mRef.update({
        mass: est.v,
        mass_unit: est.unit,
        estimated_mass_reasoning: est.why,
        est_mass: true
      });

      const updatedMSnap = await mRef.get();
      const updatedMData = updatedMSnap.data() || {};
      if (updatedMData.linked_product && updatedMData.mass && updatedMData.mass_unit) {
        const pRef = updatedMData.linked_product;
        const pSnap = await pRef.get();
        if (pSnap.exists) {
          const pData = pSnap.data() || {};
          if (pData.mass && pData.mass_unit) {
            const convertToGrams = (mass, unit) => {
              if (typeof mass !== 'number' || !unit) return null;
              const u = unit.toLowerCase();
              if (u === 'g') return mass;
              if (u === 'kg') return mass * 1000;
              if (u === 'mg') return mass / 1000;
              if (u === 'lb' || u === 'lbs') return mass * 453.592;
              if (u === 'oz') return mass * 28.3495;
              return null;
            };

            const mMassGrams = convertToGrams(updatedMData.mass, updatedMData.mass_unit);
            const pMassGrams = convertToGrams(pData.mass, pData.mass_unit);

            if (mMassGrams !== null && pMassGrams !== null && pMassGrams > 0) {
              const percentageOPM = (mMassGrams / pMassGrams) * 100;
              await mRef.update({ percentage_of_p_mass: percentageOPM });
              logger.info(`[cf22] Calculated and saved percentage_of_p_mass: ${percentageOPM.toFixed(2)}% for material ${materialId}`);
            }
          }
        }
      }
    }
    /* always store whatever URLs we gathered, even if mass = Unknown */
    await saveURLs({
      urls: Array.from(collectedUrls),
      materialId,
      productId: linkedProductId,
      mMassData: true,
      sys: SYS_2,
      user: USER_PROMPT,
      thoughts: estThoughts,
      answer: estAnswer,
      cloudfunction: 'cf22',
    });
    await incrementChildProgress(materialId, 'cf22');
    await mRef.update({ apcfMassFinder_done: true });
    res.json("Done");

  } catch (err) {
    console.error("[cf22] Uncaught error:", err);
    await incrementChildProgress(materialId, 'cf22');
    res.status(500).json({ error: String(err) });
  }
});