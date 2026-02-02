//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf33 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  console.log("[cf33] Invoked");

  try {
    // 1. Parse productId
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || "";
    if (!productId.trim()) {
      res.status(400).json({ error: "Missing productId" });
      return;
    }
    console.log(`[cf33] productId = ${productId}`);

    // 2. Fetch product document
    const pRef = db.collection("products_new").doc(productId);
    const pSnap = await pRef.get();
    if (!pSnap.exists) {
      res.status(404).json({ error: `Product ${productId} not found` });
      return;
    }
    const pData = pSnap.data() || {};
    const productName = (pData.name || "").toString().trim();
    const productDescription = (pData.description || "").toString().trim();

    const promptLines = [`Product Name: ${productName}`];
    if (productDescription) {
      promptLines.push(`Product Description: ${productDescription}`);
    }
    const userPrompt = "...".join('\n');

    console.log(`[cf33] fetched product name = "${productName}"`);
    const collectedUrls = new Set();

    /* helper - parse mass lines */
    const parseExact = txt => {
      const m = txt.match(/.*/);
      const u = txt.match(/.*/);
      if (!m || !u) return null;
      return { v: parseFloat(m[1].replace(/.*/, "")), unit: u[1].toLowerCase() };
    };

    const parseEst = txt => {
      const m = txt.match(/.*/);
      const u = txt.match(/.*/);
      const r = txt.match(/.*/);
      if (!m || !u || !r) return null;
      return {
        v: parseFloat(m[1].replace(/.*/, "")),
        unit: u[1].toLowerCase(),
        why: r[1].trim()
      };
    };

    // 3. Build system message for the FIRST attempt (exact mass)
    const SYS_MSG_1 = "..."
    const vGenerationConfig1 = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 32768
      },
    };

    logger.info(
      `[cf33] ▶️ Starting model escalation for EXACT mass: 'aiModel' -> 'aiModel'`
    );

    const { answer: exactAnswer, thoughts: thoughts1, cost: cost1, flashTks: flashTks1, proTks: proTks1, searchQueries: searchQueries1, modelUsed: model1, rawConversation: rawConversation1 } = await runGeminiWithModelEscalation({
      primaryModel: 'aiModel',
      secondaryModel: 'aiModel',
      generationConfig: vGenerationConfig1,
      user: userPrompt,
      collectedUrls,
      cloudfunction: 'cf33'
    });

    // Log the cost of the first attempt
    await logAITransaction({
      cfName: 'cf33',
      productId: productId,
      cost: cost1,
      flashTks: flashTks1,
      proTks: proTks1,
      searchQueries: searchQueries1,
      modelUsed: model1,
    });

    await logAIReasoning({
      sys: SYS_MSG_1,
      user: userPrompt,
      thoughts: thoughts1,
      answer: exactAnswer,
      cloudfunction: 'cf33',
      productId: productId,
      rawConversation: rawConversation1,
    });

    const exact = parseExact(exactAnswer);

    if (exact) {
      await pRef.update({ mass: exact.v, mass_unit: exact.unit, apcfProductTotalMass_done: true });
      if (collectedUrls.size) {
        await saveURLs({
          urls: Array.from(collectedUrls),
          productId,
          pMassData: true,
          sys: SYS_MSG_1,
          user: userPrompt,
          thoughts: thoughts1,
          answer: exactAnswer,
          cloudfunction: 'cf33',
        });
      }
      res.json("Done");
      return;
    }

    // 4. If exact mass is not found, proceed to ESTIMATION
    logger.warn("[cf33] Exact mass not found. Proceeding to estimation.");

    const SYS_MSG_2 = "..."

    const vGenerationConfig2 = {
//
//
//
//
//
      },
    };

    logger.info(`[cf33] ▶️ Starting single-pass for ESTIMATED mass: 'aiModel'`);

    // For estimation, we can go straight to the more powerful model.
    const { answer: estAnswer, thoughts: thoughts2, cost: cost2, totalTokens: tokens2, searchQueries: searchQueries2, model: model2, rawConversation: rawConversation2 } = await runGeminiStream({
      model: 'aiModel', //flash3
      generationConfig: vGenerationConfig2,
      user: userPrompt,
      collectedUrls
    });

    // Log the cost of the second (estimation) attempt
    await logAITransaction({
      cfName: 'cf33',
      productId: productId,
      cost: cost2,
      proTks: tokens2,
      searchQueries: searchQueries2, // This call only uses the pro model
      modelUsed: model2,
    });

    await logAIReasoning({
      sys: SYS_MSG_2,
      user: userPrompt,
      thoughts: thoughts2,
      answer: estAnswer,
      cloudfunction: 'cf33',
      productId: productId,
      rawConversation: rawConversation2,
    });

    const est = parseEst(estAnswer);

    if (est) {
      await pRef.update({
        mass: est.v,
        mass_unit: est.unit,
        est_mass: true,
        apcfProductTotalMass_done: true
      });
    } else {
      // If even estimation fails, mark as done to prevent loops.
      await pRef.update({ apcfProductTotalMass_done: true });
    }

    if (collectedUrls.size) {
      await saveURLs({
        urls: Array.from(collectedUrls),
        productId,
        pMassData: true,
        sys: SYS_MSG_2,
        user: userPrompt,
        thoughts: thoughts2,
        answer: estAnswer,
        cloudfunction: 'cf33',
      });
    }

    res.json("Done");

  } catch (err) {
    console.error("[cf33] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});