//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf17 = onRequest({
  region: REGION,
  timeoutSeconds: 3600, // Deep Research needs max time
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  try {
    const materialId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || null;
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || null;

    if ((!materialId && !productId) || (materialId && productId)) {
      res.status(400).json({ error: "Provide exactly one of materialId OR productId" });
      return;
    }

    logger.info(`[cf17] Invoked for ${productId ? 'Product ' + productId : 'Material ' + materialId}`);

    /* ╭── 1. Locate Target Docs & Previous Reasoning ──╮ */
    let targetRef, targetData, reasoningCollection, reasoningQuery;

    if (productId) {
      targetRef = db.collection("products_new").doc(productId);
      reasoningCollection = targetRef.collection("pn_reasoning");
    } else {
      targetRef = db.collection("materials").doc(materialId);
      reasoningCollection = targetRef.collection("m_reasoning");
    }

    const targetSnap = await targetRef.get();
    if (!targetSnap.exists) {
      res.status(404).json({ error: "Target document not found" });
      return;
    }
    targetData = targetSnap.data();

    // Fetch the most recent 'cf15' reasoning
    const rSnap = await reasoningCollection
      .where("cloudfunction", "==", "cf15")
      .orderBy("createdAt", "desc")
      .limit(1)
      .get();

    if (rSnap.empty) {
      res.status(404).json({ error: "No previous cf15 reasoning found. Cannot refine." });
      return;
    }

    const rDoc = rSnap.docs[0];
    const rData = rDoc.data();

    /* ╭── 2. Construct Deep Research Prompt ──╮ */

    // Extract Reasoning *after* "User Prompt:"
    let previousContext = "";
    if (rData.reasoningOriginal) {
      const splitParts = rData.reasoningOriginal.split("User Prompt:");
      if (splitParts.length > 1) {
        // Take everything after the first "User Prompt:"
        // We join back in case "User Prompt:" appeared multiple times, though usually it's once structure.
        // Actually, better to take the *last* part if structure is fixed, but let's take everything after expectation.
        // The structure is System... User Prompt:... Response:...
        previousContext = splitParts.slice(1).join("User Prompt:");
      } else {
        previousContext = rData.reasoningOriginal; // Fallback
      }
    }

    const userPromptPart = "...";

    /* ╭── 3. Execute Deep Research Agent ──╮ */
    logger.info(`[cf17] Starting Deep Research Session...`);

    const deepResearchPayload = {
      agent: 'deep-research-pro-preview-12-2025',
      input: `System Instruction: ${ SYS_MSG_MPCFFULL_CORE }\n\nUser Request: ${ userPromptPart }`,
      background: true, // Run in background mode if supported/needed
      stream: true,
      store: true,
      agent_config: {
        type: 'deep-research',
        thinking_summaries: 'auto'
      }
    };

    // Shared State for Interaction
    let interactionId = null;
    let lastEventId = null;
    let isComplete = false;
    let drFinalAnswer = "";
    let drFinalOutputs = [];
    let drCollectedUrls = new Set();
    let deepResearchHistory = "";
    let drUsage = null;

    // Stream Handler
    const handleStream = async (readableBody) => {
      for await (const chunk of parseNDJSON(readableBody)) {
        if (chunk.event_type === 'interaction.start') {
          interactionId = chunk.interaction.id;
          logger.info(`[cf17] Interaction Started: ${ interactionId }`);
        }
        if (chunk.event_id) lastEventId = chunk.event_id;

        if (chunk.event_type === 'content.delta') {
          if (chunk.delta.type === 'text') {
            drFinalAnswer += (chunk.delta.text || '');
            deepResearchHistory += (chunk.delta.text || '');
          } else if (chunk.delta.type === 'thought_summary') {
            deepResearchHistory += `\n[Thought] ${ chunk.delta.content?.text }\n`;
          }
        } else if (chunk.event_type === 'interaction.complete') {
          isComplete = true;
        } else if (chunk.event_type === 'error') {
          const errCode = chunk.error?.code;
          if (errCode === 'deadline_exceeded' || errCode === 'gateway_timeout') {
            logger.warn(`[cf17] Stream timeout(expected).`);
          } else if (errCode === 13 || (chunk.error?.message && chunk.error.message.includes("BROWSE_URL_STATUS"))) {
            logger.error(`[cf17] Browse Error: ${ chunk.error?.message }`);
            isComplete = true;
          } else {
            logger.error(`[cf17] Stream Error: ${ JSON.stringify(chunk) }`);
          }
        }

        if (chunk.interaction && chunk.interaction.outputs) {
          const extracted = extractUrlsFromInteraction(chunk.interaction.outputs);
          extracted.forEach(u => drCollectedUrls.add(u));
        }
      }
    };

    // Start Interaction
    const streamBody = await createInteraction(deepResearchPayload, true);
    await handleStream(streamBody);

    if (!interactionId) throw new Error("Failed to acquire Interaction ID.");

    // Reconnection Loop
    const MAX_RECONNECT_TIME_MS = 3000 * 1000; // 50 mins
    const startTime = Date.now();
    while (!isComplete && interactionId) {
      if (Date.now() - startTime > MAX_RECONNECT_TIME_MS) throw new Error("Timeout waiting for DR Agent");
      try {
        const sBody = await getInteraction(interactionId, { stream: true, last_event_id: lastEventId });
        await handleStream(sBody);
        if (!isComplete) await sleepAI(2000);
      } catch (e) {
        logger.warn(`[cf17] Reconnect fail: ${ e.message }`);
        await sleepAI(5000);
      }
    }

    // Capture Final Outputs & Usage
    try {
      const iObj = await getInteraction(interactionId);
      if (iObj.status === 'completed' && iObj.outputs) {
        drFinalOutputs = iObj.outputs;
        // Ensure we have the absolute final text
        const txt = drFinalOutputs.filter(o => o.type === 'text');
        if (txt.length) drFinalAnswer = txt[txt.length - 1].text;
        drUsage = iObj.usage;
      }
    } catch (e) {
      logger.error(`[cf17] Failed to fetch final interaction state: ${ e.message }`);
    }

    /* ╭── 4. Process Results & Update Firestore ──╮ */

    // Parse CF Value
    const newCfValue = ((txt) => {
      // Handle: *cf_value = 201* or cf_value = 201
      const m = txt.match( /.*/);
      if (!m) return null;
      // Remove commas, then parse
      const n = parseFloat(m[1].replace( /.*/, ""));
      return isFinite(n) ? n : null;
    })(drFinalAnswer);

    if (newCfValue !== null) {
      logger.info(`[cf17] ✅ DR Refinement Succeeded.New Value: ${ newCfValue }`);

      const batch = db.batch();

      // 4a. Archive Old Reasoning
      batch.update(rDoc.ref, { cloudfunction: "cf15-original" });

      // 4b. Update Main Document
      const currentCfFull = targetData.cf_full || targetData.estimated_cf || 0;
      const updatePayload = {
        cf_full_original: currentCfFull,
        cf_full: newCfValue,
        cf_full_refined: newCfValue,
        estimated_cf: newCfValue, // Overwrite estimated
        apcfMPCFFullDR_done: true,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };
      batch.update(targetRef, updatePayload);

      // 4c. Propagate to Parent Chain (Materials Only)
      if (materialId) {
        const pmChain = targetData.pmChain || [];
        // Calculate Delta: New - Old
        // NOTE: We assume 'estimated_cf' on the material was the contribution to the parent.
        const delta = newCfValue - currentCfFull;

        if (delta !== 0) {
          logger.info(`[cf17] Propagating delta ${ delta } to ${ pmChain.length } parents.`);
          for (const link of pmChain) {
            if (!link.documentId) continue;
            const pRef = link.material_or_product === "Product"
              ? db.collection("products_new").doc(link.documentId)
              : db.collection("materials").doc(link.documentId);
            batch.update(pRef, { estimated_cf: admin.firestore.FieldValue.increment(delta) });
          }
        }
      }

      await batch.commit();
      logger.info(`[cf17] Firestore Batch Update Committed.`);

    } else {
      logger.warn(`[cf17] ⚠️ DR Agent did not return a valid cf_value.No updates made.Content: ${ drFinalAnswer?.substring(0, 500) } `);
      // Still mark done? User instruction didn't specify failure case. 
      // Safest to NOT mark done so retry is possible, OR mark done to prevent infinite loops.
      // Based on instruction "Set the /apcfMPCFFullDR_done ... to true", implies always done if it ran.
      await targetRef.update({ apcfMPCFFullDR_done: true });
    }

    /* ╭── 5. Logging ──╮ */

    // Log URLS
    if (drCollectedUrls.size > 0) {
      await saveURLs({
        urls: Array.from(drCollectedUrls),
        productId,
        materialId,
        pMPCFData: !!productId,
        mMPCFData: !!materialId,
        sys: SYS_MSG_MPCFFULL_CORE, // Using the 'Core' sys msg as derived from step 2 construction
        user: `${ userPromptPart } `,
        thoughts: deepResearchHistory, // DR History acts as thoughts/process
        answer: drFinalAnswer,
        cloudfunction: 'cf17' // User requested this name
      });
    }

    // Log Transaction (Usage)
    // Convert DR Usage to Log Format
    let cost = 0;
    let totalTokens = { input: 0, output: 0, toolCalls: 0 };
    if (drUsage) {
      totalTokens.input = drUsage.total_input_tokens || 0;
      totalTokens.output = drUsage.total_output_tokens || 0;
      totalTokens.toolCalls = drUsage.total_tool_use_tokens || 0;

      // Calculate Cost manually (Gemini 3 Pro pricing proxy for DR preview?)
      // DR pricing is complex. Fallback to model scalar logic used in logAITransactionI or standard.
      // User normally uses calculated fields. I'll rely on a safe approximation or 0 if unknown.
      // Using 'aiModel' pricing logic from helper:
      cost = calculateCost('aiModel', {
        input: totalTokens.input,
        output: totalTokens.output,
        toolCalls: totalTokens.toolCalls // Note: Deep Research usually includes search costs
      });
    }

    await logAITransaction({
      cfName: 'cf17', // User requested this name
      productId,
      materialId,
      cost,
      totalTokens: totalTokens.input + totalTokens.output, // Passing scalar for standard log
      modelUsed: 'deep-research-pro-preview-12-2025'
    });

    // Log Reasoning
    await logAIReasoning({
      sys: SYS_MSG_MPCFFULL_CORE,
      user: `${ userPromptPart } `,
      thoughts: deepResearchHistory,
      answer: drFinalAnswer,
      cloudfunction: 'cf17', // User requested this name
      productId,
      materialId,
      rawConversation: [{ role: 'model', parts: [{ text: drFinalAnswer }] }] // Simplified for compatibility
    });

    res.json("Done");

  } catch (err) {
    logger.error("[cf17] Uncaught Error", err);
    res.status(500).json({ error: err.message });
  }
});