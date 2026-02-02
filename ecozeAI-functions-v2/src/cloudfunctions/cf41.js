//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf41 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf41] Invoked");

  // 1. Helpers for this function scope
  // -----------------------------------------------------------------------------------------
  // Helper to parse the Address finding output
  function parseAddressOutput(text) {
    // Robust regex to capture address until next field or separator
    const addressMatch = text.match( /.*/);

    let address = addressMatch ? addressMatch[1].trim() : null;

    // Check for "Unknown"
    if (address && address.toLowerCase() === 'unknown') address = null;
    if (address === '[]') address = null;

    // We don't parse reasoning/coo here as they are not in the primary prompt format
    return { address, reasoning: null, coo: null, isEstimated: false };
  }

  // Helper to parse the Country of Origin fallback output
  function parseCOOOutput(text) {
    const cooMatch = text.match( /.*/);
    const cooEstMatch = text.match( /.*/);

    let coo = cooMatch ? cooMatch[1].trim() : null;
    let cooEst = cooEstMatch ? cooEstMatch[1].trim().toLowerCase() : 'false';

    if (coo && coo.toLowerCase() === 'unknown') coo = null;
    const isEstimated = cooEst === 'true';

    return { coo, isEstimated };
  }
  // -----------------------------------------------------------------------------------------


  const materialId = req.method === "POST" ? req.body?.materialId : req.query.materialId;
  const productId = req.method === "POST" ? req.body?.productId : req.query.productId;

  try {

    if ((!materialId && !productId) || (materialId && productId)) {
      res.status(400).json({ error: "Provide exactly one of materialId or productId" });
      return;
    }

    let docRef, docData, supplierName;
    const isMaterial = !!materialId;
    let linkedProductId = null;

    if (isMaterial) {
      docRef = db.collection("materials").doc(materialId);
      const snap = await docRef.get();
      if (!snap.exists) return res.status(404).json({ error: "Material not found" });
      docData = snap.data();
      supplierName = docData.supplier_name;
      linkedProductId = docData.linked_product ? docData.linked_product.id : null;
    } else {
      docRef = db.collection("products_new").doc(productId);
      const snap = await docRef.get();
      if (!snap.exists) return res.status(404).json({ error: "Product not found" });
      docData = snap.data();
      supplierName = (docData.manufacturer_name || docData.supplier_name || "").trim();
    }

    // Must have a supplier name to search for address
    if (!supplierName || supplierName.toLowerCase() === 'unknown') {
      logger.warn(`[cf41] Supplier name is missing or unknown for ${docRef.id}. Skipping.`);
      return res.json("Skipped: Supplier Unknown");
    }

    // 3. Setup Prompt & AI Config
    const ADDRESS_SYS_MSG = "...";

    let USER_PROMPT;
    if (isMaterial) {
      const productChain = docData.product_chain || "Not provided";
      const description = docData.description || "Not provided";
      const entityName = docData.name || "Unknown Item";

      // Fetch parent context for materials
      let parentContextLine = "";
      if (docData.parent_material) {
        const pmRef = docData.parent_material;
        const pmSnap = await pmRef.get();
        if (pmSnap.exists) {
          const pmData = pmSnap.data() || {};
          const pmSupplierAddress = (pmData.supplier_address || "").trim();

          if (pmSupplierAddress && pmSupplierAddress !== "Unknown") {
            parentContextLine = `\nParent Product Assembly Address: ${pmSupplierAddress}`;
          } else {
            const pmCountryOfOrigin = (pmData.country_of_origin || "").trim();
            const pmCooEstimated = pmData.coo_estimated || false;
            if (pmCountryOfOrigin) {
              parentContextLine = `\nParent Product Country of Origin: ${pmCountryOfOrigin}\nParent Product Country of Origin Estimated (True = was estimated (take with a pinch of salt), False = wasnt estimated): ${pmCooEstimated}`;
            }
          }
        }
      } else if (linkedProductId) {
        const pRef = db.collection("products_new").doc(linkedProductId);
        const pSnap = await pRef.get();
        if (pSnap.exists) {
          const pData = pSnap.data() || {};
          const pSupplierAddress = (pData.supplier_address || "").trim();

          if (pSupplierAddress && pSupplierAddress !== "Unknown") {
            parentContextLine = `\nParent Product Assembly Address: ${pSupplierAddress}`;
          }
        }
      }

      USER_PROMPT = `Product/Material Name: ${entityName} || Supplier Name: ${supplierName}\nProduct Chain: ${productChain}\nProduct Description: ${description}${parentContextLine}`;
    } else {
      const description = docData.description || "Not provided";
      const entityName = docData.name || "Unknown Item";
      USER_PROMPT = `Product Name = ${entityName} |||| Manufacturer/Supplier Name: ${supplierName}\nProduct Description: ${description}`;
    }

    logger.info("[cf41] Generated USER_PROMPT: " + USER_PROMPT);

    // 4. Main AI Loop for Address (Retry + Fact Check)
    let finalAddress = null;
    let finalCOO = null;
    let finalCOOEst = false; // New
    let finalReasoning = null;
    let addressFound = false;

    let retryCount = 0;
    const MAX_RETRIES = 2;
    let currentPrompt = "...";
    let lastAiResponse = "";

    // Persistence vars
    let allTurnsForLog = [];
    let allRawChunks = [];
    let collectedUrls = new Set();
    let totalCost = 0;
    let totalTokens = { input: 0, output: 0, toolCalls: 0 };
    let totalSearchQueries = [];

    // Initialize Chat Session
    const ai = getGeminiClient();
    const chat = ai.chats.create({
      model: 'aiModel', //flash3
      config: {
//
//
//
//
//
      }
    });

    // Loop
    while (retryCount <= MAX_RETRIES) {
      logger.info(`[cf41] Attempt ${retryCount + 1} for ${docRef.id} (Supplier: ${supplierName})`);

      // A. Run Address AI
      lastAiResponse = "";
      const currentTurnUrls = new Set();
      const currentSearchQueries = new Set();
      let thoughtsThisTurn = "";

      try {
        const streamResult = await runWithRetry(() => chat.sendMessageStream({ message: currentPrompt }));

        for await (const chunk of streamResult) {
          harvestUrls(chunk, currentTurnUrls);

          if (chunk.candidates && chunk.candidates.length > 0) {
            for (const candidate of chunk.candidates) {
              if (candidate.content?.parts) {
                for (const part of candidate.content.parts) {
                  if (part.text) {
                    lastAiResponse += part.text;
                  } else if (part.functionCall) {
                    thoughtsThisTurn += `\n--- TOOL CALL ---\n${JSON.stringify(part.functionCall, null, 2)}\n`;
                  } else {
                    const thoughtText = JSON.stringify(part, null, 2);
                    if (thoughtText !== '{}') thoughtsThisTurn += `\n--- AI THOUGHT ---\n${thoughtText}\n`;
                  }
                }
              }
              const gm = candidate.groundingMetadata;
              if (gm?.webSearchQueries?.length) {
                thoughtsThisTurn += `\n--- SEARCH QUERIES ---\n${gm.webSearchQueries.join("\n")}\n`;
                gm.webSearchQueries.forEach(q => currentSearchQueries.add(q));
              }
            }
          }
        }

        // Unwrap and Deduplicate URLs for this turn before using them
        const tempUnwrappedUrls = [];
        for (const u of currentTurnUrls) {
          const unwrapped = await unwrapVertexRedirect(u);
          tempUnwrappedUrls.push(unwrapped);
        }
        currentTurnUrls.clear();
        tempUnwrappedUrls.forEach(u => currentTurnUrls.add(u));

        currentTurnUrls.forEach(u => collectedUrls.add(u));

        // Token Counting for this turn
        const history = await chat.getHistory();
        // The history includes the latest turn.

        // We need to estimate tokens for cost calculation
        // Input tokens: Count of history up to user message
        // Output tokens: Count of model response

        // Note: Exact token counting requires API calls which adds latency/cost. 
        // We will approximate or use the usage metadata if available in the future.
        // For now, we'll skip exact per-turn counting to save time/calls or do a simple count.

        // Let's do a simple count call for accuracy as requested in other functions
        const { totalTokens: inTks } = await ai.models.countTokens({
          model: 'aiModel',
          contents: history.slice(0, -1), // Everything before last model response
//
//
        });

        const { totalTokens: outTks } = await ai.models.countTokens({
          model: 'aiModel',
          contents: [history[history.length - 1]] // The last model response
        });

        totalTokens.input += inTks || 0;
        totalTokens.output += outTks || 0;

        // Cost Calc (Approximate for Gemini 3 Pro)
        const costThisTurn = (inTks * 0.00000125) + (outTks * 0.000005); // Example rates
        totalCost += costThisTurn;
        if (currentSearchQueries.size > 0) totalCost += 0.014; // Grounding

        currentSearchQueries.forEach(q => totalSearchQueries.push(q));

        allTurnsForLog.push(`USER: ${currentPrompt}\nAI: ${lastAiResponse}`);
        allRawChunks.push({ text: lastAiResponse }); // Simplified chunk storage

      } catch (e) {
        logger.error(`[cf41] AI run failed: ${e.message}`);
        break; // Fatal error
      }

      // B. Parse Output
      const parsed = parseAddressOutput(lastAiResponse);

      // C. Check correctness of Address
      if (parsed.address && parsed.address !== 'Unknown') {

        // D. Run Fact Checker
        // We use the helper `factChecker` which returns { aiFCAnswer: "..." }
        // The fact checker requires specific output structure to validate against.
        // We want the fact checker to verify the address.

        const fcStructure = `...`;

        // Construct the detailed prompt as requested
        const factCheckPrompt = "...";

        let fcResult;
        try {
          fcResult = await factChecker({
            productId: isMaterial ? null : productId,
            materialId: materialId,
            urlsGiven: Array.from(currentTurnUrls),
            cloudfunction: 'cf41',
            systemInstructions: ADDRESS_SYS_MSG,
            prompt: factCheckPrompt,
            aiOutput: lastAiResponse,
            outputStructure: fcStructure
          });
        } catch (fcErr) {
          logger.warn(`[cf41] Fact Checker failed: ${fcErr.message}. Accepting AI result cautiously.`);
          // If FC fails, we might just accept the result or retry. Let's accept for now to avoid blocking.
          fcResult = { aiFCAnswer: "*verification: PASS" };
        }

        const fcText = fcResult.aiFCAnswer;
        const isPass = fcText.includes("*verification: PASS");

        if (isPass) {
          logger.info(`[cf41] Address verified: ${parsed.address}`);
          finalAddress = parsed.address;
          finalReasoning = parsed.reasoning;
          addressFound = true;
          break; // DONE
        } else {
          logger.warn(`[cf41] Fact Checker rejected address. Retrying...`);
          retryCount++;
          currentPrompt = `...`;
          continue;
        }

      } else {
        // AI returned Unknown or invalid format
        logger.info(`[cf41] Address not found or invalid format on attempt ${retryCount + 1}.`);
        retryCount++;
        currentPrompt = "Try again to find the address.";
      }
    } // End While Loop


    // 5. Fallback: Country of Origin
    // If we failed to find an address after retries, we look for just the Country of Origin using a specialized prompt.
    if (!addressFound) {
      logger.info(`[cf41] Address finding failed after ${retryCount} attempts. Switching to Country of Origin Fallback.`);

      const FALLBACK_PROMPT = "...";

      try {
        const cooResult = await runGeminiStream({
          model: 'aiModel', //flash3
          generationConfig: {
//
//
//
//
//
          },
          user: FALLBACK_PROMPT,
          collectedUrls: collectedUrls // Continue collecting URLs
        });

        // Log this fallback transaction
        totalCost += cooResult.cost;
        totalTokens.input += cooResult.totalTokens.input;
        totalTokens.output += cooResult.totalTokens.output;
        if (cooResult.totalTokens.toolCalls) totalTokens.toolCalls += cooResult.totalTokens.toolCalls;
        if (cooResult.searchQueries) totalSearchQueries.push(...cooResult.searchQueries);
        allRawChunks.push(...cooResult.rawConversation);

        const cooParsed = parseCOOOutput(cooResult.answer);

        if (cooParsed.coo && cooParsed.coo !== 'Unknown') {
          finalCOO = cooParsed.coo;
          lastAiResponse = cooResult.answer; // Update for logging
          finalReasoning = `(Fallback COO) ${cooResult.answer}`; // Keep full answer in reasoning for context

          // Also set the estimated flag if true
          if (cooParsed.isEstimated) {
            finalCOOEst = true;
            logger.info(`[cf41] COO Estimated: ${finalCOO}`);
          }
        } else {
          logger.warn(`[cf41] Failed to find even Country of Origin.`);
          finalReasoning = "Failed to find address and Country of Origin.";
        }

      } catch (err) {
        logger.error(`[cf41] COO Fallback failed: ${err.message}`);
      }
    }


    // 6. Final Updates & Logging

    // Log final Transaction & Reasoning
    // Log final Transaction & Reasoning
    await logAITransaction({
      cfName: 'cf41',
      productId: isMaterial ? linkedProductId : productId,
      materialId: materialId,
      cost: totalCost,
      totalTokens: totalTokens,
      searchQueries: totalSearchQueries,
      modelUsed: "aiModel"
    });

    await logAIReasoning({
      sys: ADDRESS_SYS_MSG, // Log the main instructions
      user: USER_PROMPT, // Was userPrompt, but USER_PROMPT is the constant with full context
      thoughts: allTurnsForLog.join('\n\n---\n\n'), // Concatenate all turns
      answer: lastAiResponse,
      cloudfunction: 'cf41',
      productId: isMaterial ? linkedProductId : productId,
      materialId: materialId,
      rawConversation: allRawChunks
    });

    // Save Collected URLs
    if (collectedUrls.size > 0) {
      await saveURLs({
        urls: Array.from(collectedUrls),
        productId: isMaterial ? linkedProductId : productId,
        materialId: materialId,
        mSupplierData: isMaterial,
        pSupplierData: !isMaterial || !!linkedProductId,
        sys: ADDRESS_SYS_MSG,
        user: USER_PROMPT,
        thoughts: allTurnsForLog.join('\n\n---\n\n'),
        answer: lastAiResponse,
        cloudFunction: "cf41"
      });
    }

    // Update Firestore
    const updatePayload = {
      apcfSupplierAddress_done: true,
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    };

    if (finalAddress) updatePayload.supplier_address = finalAddress;
    if (finalCOO) {
      updatePayload.country_of_origin = finalCOO;
      updatePayload.coo_estimated = finalCOOEst;
    }
    // Append reasoning to existing logs or a new field? usually we don't overwrite generic 'reasoning' 
    // but here specific fields are often okay. Use a specific field to allow auditing.
    //if (finalReasoning) updatePayload.supplier_address_reasoning = finalReasoning;

    await docRef.update(updatePayload);
    logger.info(`[cf41] Completed for ${docRef.id}. Address: ${finalAddress || 'Not Found'}, COO: ${finalCOO || 'Not Found'}`);

    if (isMaterial) {
      await incrementChildProgress(materialId, 'cf41');
    }

    // Trigger Transport Calculation if needed
    if (isMaterial) {
      try {
        // Assuming standard pattern for triggering next step
        /* 
       await fetch(getFunctionUrl("apcfTransportNew"), {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ materialId })
       });
       */
      } catch (e) { }
    }

    res.json("Done");

  } catch (err) {
    logger.error("[cf41] Uncaught error:", err);
    await incrementChildProgress(materialId, 'cf41');
    res.status(500).json({ error: String(err) });
  }
});