//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf37 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf37] Invoked");
  try {
    // 1. Argument Parsing
    const { productId } = req.body;
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
    const productName = pData.name || "Unknown Product";
    const originalReasoning = pData.official_cf_sources || "No prior reasoning provided.";

    // 2. Prompt Construction
    const packagingFlag = pData.includePackaging === true ? " (Include Packaging)" : "";
    const userPrompt = "...";

    const SYS_MSG = "...";

    // 3. AI Call Logic with Retries & Fact Checking
    let currentHistory = [];
    let currentCollectedUrls = new Set();
    let finalAssistantText = "";
    let finalThoughts = "";
    let finalCost = 0;
    let finalTokens = { input: 0, output: 0 };
    let finalSearchQueries = [];
    let finalModel = 'aiModel';
    let finalRawConversation = [];

    // --- Helper: Run Main AI ---
    const runMainAI = async (prompt, history = [], collectedUrlsSet = new Set(), retries = 0) => {
      const vGenerationConfig = {
//
//
//
//
//
      };

      // Construct effective user prompt (append history if needed)
      let effectiveUserPrompt = "...";
      if (history.length > 0) {
        effectiveUserPrompt = history.map(h => `User: ${h.user}\nAI: ${h.ai}`).join('\n\n') + `\n\nUser: ${prompt}`;
      }

      return await runGeminiStream({
        model: 'aiModel',
        generationConfig: vGenerationConfig,
        user: effectiveUserPrompt,
        collectedUrls: collectedUrlsSet
      });
    };

    // --- Helper: Response Parser (Used locally for flow control) ---
    const parseResponseForFlow = (text) => {
      let cf = text.match( /.*/);
      let rawCF = cf ? cf[1].trim() : null;
      let parsedCF = rawCF && ! /.*/.test(rawCF) ? parseFloat(rawCF) : null;
      return { productCF: parsedCF, rawText: text };
    };

    // --- Helper: Run Fact Checker ---
    const runFactChecker = async (originalPrompt, originalSys, originalOutput, originalUrls) => {
      const fcModel = 'aiModel';
      const fcSys = `...`;

      const fcPrompt = "..."
      const fcConfig = {
//
//
//
//
//
      };

      const fcUrls = new Set();
      const result = await runGeminiStream({
        model: fcModel,
        generationConfig: fcConfig,
        user: fcPrompt,
        collectedUrls: fcUrls
      });

      // Log FC Transaction
      await logAITransaction({
        cfName: 'cf37-FC',
        productId,
        cost: result.cost,
        totalTokens: result.totalTokens,
        searchQueries: result.searchQueries,
        modelUsed: fcModel
      });
      await logAIReasoning({
        sys: fcSys,
        user: fcPrompt,
        thoughts: result.thoughts,
        answer: result.answer,
        cloudfunction: 'cf37-FC',
        productId,
        rawConversation: result.rawConversation
      });

      return result;
    };


    /* ---------------- EXECUTION FLOW ---------------- */

    // Step 1: Initial AI Call
    let currentPrompt = "...";
    let isComplete = false;
    let attemptCount = 0;
    const MAX_RETRIES_NO_URL = 2;

    // We loop for the "No URL" retries
    while (!isComplete && attemptCount <= MAX_RETRIES_NO_URL) {
      const result = await runMainAI(currentPrompt, currentHistory, currentCollectedUrls);

      finalCost += result.cost;
      finalAssistantText = result.answer;
      finalThoughts = result.thoughts;
      finalTokens = result.totalTokens;
      finalSearchQueries = result.searchQueries;
      finalRawConversation = result.rawConversation;
      finalModel = result.model;

      const parsed = parseResponseForFlow(finalAssistantText);
      const hasCF = parsed.productCF !== null;
      // Check if ANY URLs were used (search queries or context)
      const hasUrls = result.searchQueries.length > 0 || currentCollectedUrls.size > 0;

      if (hasCF && !hasUrls) {
        // Found CF but NO URLs
        if (attemptCount < MAX_RETRIES_NO_URL) {
          logger.info(`[cf37] Found CF but no URLs. Retrying (Attempt ${attemptCount + 1})...`);
          currentHistory.push({ user: currentPrompt, ai: finalAssistantText });
          currentPrompt = "...";
          attemptCount++;
        } else {
          logger.info(`[cf37] Max retries reached with no URLs. Treating as Unknown.`);
          finalAssistantText = finalAssistantText.replace( /.*/, "*product_cf: Unknown");
          isComplete = true;
        }
      } else if (hasCF && hasUrls) {
        // Found CF AND URLs -> Fact Check
        logger.info(`[cf37] Found CF and URLs. Running Fact Checker...`);

        let fcPass = false;
        let fcAttempts = 0;
        const MAX_FC_LOOPS = 1; // 0-indexed (2 total runs)

        while (!fcPass && fcAttempts <= MAX_FC_LOOPS) {
          const fcResult = await runFactChecker(currentPrompt, SYS_MSG, finalAssistantText, Array.from(currentCollectedUrls));

          const resMatch = fcResult.answer.match( /.*/);
          const resultStatus = resMatch ? resMatch[1].toLowerCase() : "fail";

          if (resultStatus === "pass") {
            logger.info(`[cf37] Fact Checker PASSED.`);
            fcPass = true;
            isComplete = true;
          } else {
            // Failed
            const fbMatch = fcResult.answer.match( /.*/);
            const feedback = fbMatch ? fbMatch[1].trim() : "No feedback provided.";

            if (fcAttempts < MAX_FC_LOOPS) {
              logger.info(`[cf37] Fact Checker FAILED. Retrying Original AI... Feedback: ${feedback}`);
              currentHistory.push({ user: currentPrompt, ai: finalAssistantText });
              currentPrompt = `...`;

              const retryResult = await runMainAI(currentPrompt, currentHistory, currentCollectedUrls);

              // Update "final" results
              finalAssistantText = retryResult.answer;
              finalThoughts = retryResult.thoughts;
              finalRawConversation = retryResult.rawConversation;
              finalCost += retryResult.cost;

              // Check if retry result is valid
              const retryParsed = parseResponseForFlow(finalAssistantText);
              const retryHasCF = retryParsed.productCF !== null;

              if (!retryHasCF) {
                logger.info(`[cf37] Retry failed to produce a CF. Ending.`);
                isComplete = true;
                break;
              }

              fcAttempts++;
            } else {
              // Double Fail
              logger.info(`[cf37] Fact Checker FAILED twice. Force Unknown.`);
              finalAssistantText = finalAssistantText.replace( /.*/, "*product_cf: Unknown");
              isComplete = true;
              fcPass = true;
            }
          }
        }
        if (!isComplete) isComplete = true; // Output is final

      } else {
        // No CF found (Unknown) -> End
        logger.info(`[cf37] No CF found (Unknown). Ending.`);
        isComplete = true;
      }
    }

    // 4. Logging
    await logAITransaction({
      cfName: 'cf37',
      productId: productId,
      cost: finalCost,
      totalTokens: finalTokens,
      searchQueries: finalSearchQueries,
      modelUsed: finalModel,
    });

    // logAIReasoning for the FINAL response
    await logAIReasoning({
      sys: SYS_MSG,
      user: currentPrompt, // The last prompt used
      thoughts: finalThoughts,
      answer: finalAssistantText,
      cloudfunction: 'cf37-final',
      productId: productId,
      rawConversation: finalRawConversation,
    });

    if (currentCollectedUrls.size) {
      await saveURLs({
        urls: Array.from(currentCollectedUrls),
        productId,
        pSDCFData: true,
        sys: SYS_MSG,
        user: currentPrompt,
        thoughts: finalThoughts,
        answer: finalAssistantText,
        cloudfunction: 'cf37',
      });
    }

    // 5. Response Parsing
    // Helper to parse text into an object
    const parseResponse = (text) => {
      let cf = text.match( /.*/);
      let origCf = text.match( /.*/);
      let origStages = text.match( /.*/);
      let stds = text.match( /.*/);
      let extra = text.match( /.*/);

      let rawCF = cf ? cf[1].trim() : null;
      let parsedCF = rawCF && ! /.*/.test(rawCF) ? parseFloat(rawCF) : null;

      let rawOrigCF = origCf ? origCf[1].trim() : null;
      let parsedOrigCF = rawOrigCF && ! /.*/.test(rawOrigCF) ? parseFloat(rawOrigCF) : null;

      let stdsList = [];
      let isIso = false;
      let stdsRaw = stds ? stds[1].trim() : null;
      if (stdsRaw && stdsRaw.toLowerCase() !== 'unknown' && stdsRaw.length > 0) {
        stdsList = stdsRaw.split(',').map(s => s.trim()).filter(s => s);
        isIso = stdsList.some(s => s.toUpperCase().startsWith('ISO'));
      }

      return {
        productCF: parsedCF,
        originalCF: parsedOrigCF,
        originalStages: origStages ? origStages[1].trim() : null,
        standardsList: stdsList,
        isIsoAligned: isIso,
        extraInformation: extra ? extra[1].trim() : null,
        isEmpty: !cf && !origCf && !origStages && !stds && !extra
      };
    };

    let originalData = parseResponse(finalAssistantText);

    // Initial variable set
    let parsedProductCF = originalData.productCF;
    let parsedOriginalCF = originalData.originalCF;
    let originalLifecycleStages = originalData.originalStages;
    let standardsList = originalData.standardsList;
    let isIsoAligned = originalData.isIsoAligned;
    let extraInfo = originalData.extraInformation;


    // --- TIKA VERIFICATION STEP REMOVED ---

    // 6. Firestore Update

    // 6. Firestore Update
    const updatePayload = {};

    if (Number.isFinite(parsedProductCF)) {
      updatePayload.supplier_cf = parsedProductCF;
      updatePayload.cf_full = parsedProductCF;
    }

    if (Number.isFinite(parsedOriginalCF)) {
      updatePayload.oscf = parsedOriginalCF;
    }

    if (originalLifecycleStages && originalLifecycleStages.toLowerCase() !== 'unknown') {
      updatePayload.socf_lifecycle_stages = originalLifecycleStages;
    }

    if (extraInfo && extraInfo.toLowerCase() !== 'unknown') {
      updatePayload.extra_information = extraInfo;
    }

    updatePayload.sdcf_standards = standardsList;
    updatePayload.sdcf_iso_aligned = isIsoAligned;

    // Only update if there is something to change
    if (Object.keys(updatePayload).length > 0) {
      await pRef.update(updatePayload);
      logger.info(`[cf37] Updated product ${productId} with:`, updatePayload);
    } else {
      logger.info(`[cf37] No valid data found in AI response to update for product ${productId}.`);
    }

    // 7. Finalization
    res.json("Done");

  } catch (err) {
    logger.error("[cf37] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});