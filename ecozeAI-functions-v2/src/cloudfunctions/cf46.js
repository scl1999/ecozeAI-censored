const { admin, db, logger, onRequest, onMessagePublished, onSchedule, onDocumentCreated, fetch, discoveryEngineClient, tasksClient, pubSubClient } = require('../../config/firebase');
const { REGION, TIMEOUT, MEM, SECRETS, VERTEX_REDIRECT_RE, INTERACTIONS_API_BASE } = require('../../config/constants');
const { getGeminiClient, runGeminiStream, runOpenModelStream, logFullConversation } = require('../../services/ai/gemini');
const { logAITransaction, logAIReasoning, logAITransactionAgent, logAIReasoningWorkItem, logAIReasoningSingle, logAIReasoningBatch, logAIReasoningFinal, logAIReasoningSimple, logAITransactionSimple } = require('../../services/ai/costs');
const { isValidUrl, unwrapVertexRedirect, saveURLs, extractUrlsFromInteraction, harvestUrls, harvestUrlsFromText, generateReasoningString } = require('../../services/ai/urls');
const { createInteraction, getInteraction, parseNDJSON } = require('../../services/ai/interactionsapi');
const { runAIChat, runAIDeepResearch, runPromisesInParallelWithRetry, productDescription, callCF } = require('../../services/ai/aimain');
const { sleep, getFormattedDate } = require('../../utils/time');
const { runWithRetry, runWithRetryI } = require('../../utils/network');
const { parseCFValue, parseNDJSON: parseNDJSONUtil, parseBoM, parseBoMTable, parseBoMTableLegacy, getStepLabel, getFormattedDate: getFormattedDateUtil } = require('../../utils/formatting');
const prompts = require('../../services/ai/prompts');
const { 
  DUPLICATE_SYS, BOM_SYS, BOM_SYS_TIER_N, GO_AGAIN_PROMPT, TAG_GENERATION_SYS, 
  SYS_APCFSF, SYS_MSG_APCFSF, VERIFY_SYS_MSG, SYS_MSG_MPCFFULL_PRO, 
  SYS_MSG_MPCFFULL_CORE, REASONING_SUMMARIZER_SYS_2, MPCFFULL_PRODUCTS_SYS, 
  MPCFFULLNEW_TAG_GENERATION_SYS 
} = prompts;

// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------

// --- Interactions API Helpers (Updated for Streaming) ---



//-----------------------------------------------------------------------------------------------------------------------------------------------------------------

exports.cf46 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM_SUPPLIER_FINDER_DR,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf46] Invoked");

  // Helper to strictly remove surrounding brackets [] if present (e.g. [Sony] -> Sony)
  const cleanSupplierName = (name) => {
    if (!name) return name;
    let n = name.trim();
    if (n.startsWith('[') && n.endsWith(']')) {
      return n.slice(1, -1).trim();
    }
    return n;
  };

  // Helper to check if the AI response indicates an unknown supplier
  const isSupplierUnknown = (text) => {
    // Check for standard format
    const suppMatch = text.match( /.*/);
    if (suppMatch && suppMatch[1] && ! /.*/.test(suppMatch[1].trim())) {
      return false; // Known supplier found in standard format
    }
    // Check for estimation format
    const mainSuppMatch = text.match( /.*/);
    if (mainSuppMatch && mainSuppMatch[1] && ! /.*/.test(mainSuppMatch[1].trim())) {
      return false; // Known supplier found in estimation format
    }

    return true; // Supplier is unknown
  };

  try {
    const materialId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || null;
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || null;

    if ((!materialId && !productId) || (materialId && productId)) {
      res.status(400).json({ error: "Provide exactly one of materialId OR productId" });
      return;
    }

    const isMaterial = !!materialId;
    let targetRef, targetData, linkedProductId, initialUserPrompt, systemPrompt;

    // 1. Fetch document data and set up initial prompts
    if (isMaterial) {
      targetRef = db.collection("materials").doc(materialId);
      const mSnap = await targetRef.get();
      if (!mSnap.exists) {
        res.status(404).json({ error: `Material ${materialId} not found` });
        return;
      }
      targetData = mSnap.data() || {};
      linkedProductId = targetData.linked_product?.id || null;
      const materialName = (targetData.name || "").trim();
      const productChain = targetData.product_chain || '(unknown chain)';

      // NEW: Fetch parent context for materials
      let parentContextLine = "";
      if (targetData.parent_material) {
        // Case 2: Material has a parent_material
        const pmRef = targetData.parent_material;
        const pmSnap = await pmRef.get();
        if (pmSnap.exists) {
          const pmData = pmSnap.data() || {};
          const pmSupplierAddress = (pmData.supplier_address || "").trim();

          if (pmSupplierAddress && pmSupplierAddress !== "Unknown") {
            parentContextLine = `\nParent Product Assembly Address: ${pmSupplierAddress}`;
          } else {
            // Parent material has no valid address, use country of origin
            const pmCountryOfOrigin = (pmData.country_of_origin || "").trim();
            const pmCooEstimated = pmData.coo_estimated || false;
            if (pmCountryOfOrigin) {
              parentContextLine = `...`;
            }
          }
        }
      } else if (linkedProductId) {
        // Case 1: Material has NO parent_material, check linked product
        const pRef = db.collection("products_new").doc(linkedProductId);
        const pSnap = await pRef.get();
        if (pSnap.exists) {
          const pData = pSnap.data() || {};
          const pSupplierAddress = (pData.supplier_address || "").trim();

          if (pSupplierAddress && pSupplierAddress !== "Unknown") {
            parentContextLine = `...`;
          }
        }
      }

      initialUserPrompt = `...`;
      systemPrompt = SYS_APCFSF;
    } else {
      targetRef = db.collection("products_new").doc(productId);
      const pSnap = await targetRef.get();
      if (!pSnap.exists) {
        res.status(404).json({ error: `Product ${productId} not found` });
        return;
      }
      targetData = pSnap.data() || {};
      const productName = (targetData.name || "").trim();
      initialUserPrompt = `Product Name: ${productName}\nProduct Description: ${targetData.description || 'No description provided.'}`;
      systemPrompt = SYS_MSG_APCFSF;
    }

    logger.info(`[cf46] Starting process for ${isMaterial ? 'material' : 'product'}: ${targetRef.id}`);

    // 2. Set up the AI chat session
    const ai = getGeminiClient();
    const collectedUrls = new Set();
    const allRawChunks = [];
    const allSearchQueries = new Set();
    const allTurnsForLog = [];
    const historyLog = []; // NEW: Track multi-turn history for Fact Checker
    let lastTurnUrls = new Set(); // NEW: Track URLs from only the most recent turn
    let lastThoughts = ""; // NEW: Track last thoughts

    // Shared state
    let totalInputTks = 0;
    let totalOutputTks = 0;
    let totalToolCallTks = 0;
    let deepResearchHistory = ""; // For fallback context
    let runFallback = true;
    let fallbackReason = ""; // 'unknown', 'fc_failed', 'error'
    let fcResponseForPrompt = "...";
    let fcUrlsForPrompt = "...";

    // Unified Result Variables (Moved to top scope)
    let finalAnswer = "";
    const allAnswers = [];
    let wasEstimated = false;
    let finalRatings = null; // Store ratings for saving
    let supplier_probability_percentage = null; // Store probability for saving
    let lastFcResponse = ""; // Store the last Fact Checker response for estimation context
    let currentPrompt = "...";
    let chat = null;

    // --- STEP 1: DEEP RESEARCH AGENT ---
    logger.info(`[cf46] Starting Deep Research for ${isMaterial ? 'material' : 'product'}: ${targetRef.id}`);

    const deepResearchPayload = {
      agent: 'deep-research-pro-preview-12-2025',
      input: `System Instruction: ${systemPrompt}\n\nUser Request: ${initialUserPrompt}`,
      background: true,
      stream: true,
      store: true,
      agent_config: {
        type: 'deep-research',
        thinking_summaries: 'auto'
      }
    };

    let interactionId = null;
    let lastEventId = null;
    let isComplete = false;
    let drFinalAnswer = "";
    let drFinalOutputs = [];
    let drCollectedUrls = new Set();
    let drUsage = null;

    try {
      // Stream Handling Logic
      const handleStream = async (readableBody) => {
        for await (const chunk of parseNDJSON(readableBody)) {
          if (chunk.event_type === 'interaction.start') {
            interactionId = chunk.interaction.id;
            logger.info(`[cf46] Interaction Started: ${interactionId}`);
          }
          if (chunk.event_id) lastEventId = chunk.event_id;

          if (chunk.event_type === 'content.delta') {
            if (chunk.delta.type === 'text') {
              process.stdout.write(chunk.delta.text || '');
              drFinalAnswer += (chunk.delta.text || '');
              deepResearchHistory += (chunk.delta.text || '');
            } else if (chunk.delta.type === 'thought_summary') {
              const thought = chunk.delta.content?.text;
              deepResearchHistory += `\n[Thought] ${thought}\n`;
            }
          } else if (chunk.event_type === 'interaction.complete') {
            isComplete = true;
          } else if (chunk.event_type === 'error') {
            const errCode = chunk.error?.code;
            // Ignore deadline_exceeded as it triggers reconnection
            if (errCode === 'deadline_exceeded' || errCode === 'gateway_timeout') {
              logger.warn(`[cf46] Stream timeout (expected), will reconnect if needed.`);
            } else if (errCode === 13 || (chunk.error?.message && chunk.error.message.includes("BROWSE_URL_STATUS"))) {
              // Handle Browse Error gracefully
              logger.error(`[cf46] Deep Research Browse Error (non-fatal, stopping stream): ${chunk.error?.message || 'Unknown Browse Error'}`);
              isComplete = true;
            } else {
              logger.error(`[cf46] Stream Error Event: ${JSON.stringify(chunk)}`);
            }
          }

          if (chunk.interaction && chunk.interaction.outputs) {
            const extracted = extractUrlsFromInteraction(chunk.interaction.outputs);
            extracted.forEach(u => drCollectedUrls.add(u));
          }
        }
      };

      const streamBody = await createInteraction(deepResearchPayload, true);
      await handleStream(streamBody);

      if (!interactionId) throw new Error("Failed to acquire Interaction ID.");

      // Reconnection Loop
      const MAX_RECONNECT_TIME_MS = 25 * 60 * 1000;
      const startTime = Date.now();
      while (!isComplete && interactionId) {
        if (Date.now() - startTime > MAX_RECONNECT_TIME_MS) throw new Error("Timeout");
        try {
          const sBody = await getInteraction(interactionId, { stream: true, last_event_id: lastEventId });
          await handleStream(sBody);
          if (!isComplete) await sleepAI(2000);
        } catch (e) {
          logger.warn(`[cf46] Reconnect fail: ${e.message}`);
          await sleepAI(5000);
        }
      }

      // Polling for Final Output
      const MAX_POLL_TIME_MS = 5 * 60 * 1000;
      const pollStartTime = Date.now();
      while (interactionId) {
        if (Date.now() - pollStartTime > MAX_POLL_TIME_MS) break;
        try {
          const iObj = await getInteraction(interactionId);
          if (iObj.status === 'completed' && iObj.outputs) {
            drFinalOutputs = iObj.outputs;
            const txt = drFinalOutputs.filter(o => o.type === 'text');
            if (txt.length) drFinalAnswer = txt[txt.length - 1].text;
            drUsage = iObj.usage;

            // Log Safety/Finish Reason if available
            if (iObj.finishReason || iObj.safetyRatings) {
              logger.info(`[cf46] Interaction Finish Info: Reason=${iObj.finishReason}`, { safetyRatings: iObj.safetyRatings });
            }
            break;
          } else if (['failed', 'error', 'cancelled'].includes(iObj.status)) break;
        } catch (e) { }
        await sleepAI(3000);
      }

      // Metrics & Logs
      if (drUsage) {
        totalInputTks += drUsage.total_input_tokens || 0;
        totalOutputTks += drUsage.total_output_tokens || 0;
        totalToolCallTks += drUsage.total_tool_use_tokens || 0;
      }

      // URL Extraction
      const urlRegex = /.*/;
      const allDrUrls = new Set();
      (drFinalAnswer.match(urlRegex) || []).forEach(u => allDrUrls.add(u));
      (deepResearchHistory.match(urlRegex) || []).forEach(u => allDrUrls.add(u));
      drCollectedUrls.forEach(u => allDrUrls.add(u));

      const unwrapped = await Promise.all(Array.from(allDrUrls).map(u => unwrapVertexRedirect(u)));
      unwrapped.forEach(u => { if (u) collectedUrls.add(u); });

      // Add to logs
      allTurnsForLog.push(`--- 🤖 Deep Research Agent ---\n${deepResearchHistory}\n\n[Final Answer]\n${drFinalAnswer}`);

      // --- STEP 1.5: FACT CHECK DR RESULT ---
      if (!isSupplierUnknown(drFinalAnswer)) {
        if (collectedUrls.size === 0) {
          logger.warn("[cf46] DR found supplier but returned NO URLs. Forcing Fallback.");
          fallbackReason = "no_grounding";
        } else {
          logger.info("[cf46] DR found supplier. Running Fact Check...");

          // Fact Check Logic (Condensed)
          const cleanUrlsFC = Array.from(collectedUrls);
          const verifyUserPromptFC = "..."${i + 1}. ${u}`).join('\n')}`;

          // We use the same verify system prompt logic as the loop, but defined here for DR
          const VERIFY_SYS_MSG_DR = "...";
          const fcCollectedUrls = new Set();
          let fcResult = await runGeminiStream({
            model: 'aiModel',
            generationConfig: {
//
//
//
//
            },
            user: verifyUserPromptFC,
            collectedUrls: fcCollectedUrls,
          });

          // Merge FC URLs to main collection
          fcCollectedUrls.forEach(u => collectedUrls.add(u));

          // Parse Ratings
          const ratings = [];
          let sMatch;
          const supplierBlockRegex = /.*/;
          while ((sMatch = supplierBlockRegex.exec(fcResult.answer)) !== null) {
            const id = sMatch[1];
            const supplierName = sMatch[2].trim();

            const ratingRegex = new RegExp(`\\*rating_${id}:\\s*(?:\\["?|"?)(.*?)(?:\\]"?|"?)(?:\\r?\\n|\\*|$)`, 'i');
            const reasoningRegex = new RegExp(`\\*rating_reasoning_${id}:\\s*([\\s\\S]*?)(?=\\s*(?:\\r?\\n\\*supplier_|\\r?\\n\\*rating_|\\r?\\n\\*rating_reasoning_|$))`, 'i');

            const rMatch = fcResult.answer.match(ratingRegex);
            const reasonMatch = fcResult.answer.match(reasoningRegex);

            ratings.push({
              id: id,
              name: supplierName,
              rating: rMatch ? rMatch[1].trim() : "Unknown",
              reasoning: reasonMatch ? reasonMatch[1].trim() : ""
            });
          }

          const badRatings = ratings.filter(r => /Weak \/ Speculative/i.test(r.rating) || /No Evidence/i.test(r.rating));

          if (badRatings.length === 0 && ratings.length > 0) {
            logger.info("[cf46] Fact Check Passed for DR.");
            runFallback = false;
            // Set final answer variables to skip loop
            finalAnswer = drFinalAnswer;
            allAnswers.push(finalAnswer);
            finalRatings = ratings; // Capture ratings for saving
          } else {
            logger.warn("[cf46] Fact Check Failed for DR.");
            fallbackReason = "fc_failed";
            fcResponseForPrompt = fcResult.answer;
            fcUrlsForPrompt = Array.from(fcCollectedUrls).join(', '); // capture FC URLs
          }

          // Log FC Cost
          totalInputTks += fcResult.totalTokens?.input || 0;
          totalOutputTks += fcResult.totalTokens?.output || 0;
          allTurnsForLog.push(`--- 🕵️ Fact Checker ---\n${fcResult.thoughts || ""}\n${fcResult.answer}`);
          allRawChunks.push(...fcResult.rawConversation);

        } // End check for collectedUrls
      } else {
        logger.info("[cf46] DR returned Unknown.");
        fallbackReason = "unknown";
      }

    } catch (drErr) {
      logger.error(`[cf46] Deep Research Failed: ${drErr.message}`);
      fallbackReason = "error";
    }


    // --- FALLBACK SETUP ---
    // 'chat' and 'currentPrompt' are declared at top scope.
    let loopContinuously = runFallback;

    let vGenerationConfig = null;

    if (runFallback) {
      logger.info("[cf46] Entering Fallback Loop (Gemini 3 Pro)...");
      vGenerationConfig = {
//
//
//
//
//
      };
      chat = ai.chats.create({ model: 'aiModel', config: vGenerationConfig });

      // Construct Prompt based on fallback reason
      const drUrlsStr = Array.from(drCollectedUrls).join(', ');

      if (fallbackReason === 'unknown') {
        currentPrompt = `...`;
      } else if (fallbackReason === 'no_grounding') {
        currentPrompt = `...`;
      } else {
        // Generic error fallback
        currentPrompt = `

${initialUserPrompt}
`;
      }
    }

    // 3. Start the multi-step conversation loop
    const MAX_DIRECT_RETRIES = 5;

    let retryCount = 0;
    let factCheckCount = 0;

    // We wrap the existing loop in a conditional check implicitly via 'loopContinuously'
    // But we also need to guard the loop execution if we already found an answer.

    while (loopContinuously) {
      // RESET raw chunks for this attempt to prevent OOM on retries
      allRawChunks.length = 0;

      const isLastAttempt = retryCount >= MAX_DIRECT_RETRIES;

      // --- START: Separate Estimation AI Logic ---
      if (isLastAttempt && !wasEstimated) {
        logger.info(`[cf46] Max retries (${MAX_DIRECT_RETRIES}) reached. Triggering separate Estimation AI.`);
        wasEstimated = true;

        const historyContext = allTurnsForLog.join('\n\n');

        const ESTIMATION_SYS_MSG = "...";

        // Use fcResponseForPrompt if available (from DR fallback logic) or lastFcResponse (from loop)
        const effectiveFcResponse = fcResponseForPrompt || lastFcResponse || "None";

        const estimationPrompt = "...";

        try {
          const estCollectedUrls = new Set();
          const estResult = await runGeminiStream({
            model: 'aiModel', // Using Pro for better reasoning
            generationConfig: {
//
//
//
//
//
            },
            user: estimationPrompt,
            collectedUrls: estCollectedUrls,
          });

          // Merge Tokens (approximate allocation to input/output to ensure total cost is captured)
          // estResult.totalTokens is the sum. We'll add it to totalOutputTks for simplicity in tracking.
          totalOutputTks += estResult.totalTokens || 0;

          // Merge Logs
          allTurnsForLog.push(`--- 👤 User (Estimation) ---\n${estimationPrompt}`);
          allTurnsForLog.push(`--- 🤖 AI (Estimation) ---\n${estResult.thoughts || ""}\n${estResult.answer}`);
          allRawChunks.push(...estResult.rawConversation);

          // Merge URLs
          estCollectedUrls.forEach(u => collectedUrls.add(u));

          // Parse Result
          finalAnswer = estResult.answer;
          allAnswers.push(finalAnswer);

          // Extract Probability Percentage
          const probMatch = finalAnswer.match( /.*/);
          if (probMatch) {
            supplier_probability_percentage = parseFloat(probMatch[1]);
          }

          logger.info(`[cf46] Estimation complete. Probability: ${supplier_probability_percentage}%`);
          logger.info(`[cf46] Estimation complete. Probability: ${supplier_probability_percentage}%`);
          // Removed break; to allow flow to Fact Checker

        } catch (estErr) {
          logger.error(`[cf46] Estimation AI failed: ${estErr.message}`);
          finalAnswer = "*supplier_name: Unknown\n*reasoning_supplier_identification: Estimation AI failed.";
          finalAnswer = "*supplier_name: Unknown\n*reasoning_supplier_identification: Estimation AI failed.";
          // Removed break to allow "Unknown" flow to handle it (or just break loop if truly failed)
          break;
        }
      }
      // --- END: Separate Estimation AI Logic ---

      if (!wasEstimated) { // Only run standard generation if NOT estimated
        const urlsThisTurn = new Set();
        const rawChunksThisTurn = [];
        const streamResult = await runWithRetry(() => chat.sendMessageStream({ message: currentPrompt }));

        let answerThisTurn = "";
        let thoughtsThisTurn = "";
        let groundingUsedThisTurn = false;
        for await (const chunk of streamResult) {
          rawChunksThisTurn.push(chunk);
          harvestUrls(chunk, urlsThisTurn);

          if (chunk.candidates && chunk.candidates.length > 0) {
            for (const candidate of chunk.candidates) {
              // Process content parts
              if (candidate.content?.parts) {
                for (const part of candidate.content.parts) {
                  if (part.text) {
                    answerThisTurn += part.text;
                  } else if (part.functionCall) {
                    thoughtsThisTurn += `\n--- TOOL CALL ---\n${JSON.stringify(part.functionCall, null, 2)}\n`;
                  } else {
                    // Capture other non-text/call parts as thoughts
                    const thoughtText = JSON.stringify(part, null, 2);
                    if (thoughtText !== '{}') {
                      thoughtsThisTurn += `\n--- AI THOUGHT ---\n${thoughtText}\n`;
                    }
                  }
                }
              }
              // Process grounding metadata
              const gm = candidate.groundingMetadata;
              if (gm?.webSearchQueries?.length) {
                thoughtsThisTurn += `\n--- SEARCH QUERIES ---\n${gm.webSearchQueries.join("\n")}\n`;
                gm.webSearchQueries.forEach(q => allSearchQueries.add(q));
                groundingUsedThisTurn = true;
              }
            }
          } else if (chunk.text) {
            // Fallback for simple text-only chunks
            answerThisTurn += chunk.text;
          }
        }

        // Ensure we flag grounding usage if harvestUrls found URLs
        if (urlsThisTurn.size > 0) {
          groundingUsedThisTurn = true;
        }

        allRawChunks.push(...rawChunksThisTurn);
        finalAnswer = answerThisTurn.trim();
        lastThoughts = thoughtsThisTurn; // Capture standard thoughts
        lastTurnUrls = new Set(urlsThisTurn); // NEW: Capture strictly this turn's URLs for FC compatibility
        allAnswers.push(finalAnswer);

        allTurnsForLog.push(`--- 👤 User ---\n${currentPrompt}`);
        const aiTurnLog = thoughtsThisTurn.replace( /.*/, '').trim();
        allTurnsForLog.push(`--- 🤖 AI ---\n${aiTurnLog}`);

        if (groundingUsedThisTurn) {
          urlsThisTurn.forEach(url => collectedUrls.add(url));
        }

        // --- Accurate Token Counting for this Turn ---
        const historyBeforeSend = await chat.getHistory();
        const currentTurnPayload = [...historyBeforeSend.slice(0, -1), { role: 'user', parts: [{ text: currentPrompt }] }];

        const { totalTokens: currentInputTks } = await ai.models.countTokens({
          model: 'aiModel',
          contents: currentTurnPayload,
//
//
        });
        totalInputTks += currentInputTks || 0;

        const { totalTokens: currentOutputTks } = await ai.models.countTokens({
          model: 'aiModel',
          contents: [{ role: 'model', parts: [{ text: finalAnswer }] }]
        });
        totalOutputTks += currentOutputTks || 0;

        const { totalTokens: currentToolCallTks } = await ai.models.countTokens({
          model: 'aiModel',
          contents: [{ role: 'model', parts: [{ text: thoughtsThisTurn }] }]
        });
        totalToolCallTks += currentToolCallTks || 0;
      } // End if (!wasEstimated)

      // --- DECISION LOGIC ---

      // 1. If we forced an estimation (fallback), we accept the result and break.
      // REMOVED: if (wasEstimated && isLastAttempt) { ... break; } 
      // We now allow it to proceed to Fact Checker.

      // 2. Check if the supplier is "Unknown"
      if (isSupplierUnknown(finalAnswer)) {
        logger.warn(`[cf46] Supplier is "Unknown" on attempt ${retryCount + 1}.`);

        // If we are already estimating and it's still Unknown, we must give up to avoid infinite loop
        if (wasEstimated) {
          logger.warn(`[cf46] Estimation returned Unknown. Giving up.`);
          break;
        }

        if (retryCount < MAX_DIRECT_RETRIES) {
          retryCount++;
          logger.info(`[cf46] Retrying (Retry #${retryCount}/${MAX_DIRECT_RETRIES})...`);
          currentPrompt = "Try again to find the supplier";
          continue; // Loop again
        } else {
          // We shouldn't reach here if the logic above "if (isLastAttempt && !wasEstimated)" works,
          // but just in case, we loop again which will trigger the estimation logic.
          logger.info(`[cf46] Retries exhausted for Unknown, triggers estimation next loop.`);
          continue;
        }
      }

      // 3. Supplier FOUND (Directly). Now we run the Fact Checker.

      // NEW: Check if we have any URLs to fact check against.
      if (collectedUrls.size === 0) {
        logger.warn(`[cf46] Supplier found, but NO URLs collected. Cannot run Fact Checker.`);

        if (retryCount < MAX_DIRECT_RETRIES) {
          retryCount++;
          logger.info(`[cf46] Retrying due to missing URLs (Retry #${retryCount}/${MAX_DIRECT_RETRIES})...`);

          currentPrompt = `...`;
          continue; // Loop again
        } else {
          logger.info(`[cf46] Retries exhausted for missing URLs. Triggering estimation next loop.`);
          continue;
        }
      }

      logger.info(`[cf46] Supplier found. Running Fact Checker...`);

      // 3a. Prepare Fact Check Data
      // Unwrap URLs current collected (accumulated from all turns so far)
      const unwrappedUrls = [];
      const rawUrls = Array.from(collectedUrls);
      const validUrlsForUnwrap = rawUrls.filter(u => typeof u === 'string' && u.trim());
      const unwrappedResults = await Promise.all(validUrlsForUnwrap.map(u => unwrapVertexRedirect(u.trim())));
      unwrappedUrls.push(...unwrappedResults);
      const cleanUrls = Array.from(new Set(unwrappedUrls.filter(u => u && u.trim())));

      // Generate Reasoning String for the Verifier
      // Note: We use the accumulated conversation so far
      const currentFormattedConversation = allTurnsForLog.join('\n\n');
      const currentAggregatedAnswer = allAnswers.join('\n\n');

      const generatedReasoning = await generateReasoningString({
        sys: systemPrompt,
        user: initialUserPrompt,
        thoughts: currentFormattedConversation,
        answer: currentAggregatedAnswer,
        rawConversation: allRawChunks,
      });

      // Strip duplicate URL sections from the AI's reasoning
      // The fact checker will receive URLs separately, so we remove:
      // 1. ALL evidence_snippets sections (can occur multiple times in multi-turn conversations)
      // 2. ALL Sources: [1] = ... sections (can occur multiple times)
      // 3. thoughtSignature blobs (useless for fact checker)
      let cleanedReasoning = generatedReasoning;

      // Remove thoughtSignature (massive base64 blobs)
      cleanedReasoning = cleanedReasoning.replace( /.*/, '');

      // Remove ALL evidence snippets sections (from *evidence_snippets: to the next ——)
      // Use global flag to catch all instances across multiple turns
      cleanedReasoning = cleanedReasoning.replace( /.*/, '');

      // Remove ALL Sources sections (from "Sources:" through the numbered list)
      // Match from "Sources:" through lines like "[1] = https://..." until we hit a blank line or end
      cleanedReasoning = cleanedReasoning.replace( /.*/, '');


      // --- NEW Prompt Construction for Fact Checker (Multi-Turn Aware) ---

      // 1. Unwrap Last Turn URLs only (for "Sources" section)
      const unwrappedLastTurnUrls = [];
      const rawLastUrls = Array.from(lastTurnUrls);
      const validLastUrls = rawLastUrls.filter(u => typeof u === 'string' && u.trim());
      const unwrappedLastResults = await Promise.all(validLastUrls.map(u => unwrapVertexRedirect(u.trim())));
      unwrappedLastTurnUrls.push(...unwrappedLastResults);
      const cleanLastTurnUrls = Array.from(new Set(unwrappedLastTurnUrls.filter(u => u && u.trim())));

      // 2. Build History String
      let historyString = "";
      if (historyLog.length > 0) {
        historyLog.forEach((entry, idx) => {
          historyString += `{Main AI Response ${idx + 1} (not including thoughts)}\n${entry.mainAnswer}\n{Fact checker ${idx + 1} response (not including thoughts)}\n${entry.fcAnswer}\n\n`;
        });
      }

      // 3. Build Prompt
      let verifyUserPrompt = "...";

      let verifyResult = null;
      let verifyAttempts = 0;
      const MAX_VERIFY_ATTEMPTS = 2;

      // 3b. Run Verification Loop (internal retry for format)
      while (verifyAttempts < MAX_VERIFY_ATTEMPTS && !verifyResult) {
        verifyAttempts++;
        logger.info(`[cf46] Calling Gemini-3-Pro verification (attempt ${verifyAttempts})...`);

        try {
          const collectedUrlsVerify = new Set();
          const rawVerifyResult = await runGeminiStream({
            model: 'aiModel',
            generationConfig: {
//
//
//
//
//
                includeThoughts: true,
                thinkingBudget: 32768
              }
            },
            user: verifyUserPrompt,
            collectedUrls: collectedUrlsVerify,
          });

          // Check format
          const hasRating = /.*/.test(rawVerifyResult.answer);
          const hasReasoning = /.*/.test(rawVerifyResult.answer);

          if (hasRating && hasReasoning) {
            verifyResult = rawVerifyResult;

            // IMPORTANT: Capture URLs from the Fact Checker and add to main pool
            // We do this immediately so they are saved even if verification fails
            if (collectedUrlsVerify.size > 0) {
              logger.info(`[cf46] Captured ${collectedUrlsVerify.size} URLs from Fact Checker.`);
              collectedUrlsVerify.forEach(url => collectedUrls.add(url));
            }

            logger.info('[cf46] Verification response format valid.');
          } else {
            logger.warn(`[cf46] Verification failed format check (attempt ${verifyAttempts}).`);
            if (verifyAttempts < MAX_VERIFY_ATTEMPTS) {
              // Update prompt for retry
              verifyUserPrompt = `...`;
            }
          }
        } catch (verifyErr) {
          logger.error(`[cf46] Verification attempt ${verifyAttempts} failed:`, verifyErr.message);
          break;
        }
      }

      // 3c. Evaluate Verification Result
      if (verifyResult) {
        factCheckCount++;
        // Log verification transaction FIRST
        await logAITransaction({
          cfName: `apcfSupplierFinderDRFactCheck_${factCheckCount}`,
          productId: isMaterial ? linkedProductId : productId,
          materialId: materialId,
          cost: verifyResult.cost,
          totalTokens: verifyResult.totalTokens,
          searchQueries: verifyResult.searchQueries || [],
          modelUsed: 'aiModel',
        });

        // Log reasoning for the fact check (excluded from summarizer by naming convention)
        // Optimization: Remove 'thoughtSignature' (massive base64 blobs) from the prompt for the logger
        // to prevent context overflow in the summarizer, while keeping actual text content.
        let optimizedUserPrompt = "...".replace( /.*/, '"thoughtSignature": "[REMOVED]"');

        await logAIReasoning({
          sys: VERIFY_SYS_MSG,
          user: optimizedUserPrompt,
          thoughts: verifyResult.thoughts || "",
          answer: verifyResult.answer,
          cloudfunction: `apcfSupplierFinderDRFactCheck_${factCheckCount}`,
          productId: isMaterial ? linkedProductId : productId,
          materialId: materialId,
          rawConversation: verifyResult.rawConversation,
        });

        // Capture for final reasoning log
        allTurnsForLog.push(`--- 🕵️ Fact Checker ---\n${verifyResult.thoughts || ""}\n${verifyResult.answer}`);
        allRawChunks.push(...verifyResult.rawConversation);

        // NEW: Capture history for subsequent turns (if any)
        historyLog.push({
          mainAnswer: finalAnswer,
          fcAnswer: verifyResult.answer
        });

        // Parse all ratings
        const ratings = [];
        // Updated regex to capture supplier name as well
        // Format: *supplier_N: ... *rating_N: ... *rating_reasoning_N: ...
        // We iterate by finding *supplier_N blocks

        const supplierBlockRegex = /.*/;
        let sMatch;

        while ((sMatch = supplierBlockRegex.exec(verifyResult.answer)) !== null) {
          const id = sMatch[1];
          const supplierName = sMatch[2].trim();

          // Find corresponding rating and reasoning for this ID
          const ratingRegex = new RegExp(`\\*rating_${id}:\\s*(?:\\["?|"?)(.*?)(?:\\]"?|"?)(?:\\r?\\n|\\*|$)`, 'i');
          const reasoningRegex = new RegExp(`\\*rating_reasoning_${id}:\\s*([\\s\\S]*?)(?=\\s*(?:\\r?\\n\\*supplier_|\\r?\\n\\*rating_|\\r?\\n\\*rating_reasoning_|$))`, 'i');

          const rMatch = verifyResult.answer.match(ratingRegex);
          const reasonMatch = verifyResult.answer.match(reasoningRegex);

          const ratingText = rMatch ? rMatch[1].trim() : "Unknown";
          const reasoningText = reasonMatch ? reasonMatch[1].trim() : "";

          ratings.push({
            id: id,
            name: supplierName,
            rating: ratingText,
            reasoning: reasoningText
          });
        }

        logger.info(`[cf46] Fact Check Ratings parsed: ${ratings.length}`);

        // Determine Pass/Fail
        // Fail if ANY supplier has a "bad" rating (Probable, Weak / Speculative OR No Evidence)
        const badRatings = ratings.filter(r =>
          /Weak \/ Speculative/i.test(r.rating) ||
          /No Evidence/i.test(r.rating)
        );
        const isFactCheckFailed = badRatings.length > 0;

        if (isFactCheckFailed) {
          logger.warn(`[cf46] Fact check FAILED for ${badRatings.length} suppliers.`);

          if (wasEstimated) {
            logger.info("[cf46] Estimation fact check failed (low confidence). Saving as is with actual ratings.");
            // We do NOT break here anymore. We let it fall through to the persistence logic 
            // so we can save the actual bad ratings (e.g. 4 or 5) instead of just assuming.
            finalRatings = ratings;
            break;
          }

          // If we have retries left, we try again with specific feedback
          if (retryCount < MAX_DIRECT_RETRIES) {
            retryCount++;
            logger.info(`[cf46] Retrying with feedback (Retry #${retryCount}/${MAX_DIRECT_RETRIES})...`);

            const currentAllUrls = [];
            for (const u of collectedUrls) {
              if (typeof u === 'string' && u.trim()) currentAllUrls.push(u.trim());
            }

            // Construct feedback string from all ratings
            const feedbackDetails = ratings.map(r => `Supplier ${r.id}: ${r.rating}\nReasoning: ${r.reasoning}`).join('\n\n');

            // Capture feedback for potential estimation
            lastFcResponse = `Fact Checker Response:\n${feedbackDetails}\n\nSources:\n${currentAllUrls.join('\n')}`;

            currentPrompt = `...`;
            continue; // Loop again
          } else {
            logger.info(`[cf46] Retries exhausted for Fact Check failure.`);

            // FALLBACK LOGIC
            // Check if ALL are bad -> Estimate
            // Check if SOME are good -> Filter & Promote

            const goodRatings = ratings.filter(r =>
              ! /.*/.test(r.rating) &&
              ! /.*/.test(r.rating)
            );

            if (goodRatings.length === 0) {
              logger.info(`[cf46] All suppliers failed verification. Triggering estimation next loop.`);
              continue; // Triggers estimation at top of loop
            } else {
              // NEW: Check if we only have "Probable" results and haven't estimated yet.
              const hasBetterThanProbable = goodRatings.some(r =>
                /Direct Proof/i.test(r.rating) ||
                /Strong Inference/i.test(r.rating)
              );

              if (!hasBetterThanProbable && !wasEstimated) {
                logger.info(`[cf46] Only found 'Probable' suppliers. Triggering estimation to attempt better results.`);
                continue;
              }

              logger.info(`[cf46] Some suppliers passed verification. Filtering and promoting...`);

              // We need to map the ratings back to the actual supplier names from the AI's previous answer (finalAnswer)
              // This is tricky because we only have IDs 1, 2, 3... from the verifier.
              // We assume the verifier respected the order: 1 = main, 2 = other_1, 3 = other_2...
              // Let's parse the ORIGINAL answer to get the names.

              const mainSuppMatch = finalAnswer.match( /.*/);
              const mainSupplierName = mainSuppMatch ? cleanSupplierName(mainSuppMatch[1].trim()) : null;

              const otherSuppliersMap = []; // [{id: 1, name: ...}, {id: 2, name: ...}] (indices for 'other')
              // Actually, let's just make a flat list of ALL suppliers in order: [Main, Other1, Other2...]

              const allSuppliersOrdered = [];
              if (mainSupplierName) allSuppliersOrdered.push({ type: 'main', name: mainSupplierName, originalIndex: 0 });

              const otherSuppRegex = /.*/;
              let om;
              while ((om = otherSuppRegex.exec(finalAnswer)) !== null) {
                allSuppliersOrdered.push({ type: 'other', name: cleanSupplierName(om[2].trim()), originalIndex: parseInt(om[1]) }); // Index might not be sequential in raw text but usually is
              }

              // Now match with goodRatings using NAME matching
              // We have `allSuppliersOrdered` which contains the original names from the AI.
              // We have `goodRatings` which contains the names returned by the Verifier.
              // We need to find which original suppliers passed.

              const validSuppliers = [];

              // Create a map of good rating names for fuzzy matching
              const goodNames = goodRatings.map(r => r.name.toLowerCase());

              for (const originalSupp of allSuppliersOrdered) {
                // Check if this original supplier is in the good list
                // Simple includes check or fuzzy match?
                // The verifier is asked to return the name "exactly as the AI gave us", so strict match should work, 
                // but let's be robust with lowercase.

                const origName = originalSupp.name.toLowerCase();
                // Check if any good rating name contains this original name or vice versa
                const matchedRating = goodRatings.find(r => {
                  const gn = r.name.toLowerCase();
                  return gn.includes(origName) || origName.includes(gn);
                });
                const isGood = !!matchedRating;

                if (isGood) {
                  validSuppliers.push({ ...originalSupp, ratingObj: matchedRating });
                }
              }

              if (validSuppliers.length > 0) {
                // Sort by rating priority
                const ratingPriority = { "Direct Proof": 1, "Strong Inference": 2, "Probable": 3, "Weak": 4, "No Evidence": 5 };
                const getP = (txt) => {
                  for (const k in ratingPriority) if (txt.includes(k)) return ratingPriority[k];
                  return 5;
                };
                validSuppliers.sort((a, b) => getP(a.ratingObj.rating) - getP(b.ratingObj.rating));

                // Promote first valid to Main (best rating)
                const newMain = validSuppliers[0];
                const newOthers = validSuppliers.slice(1);

                const upd = {};
                if (isMaterial) upd.supplier_name = newMain.name;
                else upd.manufacturer_name = newMain.name;

                // NEW: Set Evidence Rating for Main Supplier
                upd.supplier_evidence_rating = getP(newMain.ratingObj.rating);

                // Capture ratings for the promoted ones
                finalRatings = validSuppliers.map(vs => ({ ...vs.ratingObj, name: vs.name }));

                if (newOthers.length > 0) {
                  upd.other_known_suppliers = admin.firestore.FieldValue.arrayUnion(...newOthers.map(s => s.name));

                  // NEW: Save Structured Other Suppliers
                  const structuredOthers = newOthers.map(o => ({
                    name: o.name,
                    evidence_rating: getP(o.ratingObj.rating),
                    rating_reasoning: o.ratingObj.reasoning
                  }));
                  upd.other_suppliers = structuredOthers;
                }

                // Save FCR info (just saving the raw verification result for record)
                upd.supplier_finder_fcr = "Mixed/Filtered";
                upd.supplier_finder_fcr_reasoning = "Filtered out weak suppliers: " + badRatings.map(r => r.id).join(', ');

                if (wasEstimated) {
                  upd.supplier_estimated = true;
                  if (supplier_probability_percentage !== null) {
                    if (isMaterial) upd.supplier_probability_percentage = supplier_probability_percentage;
                    else upd.manufacturer_probability_percentage = supplier_probability_percentage;

                    if (supplier_probability_percentage > 70) {
                      if (isMaterial) upd.supplier_confidence = "High"; else upd.manufacturer_confidence = "High";
                    } else if (supplier_probability_percentage > 40) {
                      if (isMaterial) upd.supplier_confidence = "Medium"; else upd.manufacturer_confidence = "Medium";
                    } else {
                      if (isMaterial) upd.supplier_confidence = "Low"; else upd.manufacturer_confidence = "Low";
                    }
                  }
                }

                upd.supplier_finder_retries = retryCount;

                await targetRef.update(upd);
                logger.info(`[cf46] Saved filtered/promoted data: ${JSON.stringify(upd)}`);

                finalAnswer = "MANUALLY_HANDLED";
                finalRatings = null; // Prevent post-loop logic from running again
                break;
              } else {
                // Should not happen if goodRatings > 0
                logger.warn("Logic error: Good ratings found but mapping failed. Triggering estimation.");
                continue;
              }
            }
          }

        } else {
          // Fact Check PASSED (All good)

          // NEW: Check if we only have "Probable" results and haven't estimated yet.
          const hasBetterThanProbable = ratings.some(r =>
            /Direct Proof/i.test(r.rating) ||
            /Strong Inference/i.test(r.rating)
          );

          if (!hasBetterThanProbable && !wasEstimated) {
            logger.info(`[cf46] Fact Check passed but only found 'Probable' suppliers. Triggering estimation to attempt better results.`);
            continue;
          }

          logger.info(`[cf46] Fact check PASSED. Reshuffling and saving result.`);

          // RESHUFFLING LOGIC (Same as Filter/Promote but for ALL passed)
          const mainSuppMatch = finalAnswer.match( /.*/);
          const mainSupplierName = mainSuppMatch ? cleanSupplierName(mainSuppMatch[1].trim()) : null;
          const allSuppliersOrdered = [];
          if (mainSupplierName) allSuppliersOrdered.push({ type: 'main', name: mainSupplierName, originalIndex: 0 });

          const otherSuppRegex = /.*/;
          let om;
          while ((om = otherSuppRegex.exec(finalAnswer)) !== null) {
            allSuppliersOrdered.push({ type: 'other', name: cleanSupplierName(om[2].trim()), originalIndex: parseInt(om[1]) });
          }

          const validSuppliers = [];
          for (const originalSupp of allSuppliersOrdered) {
            const origName = originalSupp.name.toLowerCase();
            const matchedRating = ratings.find(r => {
              const gn = r.name.toLowerCase();
              return gn.includes(origName) || origName.includes(gn);
            });
            if (matchedRating) {
              validSuppliers.push({ ...originalSupp, ratingObj: matchedRating });
            }
          }

          if (validSuppliers.length > 0) {
            // Sort by rating priority
            const ratingPriority = { "Direct Proof": 1, "Strong Inference": 2, "Probable": 3, "Weak": 4, "No Evidence": 5 };
            const getP = (txt) => {
              for (const k in ratingPriority) if (txt.includes(k)) return ratingPriority[k];
              return 5;
            };
            validSuppliers.sort((a, b) => getP(a.ratingObj.rating) - getP(b.ratingObj.rating));

            const newMain = validSuppliers[0];
            const newOthers = validSuppliers.slice(1);

            const upd = {};
            if (isMaterial) upd.supplier_name = newMain.name;
            else upd.manufacturer_name = newMain.name;

            // Capture ratings correctly
            finalRatings = validSuppliers.map(vs => ({ ...vs.ratingObj, name: vs.name }));

            // NEW: Set Evidence Rating for Main Supplier
            upd.supplier_evidence_rating = getP(newMain.ratingObj.rating);

            if (newOthers.length > 0) {
              upd.other_known_suppliers = admin.firestore.FieldValue.arrayUnion(...newOthers.map(s => s.name));

              // NEW: Save Structured Other Suppliers (DR)
              const structuredOthers = newOthers.map(o => ({
                name: o.name,
                evidence_rating: getP(o.ratingObj.rating),
                rating_reasoning: o.ratingObj.reasoning
              }));
              upd.other_suppliers = structuredOthers;
            }

            // Save FCR info
            const ratingSummary = finalRatings.map(r => `[${r.id}] ${r.rating}`).join('; ');
            const reasoningSummary = finalRatings.map(r => `[${r.id}] ${r.reasoning}`).join('\n---\n');
            upd.supplier_finder_fcr = ratingSummary;
            upd.supplier_finder_fcr_reasoning = reasoningSummary;

            if (wasEstimated) {
              upd.supplier_estimated = true;
              if (supplier_probability_percentage !== null) {
                if (isMaterial) upd.supplier_probability_percentage = supplier_probability_percentage;
                else upd.manufacturer_probability_percentage = supplier_probability_percentage;

                if (supplier_probability_percentage > 70) {
                  if (isMaterial) upd.supplier_confidence = "High"; else upd.manufacturer_confidence = "High";
                } else if (supplier_probability_percentage > 40) {
                  if (isMaterial) upd.supplier_confidence = "Medium"; else upd.manufacturer_confidence = "Medium";
                } else {
                  if (isMaterial) upd.supplier_confidence = "Low"; else upd.manufacturer_confidence = "Low";
                }
              }
            }

            upd.supplier_finder_retries = retryCount;

            await targetRef.update(upd);
            logger.info(`[cf46] Saved reshuffled data: ${JSON.stringify(upd)}`);

            finalAnswer = "MANUALLY_HANDLED";
            finalRatings = null; // Prevent post-loop logic from running again
            break;
            logger.info(`[cf46] Saved reshuffled data: ${JSON.stringify(upd)}`);

            finalAnswer = "MANUALLY_HANDLED";
            break;
          } else {
            logger.warn("[cf46] Fact Check passed but parsing failed for reshuffle. Saving as is.");
            finalRatings = ratings;
            break;
          }
        }

      } else {
        logger.error(`[cf46] Fact checker crashed or failed format. Accepting main AI result to prevent stall.`);
        break;
      }
    }

    const upd = {};
    const supplierConfidenceMap = {}; // Map to store confidence scores

    if (wasEstimated && finalAnswer !== "MANUALLY_HANDLED") { // Only process estimation parsing if not already handled by filter logic
      const aggregatedFinalAnswer = allAnswers.join('\n');
      logger.info("[cf46] Processing estimated supplier response.");
      const mainSuppMatch = aggregatedFinalAnswer.match( /.*/);
      const probabilityMatch = aggregatedFinalAnswer.match( /.*/);

      // Helper to get rating integer
      const getEstRating = (suppName) => {
        if (!finalRatings || finalRatings.length === 0) return 4; // Default to Weak if no FC run
        const ratingPriority = { "Direct Proof": 1, "Strong Inference": 2, "Probable": 3, "Weak": 4, "No Evidence": 5 };
        const matched = finalRatings.find(r => r.name.toLowerCase().includes(suppName.toLowerCase()) || suppName.toLowerCase().includes(r.name.toLowerCase()));
        if (!matched) return 4;
        for (const k in ratingPriority) if (matched.rating.includes(k)) return ratingPriority[k];
        return 5;
      };

      const otherSuppliers = [];
      const structuredOthers = []; // NEW: For valid data
      const otherSuppRegex = /.*/;
      let match;
      // Deduplication Set
      const seenEstNames = new Set();
      const mainSuppNameForCheck = mainSuppMatch && mainSuppMatch[1] ? mainSuppMatch[1].trim().toLowerCase() : null;
      if (mainSuppNameForCheck) seenEstNames.add(mainSuppNameForCheck);

      while ((match = otherSuppRegex.exec(aggregatedFinalAnswer)) !== null) {
        const id = match[1];
        const name = cleanSupplierName(match[2].trim().replace( /.*/, ' '));
        const nameLower = name.toLowerCase();

        // Deduplication Check
        if (name && !seenEstNames.has(nameLower) && nameLower !== 'unknown') {
          seenEstNames.add(nameLower);

          // Find the probability for this specific ID
          const probabilityRegex = new RegExp(`(?<!reasoning_)other_potential_supplier_probability_${id}:\\s*("?[^"\\n]*"?)`, 'i');
          const probMatch = aggregatedFinalAnswer.match(probabilityRegex);
          const confidence = probMatch ? probMatch[1].trim() : "Unknown";

          // Find the specific probability percentage
          const probPercRegex = new RegExp(`other_potential_supplier_probability_${id}:\\s*(\\d+)`, 'i');
          const probPercMatch = aggregatedFinalAnswer.match(probPercRegex);
          const confidenceScore = probPercMatch ? parseFloat(probPercMatch[1]) : null;

          // Find the reasoning
          const reasoningRegex = new RegExp(`reasoning_other_potential_supplier_${id}:\\s*([\\s\\S]*?)(?=\\s*(?:\r?\n|other_potential_supplier_|reasoning_other_potential_supplier_|main_supplier|$))`, 'i');
          const reasoningMatch = aggregatedFinalAnswer.match(reasoningRegex);
          const reasoning = reasoningMatch ? reasoningMatch[1].trim() : "";

          otherSuppliers.push(`${name} (${confidence})`);
          supplierConfidenceMap[name.toLowerCase()] = confidence;

          // Add to structured list
          structuredOthers.push({
            name: name,
            evidence_rating: getEstRating(name),
            rating_reasoning: reasoning || "Estimated by AI fallback.",
            confidence_score: confidenceScore
          });
        }
      }

      if (mainSuppMatch && mainSuppMatch[1]) {
        let mainSupplier = cleanSupplierName(mainSuppMatch[1].trim());
        const probability = probabilityMatch ? probabilityMatch[1].trim() : "Low";
        let usingFallback = false;
        let fallbackObj = null;

        let isEstimated = true;

        // PROMOTION LOGIC: Check key evidence ratings
        let mainRating = getEstRating(mainSupplier);
        let bestCandidate = { name: mainSupplier, rating: mainRating, obj: null };

        // Check if any 'other' supplier has better evidence
        for (const other of structuredOthers) {
          if (other.evidence_rating < bestCandidate.rating) {
            bestCandidate = { name: other.name, rating: other.evidence_rating, obj: other };
          }
        }

        // If strong evidence (1 or 2) found, promote/confirm and clean up
        if (bestCandidate.rating <= 2 && bestCandidate.name.toLowerCase() !== 'unknown') {
          logger.info(`[cf46] Strong evidence (${bestCandidate.rating}) found for '${bestCandidate.name}'. Promoting to CONFIRMED.`);
          mainSupplier = bestCandidate.name;
          isEstimated = false; // Set to FALSE

          // Remove from others if it was there
          if (bestCandidate.obj) {
            const idx = structuredOthers.indexOf(bestCandidate.obj);
            if (idx > -1) structuredOthers.splice(idx, 1);
            // Remove from string list
            const sIdx = otherSuppliers.findIndex(s => s.toLowerCase().includes(bestCandidate.name.toLowerCase()));
            if (sIdx > -1) otherSuppliers.splice(sIdx, 1);
          }

          // Remove WEAK suppliers (rating 4 or 5)
          for (let i = structuredOthers.length - 1; i >= 0; i--) {
            if (structuredOthers[i].evidence_rating >= 4) {
              const nameToRemove = structuredOthers[i].name;
              structuredOthers.splice(i, 1); // Remove from obj list
              const sIdx2 = otherSuppliers.findIndex(s => s.toLowerCase().includes(nameToRemove.toLowerCase()));
              if (sIdx2 > -1) otherSuppliers.splice(sIdx2, 1); // Remove from string list
            }
          }
        } else if (mainSupplier.toLowerCase() === 'unknown' && structuredOthers.length > 0) {
          // Fallback Strategy: Promote HIGHEST PROBABILITY supplier
          structuredOthers.sort((a, b) => (b.confidence_score || 0) - (a.confidence_score || 0));

          const fallbackObj = structuredOthers.shift(); // Remove from others list
          mainSupplier = fallbackObj.name;
          // Update bestCandidate ref
          bestCandidate = { name: fallbackObj.name, rating: fallbackObj.evidence_rating, obj: fallbackObj };

          logger.info(`[cf46] Main supplier was Unknown. Promoted '${mainSupplier}' (Highest Probability) from other inputs.`);

          // Remove from string list
          const sIdx = otherSuppliers.findIndex(s => s.toLowerCase().includes(mainSupplier.toLowerCase()));
          if (sIdx > -1) otherSuppliers.splice(sIdx, 1);
        }

        if (mainSupplier.toLowerCase() !== 'unknown') {
          if (isMaterial) {
            upd.supplier_name = mainSupplier;
            upd.supplier_confidence = probability;
            if (supplier_probability_percentage !== null) upd.supplier_probability_percentage = supplier_probability_percentage;
          } else {
            upd.manufacturer_name = mainSupplier;
            upd.manufacturer_confidence = probability;
            if (supplier_probability_percentage !== null) upd.manufacturer_probability_percentage = supplier_probability_percentage;
          }
          upd.supplier_estimated = isEstimated;
          // Set Fact Check fields (Rating Integer, Rating String, Reasoning)
          upd.supplier_evidence_rating = getEstRating(mainSupplier);

          // Restore Confidence Update Logic (Adapted for new Promotion Logic)
          if (typeof bestCandidate !== 'undefined' && bestCandidate.obj && bestCandidate.obj.confidence_score) {
            const score = bestCandidate.obj.confidence_score;
            if (score >= 80) upd.supplier_confidence = "High";
            else if (score >= 50) upd.supplier_confidence = "Medium";
            else upd.supplier_confidence = "Low";

            if (isMaterial) upd.supplier_probability_percentage = score;
            else upd.manufacturer_probability_percentage = score;
          }

          // Lookup full FCR details in finalRatings
          if (typeof finalRatings !== 'undefined' && finalRatings && finalRatings.length > 0) {
            const fcrMatch = finalRatings.find(r => r.name.toLowerCase().includes(mainSupplier.toLowerCase()) || mainSupplier.toLowerCase().includes(r.name.toLowerCase()));
            if (fcrMatch) {
              upd.supplier_finder_fcr = fcrMatch.rating;
              upd.supplier_finder_fcr_reasoning = fcrMatch.reasoning;
            }
          }
        }
      }
      if (otherSuppliers.length > 0) {
        upd.other_potential_suppliers = otherSuppliers;
      }
      if (structuredOthers.length > 0) {
        upd.other_suppliers = structuredOthers;
      }

    } else if (finalAnswer !== "MANUALLY_HANDLED" && !isSupplierUnknown(finalAnswer)) {
      logger.info("[cf46] Processing direct supplier response.");
      // Make the leading asterisk optional with *?
      const suppMatch = finalAnswer.match( /.*/);

      if (suppMatch && suppMatch[1]) {
        const value = cleanSupplierName(suppMatch[1].trim());
        if (value.toLowerCase() !== 'unknown' && !value.startsWith('*')) {
          if (isMaterial) upd.supplier_name = value;
          else upd.manufacturer_name = value;
          upd.supplier_estimated = false;
        }
      }

      // --- START: New Logic for other_known_suppliers ---
      const otherSuppliers = [];
      const otherSuppRegex = /.*/;
      let otherMatch;

      while ((otherMatch = otherSuppRegex.exec(finalAnswer)) !== null) {
        const supplierName = cleanSupplierName(otherMatch[2].trim());
        if (supplierName && supplierName.toLowerCase() !== 'unknown') {
          otherSuppliers.push(supplierName);
        }
      }

      if (otherSuppliers.length > 0) {
        upd.other_known_suppliers = admin.firestore.FieldValue.arrayUnion(...otherSuppliers);
        logger.info(`[cf46] Found ${otherSuppliers.length} other known suppliers.`);
      }
      // --- END: New Logic ---

    } else {
      logger.warn("[cf46] Loop finished without a valid supplier.");
    }

    // --- NEW: Save Evidence Ratings and Structured Other Suppliers ---
    if (finalRatings && finalRatings.length > 0) {
      const ratingMap = {
        "Direct Proof": 1,
        "Strong Inference": 2,
        "Probable / General Partner": 3,
        "Weak / Speculative": 4,
        "No Evidence": 5
      };

      const getRatingInt = (text) => {
        if (!text) return 5;
        const clean = text.replace( /.*/, '').trim();
        for (const [key, val] of Object.entries(ratingMap)) {
          if (clean.toLowerCase().includes(key.toLowerCase())) return val;
        }
        return 5;
      };

      // 1. Main Supplier Rating
      const savedMainName = isMaterial ? upd.supplier_name : upd.manufacturer_name;

      if (savedMainName) {
        const mainRatingObj = finalRatings.find(r => r.name.toLowerCase().includes(savedMainName.toLowerCase()) || savedMainName.toLowerCase().includes(r.name.toLowerCase()));
        if (mainRatingObj) {
          upd.supplier_evidence_rating = getRatingInt(mainRatingObj.rating);
        } else if (finalRatings.length > 0) {
          upd.supplier_evidence_rating = getRatingInt(finalRatings[0].rating);
        }
      }

      // 2. Other Suppliers Structured
      const structuredOthers = [];

      const seenOtherNamesDR = new Set();
      for (const r of finalRatings) {
        const isMain = savedMainName && (r.name.toLowerCase().includes(savedMainName.toLowerCase()) || savedMainName.toLowerCase().includes(r.name.toLowerCase()));

        if (!isMain) {
          const rNameLower = r.name.toLowerCase();
          if (!seenOtherNamesDR.has(rNameLower) && rNameLower !== 'unknown') {
            seenOtherNamesDR.add(rNameLower);
            structuredOthers.push({
              name: r.name,
              evidence_rating: getRatingInt(r.rating),
              rating_reasoning: r.reasoning,
              confidence_score: (() => {
                const rawConf = supplierConfidenceMap[r.name.toLowerCase()];
                if (rawConf) {
                  const parsed = parseFloat(rawConf.replace( /.*/, ''));
                  return isNaN(parsed) ? null : parsed;
                }
                // Fallback based on Evidence Rating
                const ratingInt = getRatingInt(r.rating);
                switch (ratingInt) {
                  case 1: return 95;
                  case 2: return 80;
                  case 3: return 60;
                  case 4: return 30;
                  case 5: return 0;
                  default: return null;
                }
              })()
            });


          }
        }
      }

      if (structuredOthers.length > 0) {
        upd.other_suppliers = structuredOthers;
      }
    }
    // --- END: New Evidence Rating Logic ---

    upd.supplier_finder_retries = retryCount;
    if (Object.keys(upd).length > 0) {
      await targetRef.update(upd);
      logger.info(`[cf46] Saved parsed data: ${JSON.stringify(upd)}`);
    }

    // 6. Save URLs and finalize
    const formattedConversation = allTurnsForLog.join('\n\n');

    const tokens = {
      input: totalInputTks,
      output: totalOutputTks,
      toolCalls: totalToolCallTks,
    };
    const cost = calculateCost('aiModel', tokens);
    const modelToLog = runFallback ? 'aiModel' : 'deep-research-pro-preview-12-2025';

    await logAITransaction({
      cfName: 'cf46',
      productId: isMaterial ? linkedProductId : productId,
      materialId: materialId,
      cost,
      totalTokens: totalInputTks + totalOutputTks + totalToolCallTks,
      searchQueries: Array.from(allSearchQueries),
      modelUsed: modelToLog,
    });

    await logAIReasoning({
      sys: systemPrompt,
      user: initialUserPrompt,
      thoughts: formattedConversation,
      answer: allAnswers.join('\n\n'),
      cloudfunction: 'cf46',
      productId: isMaterial ? linkedProductId : productId,
      materialId: materialId,
      rawConversation: allRawChunks,
    });
    await saveURLs({
      urls: Array.from(collectedUrls),
      materialId,
      productId,
      mSupplierData: isMaterial,
      pSupplierData: !isMaterial,
      sys: systemPrompt,
      user: initialUserPrompt,
      thoughts: formattedConversation,
      answer: allAnswers.join('\n\n'),
      cloudfunction: 'cf46',
    });

    await targetRef.update({ apcfSupplierFinderDR_done: true, status: "Done" });
    res.json("Done");

  } catch (err) {
    logger.error("[cf46] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});