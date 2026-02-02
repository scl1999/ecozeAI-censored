const { admin, db, logger, onRequest, fetch } = require('../../config/firebase');
//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf45 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf45] Invoked");

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
    let targetRef, targetData, linkedProductId, initialUserPrompt;

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
      initialUserPrompt = `Product Name: ${materialName}\nProduct Chain: ${productChain}\nProduct Description: ${targetData.description || 'No description provided.'}`;
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
    }

    logger.info(`[cf45] Starting process for ${isMaterial ? 'material' : 'product'}: ${targetRef.id}`);

    // 2. Initialize Agent Engine API endpoint
    // 2. Initialize Agent Engine API endpoint (Hardcoded)
    const projectId = 'projectId';
    const location = 'ecozeAIRegion'; // Corrected to match deployment
    const agentId = '6130208333409288192'; // Latest Agent ID

    // Get auth token first
    const accessToken = await getAccessToken();

    const latestAgentName = `projects/${projectId}/locations/${location}/reasoningEngines/${agentId}`;
    logger.info(`[cf45] Using hardcoded agent: ${latestAgentName}`);

    const agentEndpoint = `https://${location}-aiplatform.googleapis.com/v1/${latestAgentName}:streamQuery`;
    const createSessionEndpoint = `https://${location}-aiplatform.googleapis.com/v1/${latestAgentName}:query`;


    let currentPrompt = "...";
    let finalAnswer = "";
    const allAnswers = [];

    // 3. Query the agent with retries and fallback
    // 3. Query the agent with retries and fallback
    let currentReasoningSteps = [];
    let foundUrls = new Set();
    let fullEvents = []; // To store full trace
    let aggregatedUsage = {
      totalTokenCount: 0,
      promptTokenCount: 0,
      candidatesTokenCount: 0,
      toolUseTokenCount: 0, // Not explicitly provided by all endpoints, but placeholders
      reasoningTokenCount: 0
    };
    let lastUsedModel = "aiModel"; // Default fallback name, updated from events


    // Session management: Explicitly create a session first
    const sanitizedUserId = targetRef.id.toLowerCase().replace( /.*/, '');
    let capturedSessionId = null;

    logger.info(`[cf45] ⏳ Creating new session for user: ${sanitizedUserId}`);

    try {
      const createSessionPayload = {
        class_method: "async_create_session",
        input: {
          user_id: sanitizedUserId
        }
      };

      const sessionResponse = await fetch(createSessionEndpoint, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(createSessionPayload)
      });

      if (!sessionResponse.ok) {
        const createErrText = await sessionResponse.text();
        logger.error(`[cf45] ❌ Failed to create session: ${sessionResponse.status} - ${createErrText}`);
        throw new Error(`Failed to create session: ${createErrText}`);
      }

      const sessionData = await sessionResponse.json();
      // Extract numeric ID from output.id or output.name? User logs show output.id
      if (sessionData && sessionData.output && sessionData.output.id) {
        capturedSessionId = sessionData.output.id;
        logger.info(`[cf45] ✅ Created session ID: ${capturedSessionId}`);
      } else {
        logger.warn(`[cf45] ⚠️ Session created but ID not found in expected path. Response: ${JSON.stringify(sessionData)}`);
        // Fallback: If we can't get ID, we might fail or default to implicit creation
      }
    } catch (err) {
      logger.error(`[cf45] Session creation error: ${err.message}`);
      // Proceeding without session ID will fall back to implicit creation for each turn (not ideal but better than crash)
    }


    // 3. Conversation loop - automatically continue conversation within same session
    const MAX_CONVERSATION_TURNS = 3;

    for (let turn = 0; turn < MAX_CONVERSATION_TURNS; turn++) {

      logger.info(`[cf45] 💬 Conversation turn ${turn + 1}/${MAX_CONVERSATION_TURNS}`);
      logger.info(`[cf45] 🔑 Session ID before call: ${capturedSessionId || 'NONE - will create new session'}`);
      logger.info(`[cf45] Prompt: ${currentPrompt.substring(0, 200)}...`);

      const payload = {
        class_method: "async_stream_query",
        input: {
          message: currentPrompt,
          user_id: sanitizedUserId
        }
      };

      // Include session_id after first turn to maintain conversation context
      if (capturedSessionId) {
        payload.input.session_id = capturedSessionId;
        logger.info(`[cf45] ✅ SENDING session_id in payload: ${capturedSessionId}`);
      } else {
        logger.info(`[cf45] ⚠️ NO session_id in payload - agent will create new session (THIS SHOULD NOT HAPPEN with generated IDs)`);
        // Fallback or double check logic if needed
      }

      logger.info(`[cf45] Full payload: ${JSON.stringify(payload)}`);

      // Call the agent via HTTP API
      const response = await fetch(agentEndpoint, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Agent Engine API error: ${response.status} - ${errorText}`);
      }

      // Read the streaming response
      const responseText = await response.text();
      logger.info(`[cf45] ━━━━ RAW AGENT RESPONSE START ━━━━`);
      logger.info(`[cf45] Response length: ${responseText.length} bytes`);
      logger.info(`[cf45] Full response text:\n${responseText}`);
      logger.info(`[cf45] ━━━━ RAW AGENT RESPONSE END ━━━━`);

      // Parse streaming response with buffer handling
      const lines = responseText.trim().split('\n');
      let agentAnswer = "";
      let buffer = "";

      // Reset for this turn
      currentReasoningSteps = [];
      foundUrls = new Set();

      for (const line of lines) {
        if (!line.trim()) continue;

        const cleanLine = line.startsWith('data: ') ? line.substring(6) : line;
        buffer += cleanLine;

        try {
          const event = JSON.parse(buffer);
          buffer = ""; // Clear buffer on successful parse

          // Log every event's session_id if present (for debugging)
          if (event.session_id) {
            logger.info(`[cf45] 📡 Event session_id from agent: ${event.session_id}`);
          }

          // CAPTURE FULL EVENT TRACE
          fullEvents.push(event);

          // CAPTURE METADATA (Model, Usage)
          if (event.gcp_vertex_agent_llm_response) {
            try {
              const llmRes = JSON.parse(event.gcp_vertex_agent_llm_response);
              if (llmRes.model_version) lastUsedModel = llmRes.model_version;
            } catch (e) { /* ignore parse error */ }
          }
          if (event.usage_metadata) {
            // Aggregate usage (API usually returns cumulative or per-chunk? 
            // Agent Engine typically returns cumulative usage in the final usage_metadata event or delta.
            // We'll take the max seen values or accumulate if deltas.
            // Usually, standard usage_metadata is cumulative for the turn.
            // We'll trust the latest non-zero value for the turn.
            if (event.usage_metadata.total_token_count) aggregatedUsage.totalTokenCount = Math.max(aggregatedUsage.totalTokenCount, event.usage_metadata.total_token_count);
            if (event.usage_metadata.prompt_token_count) aggregatedUsage.promptTokenCount = Math.max(aggregatedUsage.promptTokenCount, event.usage_metadata.prompt_token_count);
            if (event.usage_metadata.candidates_token_count) aggregatedUsage.candidatesTokenCount = Math.max(aggregatedUsage.candidatesTokenCount, event.usage_metadata.candidates_token_count);
            if (event.usage_metadata.reasoning_token_count) aggregatedUsage.reasoningTokenCount = Math.max(aggregatedUsage.reasoningTokenCount, event.usage_metadata.reasoning_token_count);
          }

          // CAPTURE URLS from Grounding or Search Tool
          if (event.grounding_metadata && event.grounding_metadata.web_search_queries) {
            // Sometimes URLs are in search_entry_point.rendered_content (HTML) or not explicitly structured
            // We'll look for chunks that look like URLs or search citations if available
          }
          // Scan ALL content parts (text, tool calls) for URL-like strings or specific tool outputs
          if (event.content && event.content.parts) {
            event.content.parts.forEach(part => {
              if (part.text) {
                // Basic URL extraction regex
                const urls = part.text.match( /.*/);
                if (urls) urls.forEach(u => foundUrls.add(u));
              }
              if (part.function_call && part.function_call.name === 'google_search') {
                // Log search queries?
              }
              if (part.function_response && part.function_response.response) {
                // Extract URLs from JSON responses if possible
                const str = JSON.stringify(part.function_response.response);
                const urls = str.match( /.*/);
                if (urls) urls.forEach(u => foundUrls.add(u));
              }
            });
          }

          // Log agent thoughts/thinking (IMPORTANT FOR DEBUGGING)
          if (event.content && event.content.parts) {
            for (const part of event.content.parts) {
              if (part.thought || part.thinking) {
                const thoughtText = part.thought || part.thinking;
                logger.info(`[cf45] 🧠 AGENT THINKING: ${thoughtText}`);
                currentReasoningSteps.push(`Thinking: ${thoughtText}`);
              }
            }
          }

          // --- ADK Event Parsing Logic ---

          // 1. Tool Calls (Input or Output)
          if (event.actions && event.actions.call_tool) {
            const toolName = event.actions.call_tool.name || "unknown_tool";
            const toolArgs = event.actions.call_tool.arguments || {};
            const toolArgsStr = JSON.stringify(toolArgs);

            logger.info(`[cf45] 🛠️ Tool Call: ${toolName}(${toolArgsStr})`);

            // Capture URLs from google_search or browser tools
            if (toolName.includes("search") || toolName.includes("browse")) {
              if (toolArgs.url) foundUrls.add(toolArgs.url);
              if (toolArgs.urls && Array.isArray(toolArgs.urls)) toolArgs.urls.forEach(u => foundUrls.add(u));
            }

            currentReasoningSteps.push(`Tool Call: ${toolName}(${toolArgsStr})`);
          }

          // Check for "function_call" in content parts (Gemini style)
          if (event.content && event.content.parts) {
            for (const part of event.content.parts) {
              if (part.function_call) {
                const fName = part.function_call.name;
                const fArgs = part.function_call.args || {};

                logger.info(`[cf45] 🛠️ Function Call: ${fName}`);
                currentReasoningSteps.push(`Function Call: ${fName}(${JSON.stringify(fArgs)})`);

                if (fName.includes("search") || fName.includes("browse")) {
                  if (fArgs.url) foundUrls.add(fArgs.url);
                  if (fArgs.urls && Array.isArray(fArgs.urls)) fArgs.urls.forEach(u => foundUrls.add(u));
                }

              } else if (part.function_response) {
                const fName = part.function_response.name;
                logger.info(`[cf45] 🔙 Function Response: ${fName}`);
                currentReasoningSteps.push(`Function Response: ${fName}`);
              } else if (part.text) {
                agentAnswer += part.text;
              }
            }
          }

          // 2. Agent Transfers (Router)
          if (event.actions && event.actions.transfer_to_agent) {
            const transferTarget = event.actions.transfer_to_agent;
            logger.info(`[cf45] 🔀 Transferring to agent: ${transferTarget}`);
            currentReasoningSteps.push(`Transfer to Agent: ${transferTarget}`);
          }

          // 3. Final Output
          if (event.output) {
            if (typeof event.output === 'string') {
              agentAnswer += event.output;
            } else if (event.output.text) {
              agentAnswer += event.output.text;
            }
          }

        } catch (e) {
          // If JSON parse fails, it might be incomplete chunk. Keep in buffer and continue.
          if (buffer.length > 10000) {
            logger.warn(`[cf45] Buffer too large, dropping: ${buffer.substring(0, 100)}...`);
            buffer = "";
          }
        }
      }

      // Store this turn's response
      finalAnswer = agentAnswer.trim();
      allAnswers.push({
        turn: turn + 1,
        prompt: currentPrompt,
        response: finalAnswer
      });

      logger.info(`[cf45] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
      logger.info(`[cf45] 📊 Turn ${turn + 1} SUMMARY:`);
      logger.info(`[cf45] 🔑 Session ID at end: ${capturedSessionId || 'NOT CAPTURED'}`);
      logger.info(`[cf45] 📝 Response length: ${finalAnswer.length} chars`);
      logger.info(`[cf45] 📄 Response preview: ${finalAnswer.substring(0, 200)}...`);
      logger.info(`[cf45] 🔍 Supplier found: ${!isSupplierUnknown(finalAnswer)}`);
      logger.info(`[cf45] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);

      // Check if we have a good answer (supplier found)
      if (!isSupplierUnknown(finalAnswer)) {
        logger.info(`[cf45] ✅ Supplier found on turn ${turn + 1}`);
        break; // Exit conversation loop
      }

      // CHECK FOR FALLBACK CONDITION (Unknown / Low Confidence)
      // If agent says "Unknown" or similar, AND we haven't reached max turns, trigger fallback prompt
      if (turn < MAX_CONVERSATION_TURNS - 1) {
        const lowerAnswer = agentAnswer.toLowerCase();
        if (lowerAnswer.includes("unknown") || lowerAnswer.includes("cannot find") || lowerAnswer.includes("cant find")) {
          logger.info(`[cf45] ⚠️ Agent returned Unknown/Unsure. Triggering probability ranking prompt.`);
          currentPrompt = `...`;
        } else {
          // Logic to stop early if answer seems good
          if (lowerAnswer.includes("supplier_name:")) {
            if (!lowerAnswer.includes("unknown")) {
              logger.info(`[cf45] ✅ Found structured answer. Stopping loop early.`);
              break;
            }
          }
        }
      }
    }

    // Use the latest (final) response for parsing
    finalAnswer = allAnswers[allAnswers.length - 1].response;


    // 4. Parse and save results from final answer
    const upd = {};
    logger.info("[cf45] Processing supplier response.");
    const suppMatch = finalAnswer.match( /.*/);

    if (suppMatch && suppMatch[1]) {
      const value = suppMatch[1].trim();
      if (value.toLowerCase() !== 'unknown' && !value.startsWith('*')) {
        if (isMaterial) upd.supplier_name = value;
        else upd.manufacturer_name = value;

        upd.supplier_found = true;
      } else {
        logger.warn("[cf45] Supplier marked as Unknown in response.");
      }
    } else {
      logger.warn("[cf45] No valid supplier name found in response format.");
    }

    // --- Enhanced Logging & Summarization Call ---
    // --- Enhanced Logging & Summarization Call ---
    // Prepare data for logAIReasoning
    const reasoningText = currentReasoningSteps.join('\n');
    const saveUrlList = Array.from(foundUrls);

    // Call logAIReasoning
    try {
      await logAIReasoning({
        sys: SYS_APCFSF, // We should pass the system prompt used
        user: initialUserPrompt, // Ensure this variable is available in scope
        thoughts: reasoningText, // The full agent interaction log
        answer: finalAnswer,
        cloudfunction: 'cf45',
        productId: isMaterial ? undefined : targetRef.id,
        materialId: isMaterial ? targetRef.id : undefined,
      });

      // Also call saveURLs explicitly to capture the found URLs
      await saveURLs({
        urls: saveUrlList,
        materialId: isMaterial ? targetRef.id : undefined,
        productId: isMaterial ? undefined : targetRef.id,
        sys: SYS_APCFSF,
        user: initialUserPrompt,
        thoughts: reasoningText,
        answer: finalAnswer,
        cloudfunction: 'cf45',
        mSupplierData: isMaterial,
        pSupplierData: !isMaterial
      });
    } catch (logErr) {
      logger.error(`[cf45] Failed to call logAIReasoning: ${logErr.message}`);
    }

    if (Object.keys(upd).length > 0) {
      await targetRef.update(upd);
      logger.info(`[cf45] Updated ${targetRef.id} with: ${JSON.stringify(upd)}`);
    }

    // 5. Log Transaction
    await logAITransactionAgent({
      cfName: 'cf45',
      productId: isMaterial ? undefined : targetRef.id,
      materialId: isMaterial ? targetRef.id : undefined,
      events: fullEvents,
      usage: aggregatedUsage,
      model: lastUsedModel
    });

    // 6. Save Full Trace
    await targetRef.update({
      fullEventsAgent: JSON.stringify(fullEvents),
      apcfSupplierFinder_done: true,
      agent_last_response: finalAnswer,
      agent_last_response_at: admin.firestore.Timestamp.now(),
      conversation_turns_used: allAnswers.length
    });

    res.status(200).send({
      status: "success",
      fullEventsAgent: fullEvents,
      answer: finalAnswer
    });

  } catch (err) {
    logger.error("[cf45] Uncaught error:", err);
    res.status(500).send({ error: err.message });
  }
});