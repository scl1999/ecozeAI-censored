let geminiCli;
function getGeminiClient() {
  if (!geminiCli) {
    // This initializes the client for Vertex AI.
    geminiCli = new GoogleGenAI({
      vertexai: true,
      project: process.env.GCP_PROJECT_ID || '...',
      location: 'global',
      apiVersion: 'v1beta1',
    });
  }
  return geminiCli;
}

async function runGeminiWithModelEscalation({
  primaryModel,
  secondaryModel,
  generationConfig,
  user,
  collectedUrls = new Set(),
  escalationCondition, // <-- NEW: Custom check function
  cloudfunction,
}) {
  // --- 1. First Attempt with the Primary Model ---
  logger.info(`[ModelEscalation] Attempting with primary model: ${primaryModel}`);
  const primaryGenConfig = {
    ...generationConfig,
    thinkingConfig: { ...generationConfig.thinkingConfig, thinkingBudget: 24576 },
  };
  const primaryResult = await runGeminiStream({
    model: primaryModel,
    generationConfig: primaryGenConfig,
    user,
    collectedUrls,
  });

  // --- 2. Check if the Primary Attempt Succeeded using the specific condition ---

  // By default, escalate if the response is just "Unknown"
  const defaultCondition = (text) => /^Unknown$/i.test(text.trim());

  // Use the custom escalationCondition if provided, otherwise use the default.
  const needsEscalation = escalationCondition
    ? escalationCondition(primaryResult.answer)
    : defaultCondition(primaryResult.answer);

  if (!needsEscalation) {
    logger.info(`[ModelEscalation] Primary model ${primaryModel} succeeded based on its condition.`);
    return {
      answer: primaryResult.answer,
      modelUsed: primaryModel,
      thoughts: primaryResult.thoughts,
      cost: calculateCost(primaryModel, primaryResult.totalTokens),
      flashTks: primaryResult.totalTokens,
      proTks: null,
      searchQueries: primaryResult.searchQueries,
      rawConversation: primaryResult.rawConversation,
    };
  }

  // --- 3. If Primary Failed, Escalate to the Secondary Model ---
  logger.warn(`[ModelEscalation] Escalation condition met for ${primaryModel}. Escalating to ${secondaryModel}.`);

  const secondaryGenConfig = {
    ...generationConfig,
    thinkingConfig: { ...generationConfig.thinkingConfig, thinkingBudget: 32768 },
  };

  const secondaryResult = await runGeminiStream({
    model: secondaryModel,
    generationConfig: secondaryGenConfig,
    user,
    collectedUrls,
  });

  const secondaryFailed = escalationCondition
    ? escalationCondition(secondaryResult.answer)
    : defaultCondition(secondaryResult.answer);

  const escalationLog = {
    cloudfunction: cloudfunction,
    escalationWorked: !secondaryFailed,
    primaryModel: primaryModel,
    secondaryModel: secondaryModel,
    primaryResponse: primaryResult.answer,
    secondaryResponse: secondaryResult.answer,
    createdAt: admin.firestore.FieldValue.serverTimestamp()
  };
  await db.collection("modelEscalation").add(escalationLog);
  logger.info(`[ModelEscalation] Logged escalation outcome for ${cloudfunction}. Worked: ${!secondaryFailed}`);


  // --- 4. Aggregate Results ---
  const totalCost = calculateCost(primaryModel, primaryResult.totalTokens) + calculateCost(secondaryModel, secondaryResult.totalTokens);
  const combinedQueries = new Set([...primaryResult.searchQueries, ...secondaryResult.searchQueries]);
  const combinedRawConversation = [...primaryResult.rawConversation, ...secondaryResult.rawConversation];

  logger.info(`[ModelEscalation] Secondary model ${secondaryModel} finished.`);

  return {
    answer: secondaryResult.answer,
    modelUsed: secondaryModel,
    thoughts: secondaryResult.thoughts,
    cost: totalCost,
    flashTks: primaryResult.totalTokens,
    proTks: secondaryResult.totalTokens,
    searchQueries: Array.from(combinedQueries),
    rawConversation: combinedRawConversation,
  };
}

async function runGeminiStream({
  model,
  generationConfig,
  user,
  collectedUrls = new Set(),
}) {
  // NEW: Inject Date into Prompt
  const dateStr = getFormattedDate();
  const effectiveUser = `[Today is ${dateStr}]\n\n${user}`;

  // 1. Handle OpenAI/DeepSeek models via the compatible client
  if (model.startsWith("gpt-oss") || model.startsWith("openai/") || model.startsWith("deepseek/")) {
    return await runOpenModelStream({ model, generationConfig, user: effectiveUser });
  }

  const ai = getGeminiClient();
  const contents = [{ role: 'user', parts: [{ text: effectiveUser }] }];
  const sys = generationConfig.systemInstruction?.parts?.[0]?.text || generationConfig.systemInstruction || '(No system prompt)';
  let totalUrlContextTks = 0; // NEW: Track URL context tokens

  // --- START: Gemini 3.0 Compatibility Logic ---
  // Clone the config so we don't mutate the original object passed in
  let finalConfig = JSON.parse(JSON.stringify(generationConfig));

  // NORMALIZATION: Ensure systemInstruction is an object (Gemini API requirement for generateContentStream)
  // This handles cases where older code passes a string (like VERIFY_SYS_MSG) instead of {parts: [{text: ...}]}
  if (finalConfig.systemInstruction && typeof finalConfig.systemInstruction === 'string') {
    logger.info(`[runGeminiStream] Auto-converting string systemInstruction to object format for model ${model}.`);
    finalConfig.systemInstruction = { parts: [{ text: finalConfig.systemInstruction }] };
  }

  // Gemini 3 uses 'thinkingLevel' (HIGH/LOW) instead of 'thinkingBudget' (token count)
  if (model.includes('gemini-3')) {
    if (finalConfig.thinkingConfig) {
      // Remove budget if present
      delete finalConfig.thinkingConfig.thinkingBudget;
      // Default to HIGH if not specified, as budget doesn't apply here
      if (!finalConfig.thinkingConfig.thinkingLevel) {
        finalConfig.thinkingConfig.thinkingLevel = "HIGH";
      } else {
        // Normalize to uppercase
        finalConfig.thinkingConfig.thinkingLevel = String(finalConfig.thinkingConfig.thinkingLevel).toUpperCase();
      }
    }
  }
  // Gemini 2.5 uses 'thinkingBudget'
  else if (model.includes('gemini-2.5')) {
    if (finalConfig.thinkingConfig) {
      // Ensure we don't pass thinkingLevel to 2.5
      delete finalConfig.thinkingConfig.thinkingLevel;
      // Ensure a budget exists if thinking is enabled
      if (!finalConfig.thinkingConfig.thinkingBudget) {
        finalConfig.thinkingConfig.thinkingBudget = 24576; // Default budget for 2.5
      }
    }
  }
  // --- END: Gemini 3.0 Compatibility Logic ---

  const collectedQueries = new Set();
  const rawChunks = [];

  // 2. Calculate Input Tokens
  const { totalTokens: inputTks } = await runWithRetry(() => ai.models.countTokens({
    model,
    systemInstruction: finalConfig.systemInstruction,
    contents,
    tools: finalConfig.tools,
  }));

  return await runWithRetry(async () => {
    let answer = "";
    let thoughts = "";
    rawChunks.length = 0;
    collectedQueries.clear();
    const attemptUrls = new Set();
    const textCandidateUrls = new Set();

    const streamResult = await ai.models.generateContentStream({
      model,
      contents,
      config: finalConfig,
    });

    for await (const chunk of streamResult) {
      rawChunks.push(chunk);

      if (chunk.candidates && chunk.candidates.length > 0) {
        for (const candidate of chunk.candidates) {
          if (candidate.content && candidate.content.parts && candidate.content.parts.length > 0) {
            for (const part of candidate.content.parts) {
              if (part.text) {
                answer += part.text;
              } else if (part.functionCall) {
                thoughts += `\n--- TOOL CALL ---\n${JSON.stringify(part.functionCall, null, 2)}\n`;
              } else {
                const thoughtText = JSON.stringify(part, null, 2);
                if (thoughtText !== '{}') {
                  thoughts += `\n--- AI THOUGHT ---\n${thoughtText}\n`;
                }
              }
            }
          }
          // Harvest Search Queries
          const gm = candidate.groundingMetadata;
          if (gm?.webSearchQueries && gm.webSearchQueries.length > 0) {
            thoughts += `\n--- SEARCH QUERIES ---\n${gm.webSearchQueries.join("\n")}\n`;
            gm.webSearchQueries.forEach(q => collectedQueries.add(q));
          }
        }
      }
      harvestMetadataUrls(chunk, attemptUrls);
      harvestFallbackTextUrls(chunk, textCandidateUrls);
    }

    // Fix for skipped/truncated URLs: Scan the full accumulated text
    harvestUrlsFromText(answer, textCandidateUrls);

    // Validate Text URLs (concurrently)
    if (textCandidateUrls.size > 0) {
      logger.info(`[runGeminiStream] Verifying ${textCandidateUrls.size} text-extracted URLs...`);
      const checks = Array.from(textCandidateUrls).map(async url => {
        // Only return if valid (200 OK via HEAD)
        return (await isValidUrl(url)) ? url : null;
      });

      const validUrls = await Promise.all(checks);
      validUrls.forEach(u => {
        if (u) attemptUrls.add(u);
      });
    }

    attemptUrls.forEach(url => collectedUrls.add(url));

    // Calculate Output Tokens
    const { totalTokens: outputTks } = await runWithRetry(() => ai.models.countTokens({
      model,
      contents: [{ role: 'model', parts: [{ text: answer }] }]
    }));

    // Calculate Tool/Thinking Tokens
    const { totalTokens: toolCallTks } = await runWithRetry(() => ai.models.countTokens({
      model,
      contents: [{ role: 'model', parts: [{ text: thoughts }] }]
    }));

    logFullConversation({
      sys: sys,
      user: user,
      thoughts: thoughts,
      answer: answer,
      generationConfig: finalConfig
    });

    const tokens = {
      input: inputTks || 0,
      output: outputTks || 0,
      toolCalls: toolCallTks || 0,
    };

    // Add specific cost logic for Gemini 3 if pricing differs (placeholder for now)
    let cost = calculateCost(model, tokens);
    const GROUNDING_COST_PER_PROMPT = 0.014;

    if (collectedQueries.size > 0) {
      cost += GROUNDING_COST_PER_PROMPT;
    }

    return {
      answer: answer.trim(),
      model: model,
      thoughts: thoughts.trim(),
      totalTokens: tokens,
      cost: cost,
      searchQueries: Array.from(collectedQueries),
      rawConversation: rawChunks,
    };
  });
}

async function runGeminiStreamBrowserUse({
  model = 'gemini-2.5-flash',
  generationConfig,
  user,
  productId,
  materialId,
  existingHistory = [],
  sysMsgAdd = "",
  collectedUrls = new Set()
}) {
  const dateStr = getFormattedDate();
  const effectiveUser = `[Today is ${dateStr}]\n\n${user}`;

  const ai = getGeminiClient();
  const sys = (generationConfig.systemInstruction?.parts?.[0]?.text || '') + "\n" + sysMsgAdd;

  // Update generation config with new system prompt and tools
  const orchestratorConfig = {
    ...generationConfig,
    systemInstruction: { parts: [{ text: sys }] },
    tools: [
      ...(generationConfig.tools || []),
      { functionDeclarations: [URL_FINDER_DECLARATION, BROWSER_USE_DECLARATION, URL_ANALYSE_DECLARATION] }
    ]
  };

  const history = [
    ...existingHistory,
    { role: 'user', parts: [{ text: effectiveUser }] }
  ];

  let totalInputTks = 0;
  let totalOutputTks = 0;
  let totalToolCallTks = 0;
  let totalGroundingCost = 0;
  let totalSubModelCost = 0; // Track costs from Tier 2 calls if possible (or estimate)

  const MAX_TURNS = 10;
  let turn = 0;
  let finalAnswer = "";
  let allThoughts = "";
  const collectedQueries = new Set();

  while (turn < MAX_TURNS) {
    turn++;
    logger.info(`[runGeminiStreamBrowserUse] Turn ${turn}/${MAX_TURNS}`);

    // Count Input Tokens
    const { totalTokens: inputTks } = await runWithRetry(() => ai.models.countTokens({
      model,
      contents: history,
      systemInstruction: orchestratorConfig.systemInstruction,
      tools: orchestratorConfig.tools,
    }));
    totalInputTks += inputTks || 0;

    const streamResult = await ai.models.generateContentStream({
      model,
      contents: history,
      config: orchestratorConfig,
    });

    let answerThisTurn = "";
    let thoughtsThisTurn = "";
    let functionCall = null;

    for await (const chunk of streamResult) {
      if (chunk.candidates && chunk.candidates.length > 0) {
        for (const candidate of chunk.candidates) {
          if (candidate.content && candidate.content.parts) {
            for (const part of candidate.content.parts) {
              if (part.text) {
                answerThisTurn += part.text;
              } else if (part.functionCall) {
                functionCall = part.functionCall;
                thoughtsThisTurn += `\n--- TOOL CALL ---\n${JSON.stringify(part.functionCall, null, 2)}\n`;
              }
            }
          }
          // Harvest Search Queries
          const gm = candidate.groundingMetadata;
          if (gm?.webSearchQueries && gm.webSearchQueries.length > 0) {
            thoughtsThisTurn += `\n--- SEARCH QUERIES ---\n${gm.webSearchQueries.join("\n")}\n`;
            gm.webSearchQueries.forEach(q => collectedQueries.add(q));
          }
        }
      }
      harvestUrls(chunk, collectedUrls);
    }

    allThoughts += thoughtsThisTurn;

    // Count Output Tokens
    const { totalTokens: outputTks } = await runWithRetry(() => ai.models.countTokens({
      model,
      contents: [{ role: 'model', parts: [{ text: answerThisTurn + thoughtsThisTurn || " " }] }]
    }));
    totalOutputTks += outputTks || 0;

    const modelResponseParts = [];
    if (answerThisTurn) modelResponseParts.push({ text: answerThisTurn });
    if (functionCall) modelResponseParts.push({ functionCall: functionCall });

    if (modelResponseParts.length > 0) {
      history.push({ role: 'model', parts: modelResponseParts });
    }

    if (functionCall) {
      logger.info(`[runGeminiStreamBrowserUse] Executing tool: ${functionCall.name}`);
      let toolResult = "";
      try {
        if (functionCall.name === 'urlFinder') {
          toolResult = await executeUrlFinder(functionCall.args);
          try {
            const urls = JSON.parse(toolResult);
            if (Array.isArray(urls)) {
              urls.forEach(u => collectedUrls.add(u));
            }
          } catch (e) { logger.warn("[runGeminiStreamBrowserUse] Failed to parse urlFinder result for URLs", e); }
        } else if (functionCall.name === 'browserUse') {
          const { task, url } = functionCall.args;
          toolResult = await executeBrowserUse({ task, url });

          // Extract URLs from browser use result text
          // Look for http/https links in the text result
          const urlRegex = /(https?:\/\/[^\s]+)/g;
          const foundUrls = toolResult.match(urlRegex);
          if (foundUrls) {
            foundUrls.forEach(u => collectedUrls.add(u));
          }
        } else if (functionCall.name === 'urlAnalyse') {
          toolResult = await executeUrlAnalyse(functionCall.args);
        } else {
          toolResult = `Error: Unknown tool ${functionCall.name}`;
        }
      } catch (e) {
        toolResult = `Error executing tool: ${e.message}`;
      }

      history.push({
        role: 'function',
        parts: [{
          functionResponse: {
            name: functionCall.name,
            response: { result: toolResult }
          }
        }]
      });
    } else {
      finalAnswer = answerThisTurn;
      break;
    }
  }

  // Calculate Costs
  const tokens = { input: totalInputTks, output: totalOutputTks, toolCalls: totalToolCallTks };
  let cost = calculateCost(model, tokens);
  // Note: We are not tracking sub-model costs accurately here yet as runGeminiStream doesn't return cost in the simple call.
  // For now, we assume the main model cost + overhead. 
  // TODO: Improve sub-model cost tracking by returning cost from execute* functions.

  // Log Transaction
  await logAITransaction({ // Using existing logger for now, will refactor if needed
    cfName: 'runGeminiStreamBrowserUse',
    productId,
    materialId,
    cost,
    totalTokens: tokens,
    searchQueries: Array.from(collectedQueries),
    modelUsed: model,
  });

  // Log Reasoning
  await logAIReasoning({
    sys: sys,
    user: user,
    thoughts: allThoughts,
    answer: finalAnswer,
    cloudfunction: 'runGeminiStreamBrowserUse',
    productId,
    materialId,
    rawConversation: history,
  });

  return {
    answer: finalAnswer.trim(),
    thoughts: allThoughts,
    cost,
    cost,
    totalTokens: tokens,
    searchQueries: Array.from(collectedQueries),
    model: model,
    rawConversation: history
  };
}