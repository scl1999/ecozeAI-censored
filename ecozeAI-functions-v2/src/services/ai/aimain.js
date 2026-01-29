async function runAIChat({
  interactionId, // identifying the PREVIOUS interaction to continue from, if any
  prompt,
  systemInstructions,
  model = "gemini-2.5-flash",
  temperature,
  maxOutputTokens, // interactions.create config?
  thinkingLevel, // interactions.create config?
  tools // tools config
}) {

  const actualModel = model || "gemini-2.5-flash";

  // 1. Handle OpenAI/DeepSeek models via the compatible client
  // Adapter for unsupported models in Interactions API
  if (actualModel.startsWith("gpt-oss") || actualModel.startsWith("openai/") || actualModel.startsWith("deepseek/")) {
    const { getFormattedDate } = require('./utils'); // Ensure imported if needed, or assume global as per context. Actually getFormattedDate seems global in this file.
    const dateStr = (typeof getFormattedDate === 'function') ? getFormattedDate() : new Date().toISOString();
    const effectiveUser = `[Today is ${dateStr}]\n\n${prompt}`;
    return await runOpenModelStream({ model: actualModel, generationConfig: { temperature, maxOutputTokens }, user: effectiveUser });
  }

  // Prepare tools config if needed
  let toolConfig = null;
  if (tools && tools.length > 0) {
    toolConfig = tools;
  }

  // Construct Input
  let inputContent = prompt;
  if (systemInstructions && !interactionId) {
    inputContent = `${systemInstructions}\n\n${prompt}`;
  }

  return runWithRetryI(async () => {
    const ai = getGeminiClient();

    logger.info(`[runAIChat] Calling client.interactions.create with model ${actualModel}...`);

    const createParams = {
      model: actualModel,
      input: inputContent,
    };

    if (interactionId) {
      createParams.previous_interaction_id = interactionId;
      logger.info(`[runAIChat] Continuing conversation from ${interactionId}`);
    } else {
      logger.info(`[runAIChat] Starting NEW interaction chain.`);
    }

    // Add config if supported by SDK. 
    // Trying standard config patterns.
    if (temperature || maxOutputTokens) {
      createParams.generation_config = {};
      if (temperature) createParams.generation_config.temperature = temperature;
      if (maxOutputTokens) createParams.generation_config.max_output_tokens = maxOutputTokens;
    }

    if (toolConfig) {
      createParams.tools = toolConfig;
    }

    const interaction = await ai.interactions.create(createParams);

    // Parse Output
    // User example: interaction.outputs[interaction.outputs.length - 1].text

    let finalAnswer = "";
    let thoughts = "";
    const collectedUrls = new Set();
    const collectedQueries = new Set();
    let usageMetadata = null;

    if (interaction.outputs) {
      for (const output of interaction.outputs) {
        if (output.text) {
          finalAnswer += output.text;
        }

        // Usage (check interaction.usage_metadata or output.usage_metadata)
        if (output.usage_metadata) usageMetadata = output.usage_metadata;

        // Grounding
        if (output.grounding_metadata) {
          const gm = output.grounding_metadata;
          if (gm.web_search_queries) {
            gm.web_search_queries.forEach(q => collectedQueries.add(q));
          }
          if (gm.grounding_chunks) {
            gm.grounding_chunks.forEach(gc => {
              if (gc.web?.uri) collectedUrls.add(gc.web.uri);
            });
          }
        }

        // Thoughts/Tools?
        if (output.function_call) {
          thoughts += `\n--- TOOL CALL ---\n${JSON.stringify(output.function_call, null, 2)}\n`;
        }
      }
    }

    // Fallback: Check if the interaction object itself has aggregated fields
    if (interaction.usage_metadata && !usageMetadata) {
      usageMetadata = interaction.usage_metadata;
    }

    const returnId = interaction.id; // The NEW ID

    logger.info(`[runAIChat] Completed interaction ${returnId}.`);

    if (finalAnswer) {
      logger.info(`\nFinal Answer:\n${finalAnswer.trim()}`);
    }

    return {
      "finalAnswer": finalAnswer.trim(),
      "thoughts": thoughts.trim(),
      "urlsUsed": Array.from(collectedUrls),
      "searchQueries": Array.from(collectedQueries),
      "interactionId": returnId, // Return the NEW ID for next turn
      "usageMetadata": usageMetadata
    };
  });
}


async function runChatLoop({
  model,
  generationConfig,
  initialPrompt,
  followUpPrompt,
  maxFollowUps = FOLLOWUP_LIMIT,
  existingHistory = [],
  collectedUrls,
  onTurnComplete // Optional callback for incremental persistence
}) {
  const ai = getGeminiClient();
  const chat = ai.chats.create({
    model,
    history: existingHistory,
    config: generationConfig,
  });
  const sys = generationConfig.systemInstruction?.parts?.[0]?.text || '(No system prompt)';
  const collectedQueries = new Set();

  // --- Token Accumulators ---
  let totalInputTks = 0;
  let totalOutputTks = 0;
  let totalToolCallTks = 0;
  let totalGroundingCost = 0;
  const GROUNDING_COST_PER_PROMPT = 0.035;
  const allRawChunks = [];
  const allTurnsForLog = [];



  logger.info("\n==================================================");
  logger.info("======= 💬 FULL CHAT CONVERSATION 💬 =======");
  logger.info("==================================================\n");
  logger.info("---------- ⚙️ SYSTEM MESSAGE ----------");
  logger.info(sys);
  logger.info("----------------------------------------\n");

  if (generationConfig) {
    logger.info("---------- 🛠️ GENERATION CONFIG ----------");
    logger.info(JSON.stringify(generationConfig, null, 2));
    logger.info("-----------------------------------------\n");
  }

  if (existingHistory && existingHistory.length > 0) {
    logger.info("---------- 📜 RESUMED HISTORY ----------");
    existingHistory.forEach(turn => {
      const role = turn.role === 'user' ? '👤 USER' : '🤖 AI';
      const text = turn.parts.map(p => p.text || JSON.stringify(p.functionCall)).join('\n');
      logger.info(`\n[${role}]`);
      logger.info(text);
    });
    logger.info("---------------------------------------\n");
  }

  const dateStr = getFormattedDate();
  let currentPrompt = `[Today is ${dateStr}]\n\n${initialPrompt}`;
  const allAnswers = [];

  for (let i = 0; i <= maxFollowUps; i++) {
    // --- Count input tokens for this turn ---
    const historyBeforeSend = await chat.getHistory();
    const currentTurnPayload = [
      ...historyBeforeSend,
      { role: 'user', parts: [{ text: currentPrompt }] }
    ];

    const { totalTokens: currentInputTks } = await runWithRetry(() => ai.models.countTokens({
      model,
      contents: currentTurnPayload,
      systemInstruction: generationConfig.systemInstruction, // System prompt is part of every call
      tools: generationConfig.tools, // Tools are part of every call
    }));
    totalInputTks += currentInputTks || 0;

    logger.info(`---------- 👤 USER MESSAGE (Turn ${i + 1}) ----------`);
    logger.info(currentPrompt);
    logger.info("------------------------------------------------\n");

    const streamResult = await runWithRetry(() =>
      chat.sendMessageStream({ message: currentPrompt })
    );

    let answerThisTurn = "";
    let thoughtsThisTurn = "";
    let groundingUsedThisTurn = false;
    const rawChunksThisTurn = [];

    for await (const chunk of streamResult) {
      rawChunksThisTurn.push(chunk);
      harvestUrls(chunk, collectedUrls);

      if (chunk.candidates && chunk.candidates.length > 0) {
        for (const candidate of chunk.candidates) {
          // 1. Process content parts for text and function calls
          if (candidate.content && candidate.content.parts && candidate.content.parts.length > 0) {
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

          // 2. Process grounding metadata for search queries
          const gm = candidate.groundingMetadata;

          // 1. Collect Search Queries
          if (gm?.webSearchQueries?.length) {
            thoughtsThisTurn += `\n--- SEARCH QUERIES ---\n${gm.webSearchQueries.join("\n")}\n`;
            gm.webSearchQueries.forEach(q => collectedQueries.add(q));
            groundingUsedThisTurn = true;
          }

          // 2. Collect URLs from Grounding Chunks (Standard Gemini Grounding)
          if (gm?.groundingChunks?.length) {
            gm.groundingChunks.forEach(chunk => {
              if (chunk.web?.uri) {
                collectedUrls.add(chunk.web.uri);
                groundingUsedThisTurn = true;
              }
            });
          }

          // 3. Fallback/Alternative: groundingSupports (sometimes used in older/other contexts)
          if (gm?.groundingSupports?.length) {
            gm.groundingSupports.forEach(support => {
              support.groundingChunkIndices?.forEach(idx => {
                const chunk = gm.groundingChunks?.[idx];
                if (chunk?.web?.uri) {
                  collectedUrls.add(chunk.web.uri);
                }
              });
            });
          }
        }
      } else if (chunk.text) {
        // Fallback for simple chunks that only contain text at the top level
        answerThisTurn += chunk.text;
      }
    }
    await streamResult.response;
    if (groundingUsedThisTurn) {
      totalGroundingCost += GROUNDING_COST_PER_PROMPT;
      logger.info(`[runChatLoop] Grounding used in Turn ${i + 1}. Accumulated grounding cost: $${totalGroundingCost}`);
    }

    allRawChunks.push(...rawChunksThisTurn);

    const trimmedAnswer = answerThisTurn.trim();

    // --- Count output and tool call tokens for this turn ---
    const { totalTokens: currentOutputTks } = await runWithRetry(() => ai.models.countTokens({
      model,
      contents: [{ role: 'model', parts: [{ text: trimmedAnswer }] }]
    }));
    totalOutputTks += currentOutputTks || 0;

    const { totalTokens: currentToolCallTks } = await runWithRetry(() => ai.models.countTokens({
      model,
      contents: [{ role: 'model', parts: [{ text: thoughtsThisTurn }] }]
    }));
    totalToolCallTks += currentToolCallTks || 0;

    // --- DEBUG: History tracking ---
    const debugHistory = await chat.getHistory();
    logger.info(`[runChatLoop] Turn ${i + 1} finished (before local push). SDK History Length: ${debugHistory.length}`);


    // Log the user prompt for this turn
    allTurnsForLog.push(`--- 👤 User ---\n${currentPrompt}`);
    // Log the AI thoughts/tools and text response for this turn
    const aiTurnLog = [thoughtsThisTurn.trim(), trimmedAnswer].filter(Boolean).join('\n\n');
    allTurnsForLog.push(`--- 🤖 AI ---\n${aiTurnLog}`);

    if (thoughtsThisTurn.trim()) {
      logger.info(`---------- 🤔 AI THOUGHTS (Turn ${i + 1}) ----------`);
      logger.info(thoughtsThisTurn.trim());
      logger.info("------------------------------------------------\n");
    }
    logger.info(`---------- 🤖 AI MESSAGE (Turn ${i + 1}) ----------`);
    logger.info(trimmedAnswer);
    logger.info("----------------------------------------------\n");

    // Check if "Done" is present at the end
    const containsDone = /(?:^|\n)\s*done[.!]*\s*$/i.test(trimmedAnswer);
    // Check if there are new materials in the response
    const hasNewMaterials = /\*tier1_material_name_\d+:/i.test(trimmedAnswer);

    // Only consider it "done" if "Done" is present AND there are no new materials
    const isDone = containsDone && !hasNewMaterials;

    if (containsDone && hasNewMaterials) {
      logger.info(`[runChatLoop] Turn ${i + 1}: "Done" detected but new materials found. Continuing loop.`);
    }

    if (isDone || i === maxFollowUps) {
      if (i === maxFollowUps && !isDone) {
        allAnswers.push(trimmedAnswer);
      }
      break;
    }
    allAnswers.push(trimmedAnswer);

    // Incremental Persistence Check
    if (onTurnComplete) {
      const currentHistory = await chat.getHistory();
      await onTurnComplete(currentHistory, trimmedAnswer);
    }

    currentPrompt = followUpPrompt;
  }

  logger.info("==================================================");
  logger.info("============== END CONVERSATION =============");
  logger.info("==================================================\n");

  const finalHistory = await chat.getHistory();
  const aggregatedAnswer = allAnswers.join('\n\n');

  logger.info(`[runChatLoop] 🪙 Final Token Counts: Input=${totalInputTks}, Output=${totalOutputTks}, ToolCalls=${totalToolCallTks}`);

  const tokens = {
    input: totalInputTks,
    output: totalOutputTks,
    toolCalls: totalToolCallTks,
  };

  const tokenCost = calculateCost(model, tokens);
  const cost = tokenCost + totalGroundingCost;

  logger.info(`[runChatLoop] 🪙 Final Token Counts: Input=${tokens.input}, Output=${tokens.output}, ToolCalls=${tokens.toolCalls}`);
  logger.info(`[runChatLoop] 🪙 Final Costs: Tokens=$${tokenCost.toFixed(6)}, Grounding=$${totalGroundingCost.toFixed(6)}, Total=$${cost.toFixed(6)}`);

  return {
    finalAnswer: aggregatedAnswer,
    model: model,
    history: finalHistory,
    logForReasoning: allTurnsForLog.join('\n\n'),
    tokens: tokens,
    cost: cost,
    searchQueries: Array.from(collectedQueries),
    rawConversation: allRawChunks,
  };
}

async function persistHistory({ docRef, history, loop, wipeNow = false }) {
  // rolling copy while the loop is running
  await docRef.update({
    z_ai_history: JSON.stringify(history),
    ai_loop: loop,
  });

  if (wipeNow) {
    // decide sub-collection automatically
    const topLevel = docRef.path.split('/')[0];           // 'materials' | 'products_new'
    const archiveCol =
      topLevel === 'materials' ? 'm_ai_archives' :
        topLevel === 'products_new' ? 'p_ai_archives' :
          'ai_archives';

    await docRef.collection(archiveCol).add({
      finishedAt: admin.firestore.FieldValue.serverTimestamp(),
      z_ai_history: JSON.stringify(history),
      loops: loop,
    });

    // delete live copy so the next run starts fresh
    await docRef.update({ z_ai_history: admin.firestore.FieldValue.delete() });
  }
}

async function incrementChildProgress(materialId, cloudFunctionName) {
  if (!materialId) return;
  try {
    const mRef = db.collection("materials").doc(materialId);
    const mSnap = await mRef.get();
    if (!mSnap.exists) return;
    const mData = mSnap.data();

    let targetRef;
    if (mData.parent_material) {
      targetRef = mData.parent_material;
    } else if (mData.linked_product) {
      targetRef = mData.linked_product;
    } else {
      return;
    }

    await db.runTransaction(async (t) => {
      const tSnap = await t.get(targetRef);
      if (!tSnap.exists) return;
      const tData = tSnap.data();
      let progress = tData.child_materials_progress || [];

      let found = false;
      const newProgress = progress.map(item => {
        if (item.cloudfunction === cloudFunctionName) {
          found = true;
          return { ...item, number_done: (item.number_done || 0) + 1 };
        }
        return item;
      });

      if (!found) {
        newProgress.push({ cloudfunction: cloudFunctionName, number_done: 1 });
      }

      t.update(targetRef, { child_materials_progress: newProgress });
    });
  } catch (err) {
    console.error(`[incrementChildProgress] Failed for ${materialId}:`, err);
  }
}

async function runPromisesInParallelWithRetry(
  promiseFactories,
  maxRetries = 3,
  baseDelayMs = 20000 // A long initial delay for system-wide rate limits
) {
  let attempts = 0;
  let remainingFactories = [...promiseFactories];

  while (attempts < maxRetries && remainingFactories.length > 0) {
    attempts++;
    const promises = remainingFactories.map(factory => factory());
    const results = await Promise.allSettled(promises);

    const failedFactories = [];
    results.forEach((result, index) => {
      if (result.status === 'rejected') {
        // Check specifically for the 429 rate limit error
        if (result.reason && result.reason.status === 429) {
          failedFactories.push(remainingFactories[index]);
        } else {
          // For other errors, just log them but don't retry
          logger.error(`[runPromisesInParallelWithRetry] A non-retriable error occurred:`, result.reason);
        }
      }
    });

    if (failedFactories.length === 0) {
      logger.info(`[runPromisesInParallelWithRetry] All promises succeeded on attempt ${attempts}.`);
      return; // Success
    }

    remainingFactories = failedFactories;
    logger.warn(`[runPromisesInParallelWithRetry] Attempt ${attempts} failed for ${remainingFactories.length} promises due to rate limiting.`);

    if (attempts < maxRetries) {
      const backoff = Math.pow(2, attempts - 1);
      const jitter = Math.random() * 5000;
      const delay = (baseDelayMs * backoff) + jitter;
      logger.info(`[runPromisesInParallelWithRetry] Waiting for ~${Math.round(delay / 1000)}s before retrying...`);
      await sleep(delay);
    }
  }

  if (remainingFactories.length > 0) {
    logger.error(`[runPromisesInParallelWithRetry] CRITICAL: Failed to execute ${remainingFactories.length} promises after ${maxRetries} attempts.`);
  }
}