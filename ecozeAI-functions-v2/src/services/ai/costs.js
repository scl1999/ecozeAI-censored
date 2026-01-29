function getModelPricing(model = '', inputTokens = 0) {
  const normalizedModel = model || '';

  if (normalizedModel.includes('gemini-3-pro') || normalizedModel.includes('deep-research')) {
    const tierTwo = inputTokens > 200000;
    const inputRate = (tierTwo ? 4.00 : 2.00) / 1000000;
    const outputRate = (tierTwo ? 18.00 : 12.00) / 1000000;
    return { inputRate, outputRate, toolRate: outputRate };
  }

  if (normalizedModel.includes('gemini-2.5-pro')) {
    const tierTwo = inputTokens > 200000;
    const inputRate = (tierTwo ? 2.5 : 1.25) / 1000000;
    const outputRate = (tierTwo ? 15 : 10) / 1000000;
    return { inputRate, outputRate, toolRate: outputRate };
  }

  if (normalizedModel.includes('gemini-2.5-flash-lite')) {
    const inputRate = 0.1 / 1000000;
    const outputRate = 0.4 / 1000000;
    return { inputRate, outputRate, toolRate: outputRate };
  }

  if (normalizedModel.includes('gemini-2.5-flash')) {
    const inputRate = 0.3 / 1000000;
    const outputRate = 2.5 / 1000000;
    return { inputRate, outputRate, toolRate: outputRate };
  }

  if (normalizedModel.includes('gemini-3-flash')) {
    // Gemini 3 Flash Preview has flat pricing regardless of input token count
    const inputRate = 0.5 / 1000000;
    const outputRate = 3.0 / 1000000;
    return { inputRate, outputRate, toolRate: outputRate };
  }

  if (normalizedModel.includes('gpt-oss-120b')) {
    const inputRate = 0.15 / 1000000;
    const outputRate = 0.60 / 1000000;
    return { inputRate, outputRate, toolRate: 0 };
  }



  return { inputRate: 0, outputRate: 0, toolRate: 0 };
}

function calculateCost(model, tokens = {}) {
  const { input = 0, output = 0, toolCalls = 0 } = tokens;
  const { inputRate, outputRate, toolRate } = getModelPricing(model, input);

  return (input * inputRate) + (output * outputRate) + (toolCalls * toolRate);
}

async function logAITransaction(params) {
  let { cfName, productId, materialId, eaiefId, cost, totalTokens, flashTokens, proTokens, searchQueries, modelUsed } = params;

  // ADDED: If we have a material but no product, try to find the linked product automatically.
  if (materialId && !productId) {
    try {
      const materialRef = db.collection("materials").doc(materialId);
      const materialSnap = await materialRef.get();

      if (materialSnap.exists) {
        const materialData = materialSnap.data() || {};
        // Check for the linked_product reference and its ID
        if (materialData.linked_product && materialData.linked_product.id) {
          logger.info(`[logAITransaction] Auto-detected linked product ${materialData.linked_product.id} for material ${materialId}.`);
          productId = materialData.linked_product.id; // Re-assign the productId
        }
      }
    } catch (err) {
      // Log the lookup error but don't stop the function. The original warning will still appear if this fails.
      logger.error(`[logAITransaction] Failed to look up linked product for material ${materialId}:`, err);
    }
  }

  // 1. Validate input
  if (!cfName || cost == null || (!productId && !materialId && !eaiefId)) {
    logger.error("[logAITransaction] Missing required parameters.", { cfName, cost, productId, materialId, eaiefId });
    return;
  }

  // 2. Prepare the data payload
  const logData = {
    cfName,
    totalCost: cost,
    createdAt: admin.firestore.FieldValue.serverTimestamp()
  };
  if (totalTokens) logData.totalTokens = totalTokens;
  if (flashTokens) logData.flashTokens = flashTokens;
  if (proTokens) logData.proTokens = proTokens;
  if (searchQueries && searchQueries.length > 0) logData.search_queries = searchQueries;
  if (modelUsed) logData.modelUsed = modelUsed;

  // 3. Initialize a batch write
  const batch = db.batch();

  try {
    // --- Scenario 1: A materialId is provided ---
    // This block is now more robust because `productId` will be populated if it exists.
    if (materialId) {
      const materialRef = db.collection("materials").doc(materialId);

      // A. Create a log in the material's 'm_tokens' sub-collection.
      const tokenLogRef = materialRef.collection("m_tokens").doc();
      batch.set(tokenLogRef, logData);

      // B. Increment the totalCost on the material document.
      batch.update(materialRef, { totalCost: admin.firestore.FieldValue.increment(cost) });

      // C. If a productId was also passed (or found), increment its totalCost.
      if (productId) {
        const productRef = db.collection("products_new").doc(productId);
        batch.update(productRef, { totalCost: admin.firestore.FieldValue.increment(cost) });
        logger.info(`[logAITransaction] Queued cost increment for material ${materialId} and product ${productId}.`);
      } else {
        logger.warn(`[logAITransaction] Logging cost for material ${materialId} without updating a linked product.`);
      }
    }
    // --- Scenario 2: ONLY a productId is provided ---
    else if (productId) {
      const productRef = db.collection("products_new").doc(productId);

      // A. Create a log in the product's 'pn_tokens' sub-collection.
      const tokenLogRef = productRef.collection("pn_tokens").doc();
      batch.set(tokenLogRef, logData);

      // B. Increment the totalCost on the product document.
      batch.update(productRef, { totalCost: admin.firestore.FieldValue.increment(cost) });
      logger.info(`[logAITransaction] Queued cost increment for product ${productId}.`);
    }
    else if (eaiefId) {
      const eaiefRef = db.collection("eaief_inputs").doc(eaiefId);

      // A. Create a log in the doc's 'e_tokens' sub-collection.
      const tokenLogRef = eaiefRef.collection("e_tokens").doc();
      batch.set(tokenLogRef, logData);

      // B. Increment the totalCost on the document.
      batch.update(eaiefRef, { totalCost: admin.firestore.FieldValue.increment(cost) });
      logger.info(`[logAITransaction] Queued cost increment for eaief_input ${eaiefId}.`);
    }

    // 4. Commit all operations atomically
    await batch.commit();
    logger.info(`[logAITransaction] Successfully committed cost updates for ${cfName}.`);

  } catch (error) {
    logger.error(`[logAITransaction] Failed to log transaction for ${cfName}.`, {
      error: error.message,
      productId,
      materialId,
      eaiefId
    });
  }
}

async function logAITransactionI(params) {
  let { cfName, productId, materialId, eaiefId, interactionId } = params;

  // ADDED: If we have a material but no product, try to find the linked product automatically.
  if (materialId && !productId) {
    try {
      const materialRef = db.collection("materials").doc(materialId);
      const materialSnap = await materialRef.get();

      if (materialSnap.exists) {
        const materialData = materialSnap.data() || {};
        // Check for the linked_product reference and its ID
        if (materialData.linked_product && materialData.linked_product.id) {
          logger.info(`[logAITransactionI] Auto-detected linked product ${materialData.linked_product.id} for material ${materialId}.`);
          productId = materialData.linked_product.id; // Re-assign the productId
        }
      }
    } catch (err) {
      // Log the lookup error but don't stop the function. The original warning will still appear if this fails.
      logger.error(`[logAITransactionI] Failed to look up linked product for material ${materialId}:`, err);
    }
  }

  // 1. Validate input
  if (!cfName || !interactionId || (!productId && !materialId && !eaiefId)) {
    logger.error("[logAITransactionI] Missing required parameters.", { cfName, interactionId, productId, materialId, eaiefId });
    return;
  }

  // 2. Fetch History & Calculate Costs
  let totalCost = 0;
  let totalTokens = { input: 0, output: 0 };
  let searchQueries = [];
  let modelUsed = "mixed"; // Default if multiple models or unknown

  try {
    const history = await getInteractionChain(interactionId);

    if (!history || history.length === 0) {
      logger.warn(`[logAITransactionI] No history found for ${interactionId}. Cost will be 0.`);
    } else {
      const modelsSeen = new Set();

      for (const turn of history) {
        // Note: In Interactions API, an "Interaction" output *is* the model's turn.
        // The 'input' is the user's turn. 
        // So we process each interaction in the chain.

        // Check usage metadata (on interaction or output?)
        // Assuming interaction.usage_metadata or loop over interaction.outputs
        const um = turn.usage_metadata || (turn.outputs && turn.outputs[0]?.usage_metadata);

        if (um) {
          // User Formula:
          // total input = total_tool_use_tokens + total_input_tokens
          const inputTks = (um.total_tool_use_tokens || 0) + (um.total_input_tokens || 0);

          // total output = total_output_tokens + total_thought_tokens
          const outputTks = (um.total_output_tokens || 0) + (um.total_thought_tokens || 0);

          totalTokens.input += inputTks;
          totalTokens.output += outputTks;

          // Identify Model
          const turnModel = turn.model || "gemini-2.5-flash";
          modelsSeen.add(turnModel);

          // Calculate turn cost
          let turnToolCalls = 0;

          if (turn.outputs) {
            for (const output of turn.outputs) {
              // Google Search
              if (output.grounding_metadata?.web_search_queries) {
                const qCount = output.grounding_metadata.web_search_queries.length;
                turnToolCalls += qCount;
                searchQueries.push(...output.grounding_metadata.web_search_queries);
              }
              // Vertex Search (retrieval_queries?)
              if (output.grounding_metadata?.retrieval_queries) {
                turnToolCalls += output.grounding_metadata.retrieval_queries.length;
              }
            }
          }

          const turnCost = calculateCost(turnModel, {
            input: inputTks,
            output: outputTks,
            toolCalls: turnToolCalls
          });

          totalCost += turnCost;
        }
      }

      if (modelsSeen.size === 1) {
        modelUsed = Array.from(modelsSeen)[0];
      } else if (modelsSeen.size > 1) {
        modelUsed = "mixed (" + Array.from(modelsSeen).join(", ") + ")";
      }
    }

  } catch (err) {
    logger.error(`[logAITransactionI] Error calculating cost for ${interactionId}`, err);
  }

  // 3. Prepare the data payload
  const logData = {
    cfName,
    totalCost: totalCost,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    interactionId: interactionId // Log the ID too
  };

  if (totalTokens.input || totalTokens.output) {
    logData.totalTokens = (totalTokens.input + totalTokens.output); // Sum for simple 'totalTokens' field? Or keep obj?
    // User legacy code often summed them or stored them. The original had 'totalTokens' as a number usually.
    // Let's store the total sum, but maybe explicitly inputs/outputs if schema allows.
    // Original code inputs: `totalTokens` (scalar).
    // I will sum them.
  }

  if (searchQueries.length > 0) logData.search_queries = searchQueries;
  if (modelUsed) logData.modelUsed = modelUsed;

  // 4. Initialize a batch write
  const batch = db.batch();

  try {
    // --- Scenario 1: A materialId is provided ---
    // This block is now more robust because `productId` will be populated if it exists.
    if (materialId) {
      const materialRef = db.collection("materials").doc(materialId);

      // A. Create a log in the material's 'm_tokens' sub-collection.
      const tokenLogRef = materialRef.collection("m_tokens").doc();
      batch.set(tokenLogRef, logData);

      // B. Increment the totalCost on the material document.
      batch.update(materialRef, { totalCost: admin.firestore.FieldValue.increment(totalCost) });

      // C. If a productId was also passed (or found), increment its totalCost.
      if (productId) {
        const productRef = db.collection("products_new").doc(productId);
        batch.update(productRef, { totalCost: admin.firestore.FieldValue.increment(totalCost) });
        logger.info(`[logAITransactionI] Queued cost increment for material ${materialId} and product ${productId}.`);
      } else {
        logger.warn(`[logAITransactionI] Logging cost for material ${materialId} without updating a linked product.`);
      }
    }
    // --- Scenario 2: ONLY a productId is provided ---
    else if (productId) {
      const productRef = db.collection("products_new").doc(productId);

      // A. Create a log in the product's 'pn_tokens' sub-collection.
      const tokenLogRef = productRef.collection("pn_tokens").doc();
      batch.set(tokenLogRef, logData);

      // B. Increment the totalCost on the product document.
      batch.update(productRef, { totalCost: admin.firestore.FieldValue.increment(totalCost) });
      logger.info(`[logAITransactionI] Queued cost increment for product ${productId}.`);
    }
    else if (eaiefId) {
      const eaiefRef = db.collection("eaief_inputs").doc(eaiefId);

      // A. Create a log in the doc's 'e_tokens' sub-collection.
      const tokenLogRef = eaiefRef.collection("e_tokens").doc();
      batch.set(tokenLogRef, logData);

      // B. Increment the totalCost on the document.
      batch.update(eaiefRef, { totalCost: admin.firestore.FieldValue.increment(totalCost) });
      logger.info(`[logAITransactionI] Queued cost increment for eaief_input ${eaiefId}.`);
    }

    // 5. Commit all operations atomically
    await batch.commit();
    logger.info(`[logAITransactionI] Successfully committed cost updates for ${cfName} (Cost: $${totalCost.toFixed(6)}).`);

  } catch (error) {
    logger.error(`[logAITransactionI] Failed to log transaction for ${cfName}.`, {
      error: error.message,
      productId,
      materialId,
      eaiefId
    });
  }
}

async function logAITransactionAgent(params) {
  let { cfName, productId, materialId, events, usage, model: defaultModel, costOverride } = params;

  let calculatedTotalCost = 0;
  let aggregatedTokens = {
    inputTokens: 0,
    outputTokens: 0,
    reasoningTokens: 0,
    totalTokens: 0,
    toolCalls: 0
  };

  if (events && Array.isArray(events)) {
    // Single Pass: Calculate Cost & Tokens per Event
    events.forEach(e => {
      let eventToolCalls = 0;
      let eventGoogleSearchCalls = 0;
      let eventVertexSearchCalls = 0;

      // 1. Count Tool Calls in this specific event
      if (e.content && e.content.parts) {
        e.content.parts.forEach(p => {
          if (p.function_call) {
            const fnName = p.function_call.name || "";
            if (fnName.includes("google_search") || fnName.includes("googleSearch")) {
              eventGoogleSearchCalls++;
            } else if (fnName.includes("vertex") || fnName.includes("retrieval") || fnName.includes("grounding") || fnName.includes("search_tool")) {
              // Heuristic for "Grounding with your data" if explicit tool
              eventVertexSearchCalls++;
            } else {
              eventToolCalls++; // Generic/Other tools
            }

            aggregatedTokens.toolCalls++;
          }
        });
      }

      // Check for implicitly grounded queries in metadata (if not explicit function calls)
      // Note: usage_metadata might have "grounding_metadata" with search counts
      // but often these map to the function calls above. If they are separate, we'd add logic here.
      // For now, we rely on checking function names as primary method.

      // 2. Determine Model (Event-specific > Default)
      let eventModel = defaultModel || "gemini-3-pro-preview";
      let evtUsage = e.usage_metadata;
      let foundStats = false;

      // Check top-level usage
      if (evtUsage) foundStats = true;

      // Check inside gcp_vertex_agent_llm_response
      if (e.gcp_vertex_agent_llm_response) {
        try {
          const llmRes = JSON.parse(e.gcp_vertex_agent_llm_response);
          if (llmRes.model_version) eventModel = llmRes.model_version;
          if (!foundStats && llmRes.usage_metadata) {
            evtUsage = llmRes.usage_metadata;
            foundStats = true;
          }
        } catch (err) { /* ignore */ }
      }

      // 3. Extract Tokens
      const input = (foundStats && evtUsage.prompt_token_count) || 0;
      const output = (foundStats && evtUsage.candidates_token_count) || 0;
      const reasoning = (foundStats && evtUsage.reasoning_token_count) || 0;
      const total = (foundStats && evtUsage.total_token_count) || (input + output);

      aggregatedTokens.inputTokens += input;
      aggregatedTokens.outputTokens += output;
      aggregatedTokens.reasoningTokens += reasoning;
      aggregatedTokens.totalTokens += total;

      // 4. Calculate Cost for THIS Event
      // Base Model Cost (Tokens)
      const baseCost = calculateCost(eventModel, { input, output, toolCalls: 0 }); // 0 tool calls here as we price manually below
      calculatedTotalCost += baseCost;

      // Tool Costs (Manual Addition)
      // Google Search: $14 / 1000 = $0.014 per query
      calculatedTotalCost += (eventGoogleSearchCalls * 0.014);

      // Vertex/Data Search: $2.5 / 1000 = $0.0025 per query
      calculatedTotalCost += (eventVertexSearchCalls * 0.0025);

      // Generic Tool Calls (using model's tool rate if applicable)
      if (eventToolCalls > 0) {
        const toolRate = getModelPricing(eventModel, 0).toolRate || 0;
        calculatedTotalCost += (eventToolCalls * toolRate);
      }
    });

  } else {
    // Fallback if no events array provided
    aggregatedTokens.inputTokens = (usage && usage.promptTokenCount) || 0;
    aggregatedTokens.outputTokens = (usage && usage.candidatesTokenCount) || 0;
    aggregatedTokens.reasoningTokens = (usage && usage.reasoningTokenCount) || 0;
    aggregatedTokens.totalTokens = (usage && usage.totalTokenCount) || 0;

    if (usage) {
      calculatedTotalCost = calculateCost(defaultModel, {
        input: aggregatedTokens.inputTokens,
        output: aggregatedTokens.outputTokens,
        toolCalls: 0
      });
    }
  }

  // Override
  if (costOverride !== undefined) calculatedTotalCost = costOverride;

  // 4. Prepare Log Data
  const logData = {
    cfName,
    totalCost: calculatedTotalCost,
    totalTokens: aggregatedTokens.totalTokens, // Sum of all events
    inputTokens: aggregatedTokens.inputTokens,
    outputTokens: aggregatedTokens.outputTokens,
    reasoningTokens: aggregatedTokens.reasoningTokens,
    toolCalls: aggregatedTokens.toolCalls,
    modelUsed: "MULTI_AGENT_MIXED", // Indicate mixed usage
    createdAt: admin.firestore.FieldValue.serverTimestamp()
  };

  // 5. Write to Firestore (Batch)
  const batch = db.batch();

  try {
    if (materialId) {
      const materialRef = db.collection("materials").doc(materialId);
      const tokenLogRef = materialRef.collection("m_tokens").doc();
      batch.set(tokenLogRef, logData);
      batch.update(materialRef, { totalCost: admin.firestore.FieldValue.increment(calculatedTotalCost) });

      if (productId) {
        const productRef = db.collection("products_new").doc(productId);
        batch.update(productRef, { totalCost: admin.firestore.FieldValue.increment(calculatedTotalCost) });
      }
    } else if (productId) {
      const productRef = db.collection("products_new").doc(productId);
      const tokenLogRef = productRef.collection("pn_tokens").doc();
      batch.set(tokenLogRef, logData);
      batch.update(productRef, { totalCost: admin.firestore.FieldValue.increment(calculatedTotalCost) });
    }

    await batch.commit();
    logger.info(`[logAITransactionAgent] Logged transaction for ${cfName}: ${JSON.stringify(logData)}`);

  } catch (error) {
    logger.error(`[logAITransactionAgent] Failed to log: ${error.message}`);
  }
}