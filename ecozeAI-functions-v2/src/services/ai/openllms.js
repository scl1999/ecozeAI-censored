async function getOpenAICompatClient() {
  // This function now ALWAYS creates a new client to ensure a fresh auth token.
  const auth = new GoogleAuth({ scopes: ["https://www.googleapis.com/auth/cloud-platform"] });
  const token = await auth.getAccessToken();
  logger.info("[getOpenAICompatClient] Successfully retrieved fresh auth token.");
  const baseURL = `https://aiplatform.googleapis.com/v1/projects/${process.env.GCP_PROJECT_ID || '...'}/locations/global/endpoints/openapi`;

  return new OpenAI({
    baseURL: baseURL,
    apiKey: token,
  });
}

// Helper to count tokens for OpenAI-compatible models
async function countOpenModelTokens({ model, messages }) {
  try {
    const ai = getGeminiClient();
    const contents = messages.map(msg => ({ role: 'user', parts: [{ text: msg.content }] }));
    const { totalTokens } = await ai.models.countTokens({ model: 'gemini-2.5-flash', contents });
    return totalTokens;
  } catch (err) {
    logger.warn(`[countOpenModelTokens] Could not count tokens for ${model}:`, err.message);
    return null;
  }
}

async function runOpenModelStream({ model, generationConfig, user }) {
  const openAIClient = await getOpenAICompatClient();
  const sys = generationConfig.systemInstruction?.parts?.[0]?.text || null;

  // 1. Construct the OpenAI-compatible messages array
  const messages = [];
  if (sys) {
    const reasoningLevel = "Reasoning: high\n";
    messages.push({ role: "system", content: reasoningLevel + sys });
  }
  messages.push({ role: "user", content: user });

  // 2. Call the token counter for the input prompt
  const inputTks = await countOpenModelTokens({ model, messages }) || 0;

  const requestPayload = {
    model,
    messages,
    stream: true,
    temperature: generationConfig.temperature ?? 1.0,
    max_tokens: generationConfig.maxOutputTokens ?? 32768,
  };
  logger.info("[runOpenModelStream] Sending request to OpenAI compatible endpoint:", { payload: requestPayload });

  let stream;
  try {
    stream = await runWithRetry(() => openAIClient.chat.completions.create(requestPayload));
  } catch (err) {
    logger.error("[runOpenModelStream] API call failed!", {
      errorMessage: err.message,
      errorStatus: err.status,
      errorHeaders: err.response?.headers,
      errorResponseData: err.response?.data,
    });
    throw err;
  }

  // 4. Process the streaming response
  let answer = "";
  let thoughts = "";
  const rawChunks = [];
  for await (const chunk of stream) {
    rawChunks.push(chunk);
    const delta = chunk.choices?.[0]?.delta;
    if (delta) {
      answer += delta.content || "";
      thoughts += delta.reasoning_content || "";
    }
  }
  const finalAnswer = answer.trim();

  // ADDED: A regex to find and remove the unwanted artifact strings
  const artifactRegex = /<\|start\|>assistant.*?<\|call\|>assistant/gi;
  const cleanedAnswer = finalAnswer.replace(artifactRegex, '').trim();

  // 5. Count output tokens and calculate final cost
  const outputTks = await countOpenModelTokens({ model, messages: [{ role: 'assistant', content: cleanedAnswer }] }) || 0; // Use cleaned answer for token count
  const tokens = { input: inputTks, output: outputTks };
  const cost = calculateCost(model, tokens);

  logFullConversation({
    sys: sys,
    user: user,
    thoughts: thoughts.trim(),
    answer: cleanedAnswer,
    generationConfig: generationConfig,
  });

  // 6. Return an object with the same shape as runGeminiStream's response
  return {
    // MODIFIED: Return the cleaned answer
    answer: cleanedAnswer,
    model: model,
    thoughts: thoughts.trim(),
    totalTokens: tokens,
    cost: cost,
    searchQueries: [],
    rawConversation: rawChunks,
  };
}

async function runGptOssFactCheckSF({ generatedReasoning, cleanUrls, model = 'gpt-oss-120b' }) {
  logger.info(`[runGptOssFactCheckSF] Starting 3rd fallback fact check with ${cleanUrls.length} sources.`);

  // 1. Parallel Source Analysis
  const sourceAnalysisPromises = cleanUrls.map(async (url) => {
    // Deduplication: Use existing extractWithTika helper
    const content = await extractWithTika(url);
    if (!content || content.trim().length < 50) return null; // Skip empty/failed

    const sysMsg = `Your job is to take in an AIs reasoning and a single source's / url's content for its task in finding the supplier(s) for a specific product / component / material / ingredient. You must determine and explain if the url / source provided relevant information for the AI's task. If so, give us a detailed description on the information that is useful.

Output your response in the exact following format and no other text:

*url_source_useful: [Set to "True" and no other text if the url / source contained useful information, set to "False" and no other text if otherwise.]
*description: [a detailed description on the useful information contained in the source / url. Just set to "N/A" and no other text if the url / source was not useful.]`;

    const prompt = `Here is the AIs full conversation:
--- START OF AI CONVERSATION LOG ---

${generatedReasoning}

--- END OF AI CONVERSATION LOG ---

Here is the content of the url / source:
${content.substring(0, 100000)}... [TRUNCATED]`;

    let result = null;
    let attempts = 0;
    const MAX_RETRIES = 2;

    while (attempts <= MAX_RETRIES && !result) {
      attempts++;
      try {
        const { answer } = await runOpenModelStream({
          model,
          generationConfig: { temperature: 0.1 }, // Low temp for extraction
          user: attempts === 1 ? prompt : `You did not return your result in the desired format. You must output your response in the exact following format and no other text:
"
*url_source_useful: [Set to "True" and no other text if the url / source contained useful information, set to "False" and no other text if otherwise.]
*description: [a detailed description on the useful information contained in the source / url. Just set to "N/A" and no other text if the url / source was not useful.]
"`
        });

        // Basic validation
        if (answer.includes("*url_source_useful:") && answer.includes("*description:")) {
          result = answer;
        }
      } catch (e) {
        logger.warn(`[runGptOssFactCheckSF] Source analysis attempt ${attempts} failed for ${url}: ${e.message}`);
      }
    }

    if (result && /[\n\r]*\*?url_source_useful:\s*True/i.test(result)) {
      return `URL: ${url}\n${result}`;
    }
    return null;
  });

  const analysisResults = (await Promise.all(sourceAnalysisPromises)).filter(Boolean);

  if (analysisResults.length === 0) {
    logger.warn("[runGptOssFactCheckSF] No useful sources found via Tika/GPT fallbacks.");
    return null; // Cannot proceed without evidence
  }

  // 2. Final Fact Check Aggregation
  const finalSysMsg = `Your job is to take in an AIs reasoning and sources for its task in finding the supplier(s) for a specific product / component / material / ingredient. You must fact check its response based on the data sources / URLs it used only. Note, the sources have already been summarised by AI helpers - you will receive their responses in the prompt. Rate the main AIs response with one of the following:

1. Direct Proof
Definition: The sources explicitly name the supplier as the manufacturer of this specific product model or component.

2. Strong Inference
Definition: The sources do not explicitly name the supplier for this exact SKU, but provide evidence that makes it the only logical conclusion.

3. Probable / General Partner
Definition: The supplier is a known major partner for the brand in this category, but no specific document links them to this exact product.

4. Weak / Speculative
Definition: The connection is based on general market share, rumors, outdated information, or "best guess".

5. No Evidence
Definition: The AI provided a supplier name but the cited sources do not mention the supplier, or the AI explicitly stated "Unknown".

!! Dont question if a product exists or not, your information may be out of date. !!
Return your answer in the exact following format and no other text:

*supplier_1: [the name of the 1st supplier exactly as the AI gave us]
*rating_1: ["Direct Proof", "Strong Inference", "Probable / General Partner", "Weak / Speculative" or "No Evidence" and no other text]
*rating_reasoning_1: [A paragraph explaining your reasoning as if you were addressing the AI directly.]

[...Repeat for any other suppliers given (if any given)]
*supplier_N: [the name of the Nth supplier exactly as the AI gave us]
*rating_N: ["Direct Proof", "Strong Inference", "Probable / General Partner", "Weak / Speculative" or "No Evidence" and no other text]
*rating_reasoning_N: [A paragraph explaining your reasoning as if you were addressing the AI directly]`;

  const finalPrompt = `Here is the AIs full conversation:
--- START OF AI CONVERSATION LOG ---

${generatedReasoning}

--- END OF AI CONVERSATION LOG ---

Here are the sources:
--- START OF AI Source analyser responses---

${analysisResults.join('\n\n')}

--- END  ---`;

  let attempts = 0;
  const MAX_RETRIES = 1;

  while (attempts <= MAX_RETRIES) {
    attempts++;
    try {
      const { answer } = await runOpenModelStream({
        model,
        generationConfig: { temperature: 0.1 },
        user: attempts === 1 ? finalPrompt : `...`
      });

      // Check if it looks roughly right
      if (answer.includes("*supplier_") && answer.includes("*rating_")) {
        return {
          answer,
          totalTokens: { input: 0, output: 0 },
          cost: 0
        };
      }
    } catch (e) {
      logger.warn(`[runGptOssFactCheckSF] Final aggregation attempt ${attempts} failed: ${e.message}`);
    }
  }

  return null;
}