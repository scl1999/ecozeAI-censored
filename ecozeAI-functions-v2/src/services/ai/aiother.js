async function verifyMaterialLinks(materialIds, expectedParentRef) {
  const MAX_RETRIES = 5;
  const RETRY_DELAY_MS = 5000;

  if (!materialIds || materialIds.length === 0) {
    return; // Nothing to verify
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    logger.info(`[verifyMaterialLinks] Verification attempt ${attempt}/${MAX_RETRIES} for ${materialIds.length} materials.`);

    const docRefs = materialIds.map(id => db.collection("materials").doc(id));
    const docSnaps = await db.getAll(...docRefs);

    let allVerified = true;
    for (const docSnap of docSnaps) {
      if (!docSnap.exists) {
        logger.warn(`[verifyMaterialLinks] Document ${docSnap.id} does not exist yet.`);
        allVerified = false;
        break;
      }
      const data = docSnap.data();
      // A material is correctly linked if its parent_material OR its linked_product matches the expected parent document.
      const hasCorrectLink = (data.parent_material && data.parent_material.path === expectedParentRef.path) ||
        (data.linked_product && data.linked_product.path === expectedParentRef.path);

      if (!hasCorrectLink) {
        logger.warn(`[verifyMaterialLinks] Document ${docSnap.id} is missing or has incorrect parent link. Expected parent: ${expectedParentRef.path}.`);
        allVerified = false;
        break;
      }
    }

    if (allVerified) {
      logger.info(`[verifyMaterialLinks] All ${materialIds.length} materials successfully verified.`);
      return; // Success! Exit the function.
    }

    if (attempt < MAX_RETRIES) {
      logger.info(`[verifyMaterialLinks] Verification failed. Retrying in ${RETRY_DELAY_MS / 1000} seconds...`);
      await sleep(RETRY_DELAY_MS);
    } else {
      logger.error(`[verifyMaterialLinks] CRITICAL: Failed to verify material links after ${MAX_RETRIES} attempts. Proceeding, but warnings may occur.`);
    }
  }
}

async function bomFactChecker(systemInstructions, userPrompt, originalBOM, urlsUsed) {
  const model = "gemini-3-pro-preview";

  const factCheckSystemPrompt = `
Your job is to fact check an AIs BOM it has built for a product / material / component / ingredient (PCMI). Flag any PCMIs which are incorrect. Only fact check the PCMIs and their names. Dont fact check the supplier names or the masses. 

!! When giving PCMI numbers (e.g. tier1_material_name_n) n MUST be the number that was assigned by the previous AI. Dont change the numbers. !!
!! Dont add any new PCMIs, you can only amend or delete. !!

Output your answer (the corrected PCMIs) in the following format and no other text:

* tier1_material_name_n: [name of the nth tier 1 material forming part of the product. Or "N/A" if the PCMI doesnt exist in the parent PCMI]
* supplier_name_n: [name of nth tier 1 supplier of tier 1 material - set as "Unknown" (and no other text) if the supplier is unknown.If the supplier is the manufacturer(brand) themselves, still enter their name here. Or "N/A" if the PCMI doesnt exist in the parent PCMI]
* description_n: [description of nth tier 1 material.Set as "Unknown" (and no other text) if the supplier is unknown like above. Or "N/A" if the PCMI doesnt exist in the parent PCMI]
* mass_n: [mass of nth tier 1 material / component - set as "Unknown" (and no other text) if the mass is unknown. Or "N/A" if the PCMI doesnt exist in the parent PCMI]

[Repeat for every incorrect material and supplier]
——
* evidence_snippets:
* evidence_1_url: [URL where the evidence was found]
* evidence_1_quote: "[verbatim quote ≤ 20 words]"
[Add more evidence_N entries if helpful] 
* evidence_snippets:
* evidence_N_url: [URL where the evidence was found]
* evidence_N_quote: "[verbatim quote ≤ 20 words]" 
——
`;

  const finalPrompt = `
Prompt given to the AI:
${userPrompt}

System Instructions given to the AI:
${systemInstructions}

BOM given by the AI:
${originalBOM}

URLs used:
${urlsUsed.join('\n')}
`;

  const vGenerationConfig = {
    temperature: 1,
    maxOutputTokens: 65535,
    systemInstruction: { parts: [{ text: factCheckSystemPrompt }] },
    tools: [{ urlContext: {} }, { googleSearch: {} }],
    thinkingConfig: {
      includeThoughts: true,
      thinkingBudget: 32768
    },
  };

  try {
    const result = await runGeminiStream({
      model: model,
      generationConfig: vGenerationConfig,
      user: finalPrompt,
    });
    return result.answer;
  } catch (error) {
    logger.error("[bomFactChecker] Error running fact checker:", error);
    throw error;
  }
}

async function factChecker({
  productId = null,
  materialId = null,
  urlsGiven = [],
  cloudfunction,
  systemInstructions,
  prompt,
  aiOutput,
  outputStructure
}) {
  logger.info(`[factChecker] Starting fact check for ${cloudfunction}...`);

  // 1. Determine FactChecker Name (Sequential)
  let factCheckerName = `${cloudfunction}FactChecker_1`;
  try {
    let collectionRef;
    if (productId) {
      collectionRef = db.collection("products_new").doc(productId).collection("pn_reasoning");
    } else if (materialId) {
      collectionRef = db.collection("materials").doc(materialId).collection("m_reasoning");
    }

    if (collectionRef) {
      const prefix = `${cloudfunction}FactChecker_`;
      const snapshot = await collectionRef
        .where('cloudfunction', '>=', prefix)
        .where('cloudfunction', '<=', prefix + '\uf8ff')
        .select('cloudfunction')
        .get();

      let maxN = 0;
      snapshot.forEach(doc => {
        const cf = doc.data().cloudfunction;
        if (cf && cf.startsWith(prefix)) {
          const part = cf.substring(prefix.length);
          const n = parseInt(part, 10);
          if (!isNaN(n) && n > maxN) maxN = n;
        }
      });
      factCheckerName = `${prefix}${maxN + 1}`;
    }
  } catch (err) {
    logger.warn(`[factChecker] Failed to determine sequential name, finding fallback...`, err);
    factCheckerName = `${cloudfunction}FactChecker_${Date.now()}`;
  }
  logger.info(`[factChecker] Assigned name: ${factCheckerName}`);

  // 2. Prepare AI Inputs (No Tika)
  const fcSystemInstructions = `
Your job is to take in the details of a task given to an AI. You must take in the information that the AI cited to back its claims and the answer of the AI. You must evaluate whether the AI's answer is supported by the cited information or not and / or it missed any information. If the AI has failed to provide an answer based on the cited data, you give it feedback on where it went wrong. Output your answer in the exact following format and no other text:

${outputStructure}
`;

  const fcUserPrompt = `
The System Instructions of the AI you are evaluating:
${systemInstructions}

The Prompt given to the AI you are evaluating:
${prompt}

The answer of the AI you are evaluating to the user's request:
${aiOutput}

URLs cited by the AI:
${Array.isArray(urlsGiven) ? urlsGiven.join('\n') : urlsGiven}
`;

  const fcGenerationConfig = {
    temperature: 1,
    maxOutputTokens: 65535, // verification
    systemInstruction: { parts: [{ text: fcSystemInstructions }] },
    tools: [{ urlContext: {} }],
    thinkingConfig: {
      includeThoughts: true,
      thinkingBudget: 24576
    }
  };

  // 3. Run AI
  let fcAnswer = "";
  let fcCost = 0;
  let fcTokens = { input: 0, output: 0, toolCalls: 0 };
  let fcModel = 'gemini-2.5-flash';
  let fcThoughts = "";
  let fcRawConv = [];

  try {
    const res1 = await runGeminiStream({
      model: fcModel,
      generationConfig: fcGenerationConfig,
      user: fcUserPrompt,
    });
    fcAnswer = res1.answer;
    fcCost = res1.cost;
    fcTokens = res1.totalTokens;
    fcModel = res1.model;
    fcThoughts = res1.thoughts;
    fcRawConv = res1.rawConversation;
  } catch (e) {
    logger.error(`[factChecker] Model execution failed: ${e.message}`);
    return { aiFCAnswer: "System Error: AI execution failed." };
  }

  // 4. Check and Retry (Dynamic Check based on outputStructure)
  // Extract keys starting with * from the structure to check if they exist in the answer
  const expectedKeys = (outputStructure.match(/\*[a-zA-Z0-9_]+:/g) || []).map(k => k.replace('*', '').replace(':', ''));
  let validationPassed = true;

  if (expectedKeys.length > 0) {
    for (const key of expectedKeys) {
      if (!fcAnswer.includes(`*${key}:`)) {
        validationPassed = false;
        break;
      }
    }
  }

  if (!validationPassed) {
    logger.warn(`[factChecker] Dynamic format check failed for ${factCheckerName}. Retrying...`);
    const retryPrompt = `Your answer did not follow the expected format. Please try again. You must output your answer in the exact following format and no other text:\n\n${outputStructure}`;

    try {
      const retryResult = await runGeminiStream({
        model: fcModel,
        generationConfig: fcGenerationConfig,
        user: `${fcUserPrompt}\n\nPrevious Invalid Response:\n${fcAnswer}\n\n${retryPrompt}`
      });

      // Merge results
      fcAnswer = retryResult.answer;
      fcThoughts += `\n\n--- RETRY ---\n${retryResult.thoughts}`;
      fcCost += retryResult.cost;
      fcTokens.input += retryResult.totalTokens.input;
      fcTokens.output += retryResult.totalTokens.output;
      if (fcTokens.toolCalls && retryResult.totalTokens.toolCalls) fcTokens.toolCalls += retryResult.totalTokens.toolCalls;
      fcRawConv.push(...retryResult.rawConversation);
    } catch (e) {
      logger.error(`[factChecker] Retry failed: ${e.message}`);
    }
  }

  // 5. Log Transaction & Reasoning
  await logAITransaction({
    cfName: factCheckerName,
    productId,
    materialId,
    cost: fcCost,
    totalTokens: fcTokens,
    modelUsed: fcModel
  });

  await logAIReasoning({
    sys: fcSystemInstructions,
    user: fcUserPrompt,
    thoughts: fcThoughts,
    answer: fcAnswer,
    cloudfunction: factCheckerName,
    productId,
    materialId,
    rawConversation: fcRawConv,
  });

  // 7. Return Value (Modified to return raw string as requested)
  /*
  5. The return value of the helper function needs to be:
  aiFCAnswer (String) = {the answer of the fact checker AI}
  */
  return {
    aiFCAnswer: fcAnswer
  };
}