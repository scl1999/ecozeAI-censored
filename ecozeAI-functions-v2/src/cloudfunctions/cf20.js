//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf20 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf20] Invoked");
  try {
    const { eId } = req.body;
    if (!eId) {
      res.status(400).json({ error: "eId (eaief_inputs document ID) is required." });
      return;
    }

    const eDocRef = db.collection("eaief_inputs").doc(eId);
    const eDocSnap = await eDocRef.get();
    if (!eDocSnap.exists) {
      res.status(404).json({ error: `eaief_inputs document ${eId} not found.` });
      return;
    }
    const eDocData = eDocSnap.data() || {};
    const productName = eDocData.productName_input;
    // AMENDED: Fetch otherMetrics and UNSPSC data
    const shouldCalculateOtherMetrics = eDocData.otherMetrics === true;
    const unspscData = {
      key: eDocData.unspsc_key,
      parentKey: eDocData.unspsc_parent_key,
      code: eDocData.unspsc_code,
    };

    // --- Get the original product ID for correct logging ---
    const pDocRef = eDocData.product;
    const originalProductId = pDocRef ? pDocRef.id : null;
    if (!originalProductId) {
      throw new Error(`The triggering eaief_inputs document ${eId} is missing its reference to the original product.`);
    }

    // --- Aggregators for single, final logging ---
    let totalInputTokens = 0;
    let totalOutputTokens = 0;
    let totalToolCallTokens = 0;
    const allFormattedTurns = [];
    const allSearchQueries = new Set();
    const allRawChunks = [];
    const allCollectedUrls = new Set();
    const allAnnotatedAnswers = [];
    const allSources = [];
    const urlCitationMap = new Map();
    let citationCounter = 1;

    // --- AI Chat Loop Setup ---
    const maxProducts = 25;
    let currentNumProducts = 0;
    const allNewProductEntries = [];
    const allDocumentsForDatastore = [];

    const vGenerationConfig = {
//
//
//
//
        { googleSearch: {} },
        {
          retrieval: {
            vertexAiSearch: {
              datastore: 'projects/projectId/locations/global/collections/default_collection/dataStores/ecoze-ai-products-datastore_1755024362755',
            },
          },
        }
      ],
//
    };

    const ai = getGeminiClient();
    const chat = ai.chats.create({
      model: 'aiModel', //pro
      config: vGenerationConfig,
    });

    let initialPrompt = "...";
    if (unspscData.key) {
      const unspscBlock = `
UNSPSC Code:
Key = ${unspscData.key}
Parent Key = ${unspscData.parentKey}
Code = ${unspscData.code}
`;
      initialPrompt += `\n\n${unspscBlock.trim()}`;
    }

    const prompts = {
      initial: initialPrompt,
      go_again: "...",
      relax_manufacturer: "...",
      relax_config: "...",
      fallback_pcr: "..."
    };

    let currentPrompt = "...";
    let stage = 1;
    let loopCounter = 0;

    while (currentNumProducts < maxProducts && loopCounter < 10) {
      loopCounter++;
      logger.info(`[cf20] Loop ${loopCounter}, Stage ${stage}. Found ${currentNumProducts}/${maxProducts}. Prompting AI...`);
      allFormattedTurns.push(`--- user ---\n${currentPrompt}`);

      const streamResult = await runWithRetry(() => chat.sendMessageStream({ message: currentPrompt }));

      let answerThisTurn = "";
      let thoughtsThisTurn = "";
      const rawChunksThisTurn = [];
      for await (const chunk of streamResult) {
        rawChunksThisTurn.push(chunk);
        harvestUrls(chunk, allCollectedUrls); // Harvest from every chunk

        if (chunk.candidates && chunk.candidates.length > 0) {
          for (const candidate of chunk.candidates) {
            // 1. Process content parts for main text and tool calls/thoughts
            if (candidate.content && candidate.content.parts && candidate.content.parts.length > 0) {
              for (const part of candidate.content.parts) {
                if (part.text) {
                  answerThisTurn += part.text;
                } else if (part.functionCall) {
                  thoughtsThisTurn += `\n--- TOOL CALL ---\n${JSON.stringify(part.functionCall, null, 2)}\n`;
                } else {
                  // Capture any other non-text parts as generic thoughts
                  const thoughtText = JSON.stringify(part, null, 2);
                  if (thoughtText !== '{}') {
                    thoughtsThisTurn += `\n--- AI THOUGHT ---\n${thoughtText}\n`;
                  }
                }
              }
            }

            // 2. Process grounding metadata specifically for search queries
            const gm = candidate.groundingMetadata;
            if (gm?.webSearchQueries?.length) {
              thoughtsThisTurn += `\n--- SEARCH QUERIES ---\n${gm.webSearchQueries.join("\n")}\n`;
              gm.webSearchQueries.forEach(q => allSearchQueries.add(q));
            }
          }
        } else if (chunk.text) {
          // Fallback for simple chunks that only contain text at the top level
          answerThisTurn += chunk.text;
        }
      }
      const finalAnswer = answerThisTurn.trim();

      allFormattedTurns.push(`--- model ---\n${thoughtsThisTurn.trim()}`);
      allRawChunks.push(...rawChunksThisTurn);

      if (finalAnswer.toLowerCase().trim() === 'finished: accuracy') {
        logger.info("[cf20] AI determined that adding more products would reduce accuracy. Ending product search loop.");
        break; // Exit the while loop and proceed with the products found so far
      }

      // --- NEW: Annotate this turn's answer ---
      const { annotatedAnswer, newSourcesList } = await annotateAndCollectSources(
        finalAnswer,
        rawChunksThisTurn,
        urlCitationMap,
        citationCounter
      );
      citationCounter = urlCitationMap.size + 1;
      allAnnotatedAnswers.push(annotatedAnswer);
      if (newSourcesList.length > 0) {
        allSources.push(...newSourcesList);
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
      totalInputTokens += currentInputTks || 0;

      const { totalTokens: currentOutputTks } = await ai.models.countTokens({
        model: 'aiModel',
        contents: [{ role: 'model', parts: [{ text: finalAnswer }] }]
      });
      totalOutputTokens += currentOutputTks || 0;

      // AMEND THIS BLOCK - No change to the code itself, but it will now work correctly
      // because thoughtsThisTurn is populated.
      const { totalTokens: currentToolCallTks } = await ai.models.countTokens({
        model: 'aiModel',
        contents: [{ role: 'model', parts: [{ text: thoughtsThisTurn }] }]
      });
      totalToolCallTokens += currentToolCallTks || 0;

      const parsedProducts = parseMPCFFullProducts(finalAnswer);
      // Create a reverse map for easy lookup: Number -> URL
      const reverseUrlCitationMap = new Map(
        [...urlCitationMap.entries()].map(([url, number]) => [number, url])
      );

      const citationRegex = /.*/;
      parsedProducts.new.forEach(product => {
        // Combine the relevant text fields to search for citations
        const combinedText = product.official_cf_sources || '';
        const foundUrls = new Set();
        let match;
        // Find all citation numbers like [1], [2], etc.
        while ((match = citationRegex.exec(combinedText)) !== null) {
          const citationNumber = parseInt(match[1], 10);
          if (reverseUrlCitationMap.has(citationNumber)) {
            // If the number exists in our map, add the URL
            foundUrls.add(reverseUrlCitationMap.get(citationNumber));
          }
        }
        // Attach the found URLs directly to the product object
        product.urls = Array.from(foundUrls);
      });
      const numFound = parsedProducts.existing.length + parsedProducts.new.length;

      if (numFound > 0) {
        currentNumProducts += numFound;

        if (parsedProducts.existing.length > 0) {
          const existingBatch = db.batch();
          const names = parsedProducts.existing.map(p => p.name);
          const productsSnap = await db.collection('products_new').where('name', 'in', names).get();
          productsSnap.docs.forEach(doc => {
            existingBatch.update(doc.ref, { eai_ef_docs: admin.firestore.FieldValue.arrayUnion(eDocRef) });
          });
          await existingBatch.commit();
          logger.info(`[cf20] Linked ${productsSnap.size} existing products.`);
        }

        parsedProducts.new.forEach(p => allNewProductEntries.push(p));
        currentPrompt = prompts.go_again;
        continue;
      }

      if (stage === 1) {
        if (currentNumProducts <= 5) {
          stage = 2;
          currentPrompt = prompts.relax_manufacturer;
        } else { break; }
      } else if (stage === 2) {
        if (currentNumProducts <= 10) {
          stage = 3;
          currentPrompt = prompts.relax_config;
        } else { break; }
      } else if (stage === 3) {
        stage = 4;
        currentPrompt = prompts.fallback_pcr;
      } else {
        break;
      }
    }

    const finalTokens = {
      input: totalInputTokens,
      output: totalOutputTokens,
      toolCalls: totalToolCallTokens,
    };
    const totalCost = calculateCost('aiModel', finalTokens);

    const finalFormattedConversation = allFormattedTurns.join('\n\n');

    // --- Construct final annotated answer and sources list ---
    const finalAggregatedAnswer = allAnnotatedAnswers.join('\n\n');
    const finalSourcesString = allSources.length > 0 ? '\n\nSources:\n' + allSources.join('\n') : '';
    const finalAnswerForLogging = finalAggregatedAnswer + finalSourcesString;

    // ADDED: Log the full conversation to the console
    logFullConversation({
      sys: MPCFFULL_PRODUCTS_SYS,
      user: prompts.initial,
      thoughts: finalFormattedConversation,
      answer: finalAnswerForLogging, // Pass the fully annotated answer
      generationConfig: vGenerationConfig
    });

    // AMENDED: Logging now points to the original product
    await logAITransaction({
      cfName: 'cf20',
      productId: originalProductId,
      cost: totalCost,
      totalTokens: finalTokens,
      searchQueries: Array.from(allSearchQueries),
      modelUsed: 'aiModel', //pro
    });
    await logAIReasoning({
      sys: MPCFFULL_PRODUCTS_SYS,
      user: prompts.initial,
      thoughts: finalFormattedConversation,
      answer: finalAnswerForLogging, // Pass the fully annotated answer
      cloudfunction: 'cf20',
      productId: originalProductId,
      rawConversation: allRawChunks, // Pass the collected raw chunks
    });
    if (allCollectedUrls.size > 0) {
      await saveURLs({
        urls: Array.from(allCollectedUrls),
        productId: originalProductId,
        pMPCFData: true,
        sys: MPCFFULL_PRODUCTS_SYS,
        user: prompts.initial,
        thoughts: finalFormattedConversation,
        answer: finalAnswerForLogging,
        cloudfunction: 'cf20',
      });
    }

    // --- Final Processing of all found 'New Products' ---
    if (allNewProductEntries.length > 0) {
      // Fetch the original product's data to get the includePackaging flag
      const originalProductSnap = await pDocRef.get();
      const originalProductData = originalProductSnap.data() || {};
      const includePackagingValue = originalProductData.includePackaging === true; // Safely get boolean, defaults to false

      const batch = db.batch();
      allNewProductEntries.forEach(p => {
        const pRef = db.collection("products_new").doc();
        const payload = {
          name: p.name,
          official_cf_available: p.official_cf,
          ...(p.official_cf_sources && { official_cf_sources: p.official_cf_sources }),
          ef_name: productName,
          ef_pn: true,
          eai_ef_inputs: [productName],
          eai_ef_docs: [eDocRef],
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          includePackaging: includePackagingValue, // Add the inherited flag here
        };
        if (p.supplier_cf !== null) {
          payload.supplier_cf = p.supplier_cf;
        }
        batch.set(pRef, payload);
        p.id = pRef.id;
        if (p.urls && p.urls.length > 0) {
          saveURLs({
            urls: p.urls,
            productId: pRef.id, // Use the ID of the new product document
            pSDCFData: true,// This sets the type to 'sdCF'
          }).catch(err => {
            logger.error(`[cf20] Failed to save URLs for new product ${pRef.id}:`, err);
          });
        }

        allDocumentsForDatastore.push({
          id: pRef.id,
          structData: { name: payload.name, ef_pn: payload.ef_pn },
        });
      });
      await batch.commit();
      logger.info(`[cf20] Created ${allNewProductEntries.length} new product documents.`);

      const newProductIds = allNewProductEntries.map(p => p.id);

      // --- NEW: Run SDCF Review First and Wait ---
      logger.info(`[cf20] Triggering cf37 for ${newProductIds.length} new products...`);
      const sdcfReviewFactories = newProductIds.map(id => {
        return () => callCF("cf37", { productId: id });
      });
      await runPromisesInParallelWithRetry(sdcfReviewFactories);
      logger.info(`[cf20] Finished all cf37 calls.`);
      // --- END: New SDCF Review Step ---

      if (shouldCalculateOtherMetrics) {
        logger.info(`[cf20] Triggering cf31 for ${newProductIds.length} new products...`);
        const otherMetricsFactories = newProductIds.map(id => {
          return () => callCF("cf31", {
            productId: id,
          });
        });
        await runPromisesInParallelWithRetry(otherMetricsFactories);
        logger.info(`[cf20] Finished all cf31 calls.`);
      } else {
        logger.info(`[cf20] Skipping other metrics calculation as the flag was not set.`);
      }

      // Create an array of functions that will generate the promises
      const promiseFactories = newProductIds.map(id => {
        return () => { // This is the "factory" function
          const initialPayload = { productId: id };
          if (shouldCalculateOtherMetrics) {
            initialPayload.otherMetrics = true;
          }
          return callCF("cf11", initialPayload);
        };
      });

      logger.info(`[cf20] Triggering cf11 for ${newProductIds.length} products with concurrent retries...`);

      // Execute all promise factories with the retry logic
      await runPromisesInParallelWithRetry(promiseFactories);

      logger.info(`[cf20] Finished triggering all cf11 calls.`);

      // Polling Logic
      const MAX_POLL_MINUTES = 55;
      const POLLING_INTERVAL_MS = 30000;
      const startTime = Date.now();
      logger.info(`[cf20] Polling for completion of ${newProductIds.length} products...`);
      while (Date.now() - startTime < MAX_POLL_MINUTES * 60 * 1000) {
        const chunks = [];
        for (let i = 0; i < newProductIds.length; i += 30) {
          chunks.push(newProductIds.slice(i, i + 30));
        }
        const chunkPromises = chunks.map(chunk => db.collection("products_new").where(admin.firestore.FieldPath.documentId(), 'in', chunk).get());
        const allSnapshots = await Promise.all(chunkPromises);
        const allDocs = allSnapshots.flatMap(snapshot => snapshot.docs);
        const completedCount = allDocs.filter(doc => doc.data().apcfInitial_done === true).length;
        logger.info(`[cf20] Polling: ${completedCount}/${newProductIds.length} done.`);
        if (completedCount === newProductIds.length) {
          logger.info("[cf20] All new products completed calculations.");
          break;
        }
        await sleep(POLLING_INTERVAL_MS);
      }
      if (Date.now() - startTime >= MAX_POLL_MINUTES * 60 * 1000) {
        logger.warn(`[cf20] Polling timed out.`);
      }

      // Datastore Import
      if (allDocumentsForDatastore.length > 0) {
        logger.info(`[cf20] Adding ${allDocumentsForDatastore.length} docs to Vertex AI Search.`);
        const datastorePath = 'projects/projectId/locations/global/collections/default_collection/dataStores/ecoze-ai-products-datastore_1755024362755';
        try {
          const [operation] = await discoveryEngineClient.importDocuments({
            parent: `${datastorePath}/branches/0`,
            inlineSource: { documents: allDocumentsForDatastore },
          });
          await operation.promise();
          logger.info(`[cf20] Datastore import completed.`);
        } catch (err) {
          logger.error("[cf20] Failed to import documents:", err);
        }
      }
    }

    res.json("Done");
  } catch (err) {
    logger.error("[cf20] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});