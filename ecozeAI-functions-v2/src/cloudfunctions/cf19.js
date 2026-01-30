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

exports.cf19 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf19] Invoked");
  try {
    // 1. Argument Parsing and Doc Fetching
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || null;
    if (!productId) {
      res.status(400).json({ error: "productId is required" });
      return;
    }
    // ADDED: otherMetrics parsing
    const otherMetrics = (req.method === "POST" ? req.body?.otherMetrics : req.query.otherMetrics) || false;

    const pRef = db.collection("products_new").doc(productId);
    const pSnap = await pRef.get();
    if (!pSnap.exists) {
      res.status(404).json({ error: `Product ${productId} not found` });
      return;
    }
    const pData = pSnap.data() || {};
    const productName = pData.name;

    if (!productName) {
      logger.error(`[cf19] Product ${productId} is missing a 'name' field.`);
      res.status(400).json({ error: "Product document is missing a name." });
      return;
    }

    await pRef.update({
      apcfMPCFFullNew_started: true,
      rcOn: true
    });

    // 2. Perform AI call to generate tags
    const vGenerationConfigTags = {
//
//
//
//
        retrieval: {
          vertexAiSearch: {
            datastore: 'projects/projectId/locations/global/collections/default_collection/dataStores/ecoze-ai-products-datastore_1755024362755',
          },
        },
      }],
//
    };

    // AMENDED: Add UNSPSC code to the prompt if it exists
    let userPromptForTags = "...";
    const { unspsc_key, unspsc_parent_key, unspsc_code } = pData;
    if (unspsc_key && unspsc_parent_key && unspsc_code) {
      const unspscBlock = `...`;
      userPromptForTags = `${productName}\n\n${unspscBlock.trim()}`;
    }

    const { answer: tagsResponse, ...tagsAiResults } = await runGeminiStream({
      model: 'aiModel', //flash
      generationConfig: vGenerationConfigTags,
      user: userPromptForTags,
    });

    // 3. Create the /eaief_inputs/ document (eDoc)
    const quantityMatch = tagsResponse.match( /.*/);
    const conversionMatch = tagsResponse.match( /.*/);
    const unitMatch = tagsResponse.match( /.*/);

    const quantity = quantityMatch ? parseFloat(quantityMatch[1]) : null;
    const conversion = conversionMatch ? parseFloat(conversionMatch[1]) : 1;
    const unit = unitMatch ? unitMatch[1].trim() : null;
    const conversionOn = !!conversionMatch;

    const productTags = tagsResponse.split('\n')
      .map(line => line.match( /.*/))
      .filter(Boolean)
      .map(match => match[1].trim());

    const eDocRef = db.collection("eaief_inputs").doc();
    const eaiiPayload = {
      productName_input: productName,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      tags: productTags,
      product: pRef,
      otherMetrics: otherMetrics,
      conversionOn: conversionOn,
    };

    if (quantity !== null) eaiiPayload.quantity = quantity;
    if (conversion !== 1) eaiiPayload.conversion = conversion;
    if (unit) eaiiPayload.unit = unit;

    // ADDED: Add UNSPSC fields to the payload
    if (unspsc_key) eaiiPayload.unspsc_key = unspsc_key;
    if (unspsc_parent_key) eaiiPayload.unspsc_parent_key = unspsc_parent_key;
    if (unspsc_code) eaiiPayload.unspsc_code = unspsc_code;

    await eDocRef.set(eaiiPayload);
    logger.info(`[cf19] Created new eaief_inputs document: ${eDocRef.id}`);

    // Log AI call after eDoc is created to get its ID
    await logAITransaction({
      cfName: 'cf19-TagGeneration',
      productId: productId,
      cost: tagsAiResults.cost,
      totalTokens: tagsAiResults.totalTokens,
      searchQueries: tagsAiResults.searchQueries,
      modelUsed: tagsAiResults.model
    });

    await logAIReasoning({
      sys: MPCFFULLNEW_TAG_GENERATION_SYS,
      user: userPromptForTags,
      thoughts: tagsAiResults.thoughts,
      answer: tagsResponse,
      cloudfunction: 'cf19-TagGeneration',
      productId: productId,
      rawConversation: tagsAiResults.rawConversation,
    });

    // 4. Perform cf20
    logger.info(`[cf19] Calling cf20 with eId: ${eDocRef.id}`);
    await callCF("cf20", { eId: eDocRef.id });
    logger.info("[cf19] cf20 finished.");

    // 5. Find all matching products
    let pmDocsSnap = await db.collection('products_new')
      .where('eai_ef_docs', 'array-contains', eDocRef)
      .get();

    logger.info(`[cf19] Found an initial ${pmDocsSnap.size} products linked to eaief_inputs/${eDocRef.id}`);

    // --- START: New Deletion Logic ---
    if (!pmDocsSnap.empty) {
      logger.info(`[cf19] Filtering ${pmDocsSnap.size} products for data quality...`);

      // Check each product in parallel for deletion criteria
      const checks = pmDocsSnap.docs.map(async doc => {
        // Safeguard: Never delete the original product that triggered the function.
        if (doc.id === productId) {
          return null;
        }

        const data = doc.data();
        const hasStandards = Array.isArray(data.sdcf_standards) && data.sdcf_standards.length > 0;

        const sdcfDataSnap = await doc.ref.collection('pn_data')
          .where('type', '==', 'sdCF') // This checks for data from cf37
          .limit(1)
          .get();

        const hasSdcfData = !sdcfDataSnap.empty;

        // If it has NEITHER standards NOR sdCF data docs, it's a candidate for deletion
        if (!hasStandards || !hasSdcfData) {
          logger.info(`[cf19] Marking product ${doc.id} for deletion due to missing SDCF data.`);
          return doc.ref;
        }
        return null; // Keep this document
      });

      const results = await Promise.all(checks);
      const docsToDeleteRefs = results.filter(ref => ref !== null);

      // If there are documents to delete, perform the deletion in a batch
      if (docsToDeleteRefs.length > 0) {
        const batch = db.batch();
        docsToDeleteRefs.forEach(ref => batch.delete(ref));
        await batch.commit();
        logger.info(`[cf19] Deleted ${docsToDeleteRefs.length} products with insufficient data.`);

        // RE-FETCH the product list after deletion
        logger.info(`[cf19] Re-fetching product list after deletion.`);
        pmDocsSnap = await db.collection('products_new')
          .where('eai_ef_docs', 'array-contains', eDocRef)
          .get();
      }
    }
    // --- END: New Deletion Logic ---

    logger.info(`[cf19] Found ${pmDocsSnap.size} products linked to eaief_inputs/${eDocRef.id} after filtering.`);

    // 6. Calculate averages and update docs
    let averageCF;
    let finalCf;

    if (pmDocsSnap.empty) {
      logger.warn(`[cf19] No matching products found for eaief_inputs/${eDocRef.id}. All averages will be 0.`);
      averageCF = 0;
      finalCf = 0; // If average is 0, final is 0

      const updatePayload = {
        cf_average: finalCf,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };
      if (otherMetrics) {
        updatePayload.ap_total_average = 0;
        updatePayload.ep_total_average = 0;
        updatePayload.adpe_total_average = 0;
        updatePayload.gwp_f_total_average = 0;
        updatePayload.gwp_b_total_average = 0;
        updatePayload.gwp_l_total_average = 0;
      }
      await eDocRef.update(updatePayload);

    } else {
      // --- START: New Averaging Logic ---
      const metrics = {
        cf: [], ap: [], ep: [], adpe: [],
        gwp_f_percentages: [], gwp_b_percentages: [], gwp_l_percentages: []
      };

      pmDocsSnap.docs.forEach(doc => {
        const data = doc.data();
        // CF average calculation remains the same
        if (typeof data.supplier_cf === 'number' && isFinite(data.supplier_cf)) {
          metrics.cf.push(data.supplier_cf);
        }

        if (otherMetrics) {
          // Handle non-GWP metrics: ignore unset, include 0
          if (typeof data.ap_total === 'number' && isFinite(data.ap_total)) metrics.ap.push(data.ap_total);
          if (typeof data.ep_total === 'number' && isFinite(data.ep_total)) metrics.ep.push(data.ep_total);
          if (typeof data.adpe_total === 'number' && isFinite(data.adpe_total)) metrics.adpe.push(data.adpe_total);

          // New GWP Percentage Calculation Logic
          const supplierCf = data.supplier_cf;
          if (typeof supplierCf === 'number' && isFinite(supplierCf) && supplierCf > 0) {
            if (typeof data.gwp_f_total === 'number' && isFinite(data.gwp_f_total)) {
              metrics.gwp_f_percentages.push(data.gwp_f_total / supplierCf);
            }
            if (typeof data.gwp_b_total === 'number' && isFinite(data.gwp_b_total)) {
              metrics.gwp_b_percentages.push(data.gwp_b_total / supplierCf);
            }
            if (typeof data.gwp_l_total === 'number' && isFinite(data.gwp_l_total)) {
              metrics.gwp_l_percentages.push(data.gwp_l_total / supplierCf);
            }
          }
        }
      });

      averageCF = calculateAverage(metrics.cf, true); // Keep filtering zeros for the main CF average
      finalCf = averageCF * conversion;

      const eDocUpdatePayload = {
        cf_average: finalCf,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };

      if (otherMetrics) {
        // Calculate AP, EP, ADPE averages (don't filter zeros here)
        eDocUpdatePayload.ap_total_average = calculateAverage(metrics.ap, false) * conversion;
        eDocUpdatePayload.ep_total_average = calculateAverage(metrics.ep, false) * conversion;
        eDocUpdatePayload.adpe_total_average = calculateAverage(metrics.adpe, false) * conversion;

        // Calculate average percentages for GWP values
        const avg_gwp_f_percent = calculateAverage(metrics.gwp_f_percentages, false);
        const avg_gwp_b_percent = calculateAverage(metrics.gwp_b_percentages, false);
        const avg_gwp_l_percent = calculateAverage(metrics.gwp_l_percentages, false);

        // Calculate final GWP averages based on percentages
        eDocUpdatePayload.gwp_f_total_average = avg_gwp_f_percent * finalCf;
        eDocUpdatePayload.gwp_b_total_average = avg_gwp_b_percent * finalCf;
        eDocUpdatePayload.gwp_l_total_average = avg_gwp_l_percent * finalCf;
      }

      await eDocRef.update(eDocUpdatePayload);
      logger.info(`[cf19] Updated ${eDocRef.id} with new calculated averages.`);
      // --- END: New Averaging Logic ---
    }

    const currentCfFull = pData.cf_full || 0;

    const pDocUpdatePayload = {
      cf_full_original: currentCfFull,
      cf_full: finalCf,
      cf_full_refined: finalCf,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    await pRef.update(pDocUpdatePayload);
    logger.info(`[cf19] Updated product ${productId}: set cf_full_original to ${currentCfFull} and cf_full to ${finalCf} (averageCF: ${averageCF} * conversion: ${conversion}).`);

    // --- Start: Summarize the cf20 Reasoning ---
    logger.info(`[cf19] Starting summarization for cf20 reasoning.`);
    try {
      // 1. Find the reasoning document
      const reasoningQuery = pRef.collection("pn_reasoning")
        .where("cloudfunction", "==", "cf20")
        .limit(1);

      const reasoningSnap = await reasoningQuery.get();

      if (!reasoningSnap.empty) {
        const prDoc = reasoningSnap.docs[0];
        const reasoningData = prDoc.data();
        const originalReasoning = reasoningData.reasoningOriginal || "";

        // 2. Construct the user prompt for the summarizer AI
        const summarizerUserPrompt = "...";

        // 3. Perform the AI call
        const summarizerConfig = {
//
//
//
//
//
            includeThoughts: true,
            thinkingBudget: 24576
          },
        };

        const {
          answer: summarizerResponse,
          cost,
          totalTokens,
          modelUsed
        } = await runGeminiStream({
          model: 'openai/gpt-oss-120b-maas', //gpt-oss-120b
          generationConfig: summarizerConfig,
          user: summarizerUserPrompt,
        });

        // 4. Log the cost of this summarization call
        await logAITransaction({
          cfName: `cf20-summarizer`,
          productId: productId,
          cost,
          totalTokens,
          modelUsed,
        });

        // 5. Process the response and update the reasoning document
        const marker = "New Text:";
        const sanitizedResponse = summarizerResponse.replace( /.*/, ' ');
        const lastIndex = sanitizedResponse.toLowerCase().lastIndexOf(marker.toLowerCase());

        if (lastIndex !== -1) {
          const reasoningAmended = sanitizedResponse.substring(lastIndex + marker.length).replace( /.*/, '').trim();
          if (reasoningAmended) {
            await prDoc.ref.update({ reasoningAmended: reasoningAmended });
            logger.info(`[cf19] Successfully saved amended reasoning to document ${prDoc.id}.`);
          }
        } else {
          logger.warn(`[cf19] Summarizer AI failed to return the 'New Text:' header.`);
        }
      } else {
        logger.warn("[cf19] Could not find a reasoning document for 'cf20' to summarize.");
      }
    } catch (err) {
      logger.error("[cf19] The summarization step failed.", { error: err.message });
      // Do not block the main function from completing if summarization fails
    }

    // --- Start: Aggregate costs from newly created EF products ---
    logger.info(`[cf19] Aggregating costs from child EF products linked to ${eDocRef.id}.`);
    const pcDocsSnap = await db.collection('products_new')
      .where('eai_ef_docs', '==', [eDocRef]) // Find docs where the array contains ONLY eDocRef
      .get();

    if (!pcDocsSnap.empty) {
      let tcSum = 0;
      pcDocsSnap.forEach(doc => {
        // Safely add the totalCost, defaulting to 0 if it's missing
        tcSum += doc.data().totalCost || 0;
      });

      if (tcSum > 0) {
        await pRef.update({
          totalCost: admin.firestore.FieldValue.increment(tcSum)
        });
        logger.info(`[cf19] Incremented original product ${productId}'s totalCost by ${tcSum}.`);
      }
    } else {
      logger.warn(`[cf19] Found no child EF products to aggregate costs from.`);
    }
    // --- End: Cost Aggregation ---

    await pRef.update({
      apcfMPCFFullNew_done: true,
      status: "Done"
    });

    // 7. End the cloudfunction
    res.send("Done");

  } catch (err) {
    logger.error("[cf19] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});