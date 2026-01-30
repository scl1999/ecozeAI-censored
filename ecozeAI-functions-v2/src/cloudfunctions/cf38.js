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

exports.cf38 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf38] Invoked");
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

    // 3. AI Call (Initial - Original Logic)
    const collectedUrls = new Set();
    const vGenerationConfig = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 24576,
      },
    };

    let { answer: finalAssistantText, thoughts, cost, totalTokens, searchQueries, model, rawConversation } = await runGeminiStreamBrowserUse({
      model: 'aiModel', //flash
      generationConfig: vGenerationConfig,
      user: userPrompt,
      productId,
      collectedUrls
    });

    // Helper to check if result is unknown
    const isUnknown = (text) => {
      const match = text.match( /.*/);
      return !match || /unknown/i.test(match[1]);
    };

    // 4. Fallback Logic (3-Tier Workflow)


    // 5. Logging
    await logAITransaction({
      cfName: 'cf38',
      productId: productId,
      cost: cost,
      totalTokens: totalTokens, // Note: This might need better aggregation
      searchQueries: searchQueries,
      modelUsed: model,
    });

    await logAIReasoning({
      sys: SYS_MSG,
      user: userPrompt,
      thoughts: thoughts,
      answer: finalAssistantText,
      cloudfunction: 'cf38',
      productId: productId,
      rawConversation: rawConversation,
    });

    if (collectedUrls.size) {
      await saveURLs({
        urls: Array.from(collectedUrls),
        productId,
        cloudfunction: 'cf38'
      });
    }

    // 6. Update Firestore
    const parseAIResponse = (text) => {
      const productCfMatch = text.match( /.*/);
      const supplierCfUncertaintyMatch = text.match( /.*/);
      const originalProductCfMatch = text.match( /.*/);
      const originalCfLifecycleStagesMatch = text.match( /.*/);
      const standardsMatch = text.match( /.*/);
      const extraInformationMatch = text.match( /.*/);
      const includePackagingMatch = text.match( /.*/);

      const product_cf_raw = productCfMatch ? productCfMatch[1].trim() : null;
      const original_product_cf_raw = originalProductCfMatch ? originalProductCfMatch[1].trim() : null;
      const standardsRaw = standardsMatch ? standardsMatch[1].trim() : null;
      const extraInfo = extraInformationMatch ? extraInformationMatch[1].trim() : null;
      const originalLifecycleStages = originalCfLifecycleStagesMatch ? originalCfLifecycleStagesMatch[1].trim() : null;

      // Helper to parse number from string (e.g. "12.5 kgCO2e" -> 12.5)
      const parseNumber = (str) => {
        if (!str) return null;
        const m = str.match( /.*/);
        if (!m) return null;
        const val = parseFloat(m[1].replace( /.*/, ''));
        return Number.isFinite(val) ? val : null;
      };

      const parsedProductCF = parseNumber(product_cf_raw);
      const parsedOriginalCF = parseNumber(original_product_cf_raw);

      let standardsList = [];
      let isIsoAligned = false;
      if (standardsRaw && standardsRaw.toLowerCase() !== 'unknown' && standardsRaw.length > 0) {
        standardsList = standardsRaw.split(',').map(s => s.trim()).filter(s => s);
        isIsoAligned = standardsList.some(s => s.toUpperCase().startsWith('ISO'));
      }

      return {
        parsedProductCF,
        parsedOriginalCF,
        originalLifecycleStages,
        extraInfo,
        standardsList,
        isIsoAligned
      };
    };

    let parsedData = parseAIResponse(finalAssistantText);
    const originalParsedData = { ...parsedData }; // Save original in case Playwright fails

    // --- FALLBACK LOGIC (3-Tier Workflow) ---
    if (!Number.isFinite(parsedData.parsedProductCF)) {
      logger.info("[cf38] Initial check failed to find supplier_cf. Initiating 3-Tier Fallback...");

      const ADD_SYS_MSG = "...";

      const fallbackResult = await runGeminiStreamBrowserUse({
        model: 'aiModel',
        generationConfig: {
          ...vGenerationConfig,
//
//
//
        },
        user: userPrompt,
        productId,
        existingHistory: [],
        sysMsgAdd: ADD_SYS_MSG
      });

      // Use fallback result
      finalAssistantText = fallbackResult.answer;
      thoughts += "\n--- FALLBACK THOUGHTS ---\n" + fallbackResult.thoughts;
      cost += fallbackResult.cost;
      model = `Fallback: ${fallbackResult.model}`;

      // Re-parse response
      parsedData = parseAIResponse(finalAssistantText);
    }

    // --- TIKA VERIFICATION STEP ---
    if (Number.isFinite(parsedData.parsedProductCF)) {
      logger.info("[cf38] Valid result found. Initiating Tika Verification...");

      // 1. Extract text from all collected URLs
      let tikaText = "";
      const urlsToVerify = Array.from(collectedUrls);
      for (const url of urlsToVerify) {
        const extracted = await extractWithTika(url);
        if (extracted) {
          tikaText += `\n\n--- SOURCE: ${url} ---\n${extracted}`;
        }
      }

      if (tikaText.trim()) {
        // Limit text length to avoid context window issues (approx 100k chars)
        if (tikaText.length > 100000) tikaText = tikaText.substring(0, 100000) + "... [TRUNCATED]";

        // 2. Prepare Verification Prompt
        const VERIFY_SYS_MSG = "...";

        const verifyUserPrompt = "...";

        // 3. Call gpt-oss-120b
        try {
          const verifyResult = await runOpenModelStream({
            model: 'openai/gpt-oss-120b-maas',
            generationConfig: {
//
//
//
            },
            user: verifyUserPrompt
          });

          // 4. Update Result
          logger.info("[cf38] Tika Verification Complete. Updating result.");
          finalAssistantText = verifyResult.answer;
          parsedData = parseAIResponse(finalAssistantText); // Overwrite with verified data

          // Log verification
          cost += verifyResult.cost;
          model = `${model} + TikaVerify(gpt-oss-120b)`;
          thoughts += "\n--- TIKA VERIFICATION THOUGHTS ---\n" + verifyResult.thoughts;
        } catch (err) {
          logger.error("[cf38] Tika Verification Failed:", err);
          // Continue with original result if verification fails
        }
      } else {
        logger.info("[cf38] No text extracted from URLs. Skipping verification.");
      }
    }

    // 6. Firestore Update (using "2" suffix)
    const updatePayload = {};

    if (Number.isFinite(parsedData.parsedProductCF)) {
      updatePayload.supplier_cf2 = parsedData.parsedProductCF;
    }

    if (Number.isFinite(parsedData.parsedOriginalCF)) {
      updatePayload.oscf2 = parsedData.parsedOriginalCF;
    }

    if (parsedData.originalLifecycleStages && parsedData.originalLifecycleStages.toLowerCase() !== 'unknown') {
      updatePayload.socf_lifecycle_stages2 = parsedData.originalLifecycleStages;
    }

    if (parsedData.extraInfo && parsedData.extraInfo.toLowerCase() !== 'unknown') {
      updatePayload.extra_information2 = parsedData.extraInfo;
    }

    updatePayload.sdcf_standards2 = parsedData.standardsList;
    updatePayload.sdcf_iso_aligned2 = parsedData.isIsoAligned;

    // Only update if there is something to change
    if (Object.keys(updatePayload).length > 0) {
      await pRef.update(updatePayload);
      logger.info(`[cf38] Updated product ${productId} with:`, updatePayload);
    } else {
      logger.info(`[cf38] No valid data found in AI response to update for product ${productId}.`);
    }

    // 7. Finalization
    res.json("Done");

  } catch (err) {
    logger.error("[cf38] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});