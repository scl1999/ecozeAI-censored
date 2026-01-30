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

exports.cf42 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  console.log("[cf42] Invoked");

  try {
    // 1. Parse arguments: exactly one of materialId or productId
    const materialId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || null;
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || null;

    if ((!materialId && !productId) || (materialId && productId)) {
      res.status(400).json({ error: "Provide exactly one of materialId or productId" });
      return;
    }

    let docName = "";
    let supplierName = "";   // Always pass something (empty string if not used)
    let targetRef;            // Firestore ref to update
    let isMaterial = false;

    if (materialId) {
      console.log(`[cf42] materialId = ${materialId}`);
      const mRef = db.collection("materials").doc(materialId);
      const mSnap = await mRef.get();
      if (!mSnap.exists) {
        res.status(404).json({ error: `Material ${materialId} not found` });
        return;
      }
      const mData = mSnap.data() || {};
      docName = (mData.name || "").trim();
      supplierName = (mData.supplier_name || "").trim() || "";
      targetRef = mRef;
      isMaterial = true;
      console.log(`[cf42] fetched material name = "${docName}", supplier_name = "${supplierName}"`);
    } else {
      console.log(`[cf42] productId = ${productId}`);
      const pRef = db.collection("products_new").doc(productId);
      const pSnap = await pRef.get();
      if (!pSnap.exists) {
        res.status(404).json({ error: `Product ${productId} not found` });
        return;
      }
      const pData = pSnap.data() || {};
      docName = (pData.name || "").trim();
      supplierName = "";
      targetRef = pRef;
      console.log(`[cf42] fetched product name = "${docName}"`);
    }

    // 2. Build system prompt + initial history
    const SYS_MSG = "...";

    /* ─────────────────────────────────────────────────────────────
    * 4. Gemini 2.5-pro reasoning with Google Search grounding
    * ────────────────────────────────────────────────────────────*/

    let userPrompt;

    if (materialId) {
      console.log(`[cf42] materialId = ${materialId}`);
      const mRef = db.collection("materials").doc(materialId);
      const mSnap = await mRef.get();
      if (!mSnap.exists) {
        res.status(404).json({ error: `Material ${materialId} not found` });
        return;
      }
      const mData = mSnap.data() || {};
      docName = (mData.name || "").trim();
      supplierName = (mData.supplier_name || "").trim() || "";
      targetRef = mRef;
      isMaterial = true;

      const name = (mData.name || "Unknown").trim();
      const supplier = (mData.supplier_name || "Unknown").trim();
      const mass = mData.mass;
      const massUnit = (mData.mass_unit || "").trim();
      const description = (mData.description || "No description provided.").trim();
      const massString = (typeof mass === 'number' && isFinite(mass) && massUnit) ? `${mass} ${massUnit}` : "Unknown";

      // Use 'let' to allow for appending the peer materials section
      userPrompt = `Product Name: ${name}\nSupplier: ${supplier}\nMass: ${massString}\nDescription: ${description}`;

      // --- START: New conditional logic to find and add peer materials ---
      let peerMaterialsSnap;
      if (mData.parent_material) {
        peerMaterialsSnap = await db.collection("materials")
          .where("parent_material", "==", mData.parent_material)
          .get();
      } else if (mData.linked_product) {
        peerMaterialsSnap = await db.collection("materials")
          .where("tier", "==", 1)
          .where("linked_product", "==", mData.linked_product)
          .get();
      }

      if (peerMaterialsSnap && !peerMaterialsSnap.empty) {
        const peerLines = [];
        let i = 1;
        for (const peerDoc of peerMaterialsSnap.docs) {
          if (peerDoc.id === materialId) continue;
          const peerData = peerDoc.data() || {};
          peerLines.push(
            `material_${i}_name: ${peerData.name || 'Unknown'}`,
            `material_${i}_supplier_name: ${peerData.supplier_name || 'Unknown'}`,
            `material_${i}_description: ${peerData.description || 'No description provided.'}`
          );
          i++;
        }
        if (peerLines.length > 0) {
          userPrompt += "\n\nPeer Materials:\n" + peerLines.join('\n');
        }
        if (mData.official_cf_sources) {
          userPrompt += `\n\nWhere you might find the official disclosed CF:\n${mData.official_cf_sources}`;
        }
      }
      // --- END: New logic ---

      console.log(`[cf42] fetched material name = "${docName}", supplier_name = "${supplierName}"`);
    } else {
      console.log(`[cf42] productId = ${productId}`);
      const pRef = db.collection("products_new").doc(productId);
      const pSnap = await pRef.get();
      if (!pSnap.exists) {
        res.status(404).json({ error: `Product ${productId} not found` });
        return;
      }
      const pData = pSnap.data() || {};
      docName = (pData.name || "").trim();
      supplierName = "";
      targetRef = pRef;

      // Construct the prompt here for products
      userPrompt = `Product Name: ${docName}`;

      if (pData.official_cf_sources) {
        userPrompt += `\n\nWhere you might find the official disclosed CF:\n${pData.official_cf_sources}`;
      }

      console.log(`[cf42] fetched product name = "${docName}"`);
    }

    // Hoist linkedProductId earlier for logging usage
    let linkedProductId = null;
    if (materialId) {
      // We need to re-fetch if we didn't save mData earlier globally?
      // docName/supplierName logic fetched it but didn't save the full object to a durable var used here.
      // But we are inside the function so we can just grab it again or use mData from new query.
      // Actually, the original code fetched mData AFTER the AI call at line 10841.
      // Let's stick to the pattern but ensure we have it for logging in the loop if needed.
      const mSnapForId = await db.collection("materials").doc(materialId).get();
      const mDataForId = mSnapForId.exists ? mSnapForId.data() : {};
      linkedProductId = mDataForId.linked_product ? mDataForId.linked_product.id : null;
    }

    // --- Helper: Response Parser ---
    // Moved up so it can be used in the loop
    const parseResponse = (text) => {
      let cf = text.match( /.*/);
      let uncert = text.match( /.*/);
      let pack = text.match( /.*/);
      let stds = text.match( /.*/);
      let extra = text.match( /.*/);

      let rawCF = cf ? cf[1].trim() : null;
      let parsedCF = rawCF && ! /.*/.test(rawCF) ? parseFloat(rawCF) : null;

      let stdsList = [];
      let isIso = false;
      let stdsRaw = stds ? stds[1].trim() : null;
      if (stdsRaw && stdsRaw.toLowerCase() !== 'unknown' && stdsRaw.length > 0) {
        stdsList = stdsRaw.split(',').map(s => s.trim()).filter(s => s);
        isIso = stdsList.some(s => s.toUpperCase().startsWith('ISO'));
      }

      return {
        productCF: parsedCF,
        uncertainty: uncert ? uncert[1].trim() : null,
        includePackaging: pack ? pack[1].trim().toUpperCase() === 'TRUE' : null,
        standardsList: stdsList,
        isIsoAligned: isIso,
        extraInformation: extra ? extra[1].trim() : null,
        isEmpty: !cf && !uncert && !pack && !stds && !extra,
        rawText: text
      };
    };

    // --- Helper: Run Main AI ---
    const runMainAI = async (prompt, history = [], collectedUrlsSet = new Set(), retries = 0) => {
      const vGenerationConfig = {
//
//
//
//
//
      };

      let effectiveUserPrompt = "...";
      if (history.length > 0) {
        effectiveUserPrompt = history.map(h => `User: ${h.user}\nAI: ${h.ai}`).join('\n\n') + `\n\nUser: ${prompt}`;
      }

      return await runGeminiStream({
        model: 'aiModel',//flash3
        generationConfig: vGenerationConfig,
        user: effectiveUserPrompt,
        collectedUrls: collectedUrlsSet
      });
    };

    // --- Helper: Run Fact Checker ---
    const runFactChecker = async (originalPrompt, originalSys, originalOutput, originalUrls) => {
      const fcModel = 'aiModel';
      const fcSys = `...`;

      const fcPrompt = "...";
      const fcConfig = {
//
//
//
//
//
      };

      const fcUrls = new Set();
      const result = await runGeminiStream({
        model: fcModel,
        generationConfig: fcConfig,
        user: fcPrompt,
        collectedUrls: fcUrls
      });

      // Log FC Transaction
      await logAITransaction({
        cfName: 'apcfSupplierDisclosed-FC',
        productId: productId || linkedProductId,
        materialId: materialId,
        cost: result.cost,
        totalTokens: result.totalTokens,
        searchQueries: result.searchQueries,
        modelUsed: fcModel
      });
      await logAIReasoning({
        sys: fcSys,
        user: fcPrompt,
        thoughts: result.thoughts,
        answer: result.answer,
        cloudfunction: 'apcfSupplierDisclosed-FC',
        productId: productId || linkedProductId,
        materialId: materialId,
        rawConversation: result.rawConversation
      });

      return result;
    };


    /* ---------------- EXECUTION FLOW ---------------- */

    let currentPrompt = "...";
    let currentHistory = [];
    let currentCollectedUrls = new Set();
    let finalAssistantText = "";
    let finalThoughts = "";
    let finalCost = 0;
    let finalTokens = { input: 0, output: 0 };
    let finalSearchQueries = [];
    let finalModel = 'aiModel';
    let finalRawConversation = [];

    let isComplete = false;
    let attemptCount = 0;
    const MAX_RETRIES_NO_URL = 2;

    // Retry Loop
    while (!isComplete && attemptCount <= MAX_RETRIES_NO_URL) {
      const result = await runMainAI(currentPrompt, currentHistory, currentCollectedUrls);

      finalCost += result.cost;
      finalAssistantText = result.answer;
      finalThoughts = result.thoughts;
      finalTokens = result.totalTokens;
      finalSearchQueries = result.searchQueries;
      finalRawConversation = result.rawConversation;
      finalModel = result.model;

      const parsed = parseResponse(finalAssistantText);
      const hasCF = parsed.productCF !== null;
      const hasUrls = result.searchQueries.length > 0 || currentCollectedUrls.size > 0;

      if (hasCF && !hasUrls) {
        // Found CF but NO URLs
        if (attemptCount < MAX_RETRIES_NO_URL) {
          console.log(`[cf42] Found CF but no URLs. Retrying (Attempt ${attemptCount + 1})...`);
          currentHistory.push({ user: currentPrompt, ai: finalAssistantText });
          currentPrompt = "Go again, you must use your url context and google search tool to find out the carbon footprint of this product from an official report. Do not rely on your knowledge.";
          attemptCount++;
        } else {
          console.log(`[cf42] Max retries reached with no URLs. Treating as Unknown.`);
          // Force Unknown replace if format matches - regex must match the output format exactly
          // Or just overwrite the text if we want to be safe, but replacement is safer for preserving other fields?
          // Given the prompt structure, updating the text is easiest.
          finalAssistantText = finalAssistantText.replace( /.*/, "*product_cf: Unknown");
          isComplete = true;
        }
      } else if (hasCF && hasUrls) {
        // Found CF AND URLs -> Fact Check
        console.log(`[cf42] Found CF and URLs. Running Fact Checker...`);

        let fcPass = false;
        let fcAttempts = 0;
        const MAX_FC_LOOPS = 1;

        while (!fcPass && fcAttempts <= MAX_FC_LOOPS) {
          const fcResult = await runFactChecker(currentPrompt, SYS_MSG, finalAssistantText, Array.from(currentCollectedUrls));

          const resMatch = fcResult.answer.match( /.*/);
          const resultStatus = resMatch ? resMatch[1].toLowerCase() : "fail";

          if (resultStatus === "pass") {
            console.log(`[cf42] Fact Checker PASSED.`);
            fcPass = true;
            isComplete = true;
          } else {
            // Failed
            const fbMatch = fcResult.answer.match( /.*/);
            const feedback = fbMatch ? fbMatch[1].trim() : "No feedback provided.";

            if (fcAttempts < MAX_FC_LOOPS) {
              console.log(`[cf42] Fact Checker FAILED. Retrying Original AI... Feedback: ${feedback}`);
              currentHistory.push({ user: currentPrompt, ai: finalAssistantText });
              currentPrompt = `Go again, we got another AI to fact check your answer and you failed. Here is the AIs feedback:\n${feedback}`;

              const retryResult = await runMainAI(currentPrompt, currentHistory, currentCollectedUrls);

              // Update "final" results
              finalAssistantText = retryResult.answer;
              finalThoughts = retryResult.thoughts;
              finalRawConversation = retryResult.rawConversation;
              finalCost += retryResult.cost;

              // Check validity
              const retryParsed = parseResponse(finalAssistantText);
              const retryHasCF = retryParsed.productCF !== null;

              if (!retryHasCF) {
                console.log(`[cf42] Retry failed to produce a CF. Ending.`);
                isComplete = true;
                break;
              }

              fcAttempts++;
            } else {
              // Double Fail
              console.log(`[cf42] Fact Checker FAILED twice. Force Unknown.`);
              finalAssistantText = finalAssistantText.replace( /.*/, "*product_cf: Unknown");
              isComplete = true;
              fcPass = true;
            }
          }
        }
        if (!isComplete) isComplete = true;

      } else {
        // No CF found (Unknown) -> End
        console.log(`[cf42] No CF found (Unknown). Ending.`);
        isComplete = true;
      }
    }

    // 4. Logging (using accumulating cost/tokens)
    await logAITransaction({
      cfName: 'cf42',
      productId: productId || linkedProductId,
      materialId: materialId,
      cost: finalCost,
      totalTokens: finalTokens,
      searchQueries: finalSearchQueries,
      modelUsed: finalModel,
    });

    // logAIReasoning for the INITIAL (original) AI response
    await logAIReasoning({
      sys: SYS_MSG,
      user: currentPrompt,
      thoughts: finalThoughts,
      answer: finalAssistantText,
      cloudfunction: 'apcfSupplierDisclosed-initial',
      productId: productId || linkedProductId,
      materialId: materialId,
      rawConversation: finalRawConversation,
    });

    // 5. If the final text is “Unknown”, end early
    if ( /.*/.test(finalAssistantText)) {
      console.log("[cf42] CF not found - ending early");
      await targetRef.update({ apcfSupplierDisclosedCF_done: true });
      res.json("Done");
      return;
    }

    let originalData = parseResponse(finalAssistantText);

    // Initial variable set from original data
    let parsedProductCF = originalData.productCF;
    let supplierUncert = originalData.uncertainty || "Unknown";
    let packagingFlag = originalData.includePackaging === true; // Default false if null/false
    let standardsList = originalData.standardsList;
    let isIsoAligned = originalData.isIsoAligned;
    let extraInfo = originalData.extraInformation || "Unknown";


    // --- TIKA VERIFICATION STEP REMOVED ---

    // Final logAIReasoning call REMOVED (covered by apcfSupplierDisclosed-initial log)

    // 7. Write back to Firestore conditionally
    if (productId) {
      const pRef = db.collection("products_new").doc(productId);
      const updateData = {
        calc_supplier: true,
        supplier_cf_found: Number.isFinite(parsedProductCF)
      };

      if (Number.isFinite(parsedProductCF)) {
        updateData.supplier_cf = parsedProductCF;
        updateData.cf_full = parsedProductCF;
      }

      if (supplierUncert.toLowerCase() !== 'unknown') {
        updateData.supplier_cf_uncertainty = supplierUncert;
      }

      updateData.includePackaging = packagingFlag;

      if (extraInfo.toLowerCase() !== 'unknown') {
        updateData.extra_information = extraInfo;
      }

      updateData.sdcf_standards = standardsList;
      updateData.sdcf_iso_aligned = isIsoAligned;

      // Only update if there's more than just the flag
      if (Object.keys(updateData).length > 1) {
        await pRef.update(updateData);
        logger.info(`🏁 saved (product) →`, updateData);
      } else {
        logger.warn("[cf42] No valid data found in AI response to update for product.");
      }

      if (currentCollectedUrls.size) {
        await saveURLs({
          urls: Array.from(currentCollectedUrls),
          productId,
          pSDCFData: true,
          sys: SYS_MSG,
          user: userPrompt,
          thoughts: finalThoughts,
          answer: finalAssistantText,
          cloudfunction: 'cf42',
        });
      }
    } else {
      // Update /materials/{materialId}
      const mRef2 = db.collection("materials").doc(materialId);
      const updateData = {
        supplier_data: true,
        supplier_cf_found: Number.isFinite(parsedProductCF)
      };

      if (Number.isFinite(parsedProductCF)) {
        updateData.supplier_disclosed_cf = parsedProductCF;
        updateData.cf_full = parsedProductCF;
      }

      if (supplierUncert.toLowerCase() !== 'unknown') {
        updateData.supplier_cf_uncertainty = supplierUncert;
      }

      updateData.includePackaging = packagingFlag;

      if (extraInfo.toLowerCase() !== 'unknown') {
        updateData.extra_information = extraInfo;
      }

      updateData.sdcf_standards = standardsList;
      updateData.sdcf_iso_aligned = isIsoAligned;

      // Only update if there's more than just the flag
      if (Object.keys(updateData).length > 1) {
        await mRef2.update(updateData);
        logger.info(`🏁 saved (material) →`, updateData);
      } else {
        logger.warn("[cf42] No valid data found in AI response to update for material.");
      }


      if (currentCollectedUrls.size) {
        const mData = (await db.collection("materials").doc(materialId).get()).data() || {};
        const linkedProductId = mData.linked_product ? mData.linked_product.id : null;
        await saveURLs({
          urls: Array.from(currentCollectedUrls),
          materialId,
          productId: linkedProductId,
          mSDCFData: true,
          sys: SYS_MSG,
          user: userPrompt,
          thoughts: finalThoughts,
          answer: finalAssistantText,
          cloudfunction: 'cf42',
        });
      }
    }

    // Restore logAIReasoning for the main conversation summary
    await logAIReasoning({
      sys: SYS_MSG,
      user: userPrompt,
      thoughts: finalThoughts,
      answer: finalAssistantText,
      cloudfunction: 'cf42',
      productId: productId || linkedProductId,
      materialId: materialId,
      rawConversation: finalRawConversation,
    });

    // 9. Return “Done”
    await targetRef.update({ apcfSupplierDisclosedCF_done: true });
    res.json("Done");

  } catch (err) {
    console.error("[cf42] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});