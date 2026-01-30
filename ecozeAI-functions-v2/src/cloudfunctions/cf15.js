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

exports.cf15 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  /* ╭── 0. validate input ───────────────────────────────────────────────────────╮ */
  const materialId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || null;
  const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || null;
  try {
    const collectedUrls = new Set();

    if ((materialId && productId) || (!materialId && !productId)) {
      res.status(400).json({ error: "Provide exactly one of materialId OR productId" });
      return;
    }

    const parseCfValue = txt => {
      // 1. Sanitize the input to replace non-breaking spaces with regular spaces
      const sanitizedTxt = txt.replace(/.*/, ' ');

      // 2. Run the regex on the sanitized string
      const m = sanitizedTxt.match(/.*/);
      if (!m) return null;

      const n = parseFloat(
        m[1]
          .replace(/.*/, "")
          .replace(/.*/, "")
      );
      return isFinite(n) ? n : null;
    };

    /* ╭── 1. locate target doc ──────────────────────────────────────╮ */
    let targetRef, targetSnap, targetData;
    if (productId) {
      targetRef = db.collection("products_new").doc(productId);
    } else {
      targetRef = db.collection("materials").doc(materialId);
    }
    targetSnap = await targetRef.get();
    if (!targetSnap.exists) {
      res.status(404).json({ error: `Document not found` });
      return;
    }
    targetData = targetSnap.data() || {};
    let systemPrompt;
    if (productId && targetData.ecozeAI_Pro === false) {
      logger.info(`[cf15] Using CORE system prompt for product ${productId}.`);
      systemPrompt = SYS_MSG_MPCFFULL_CORE;
    } else {
      logger.info(`[cf15] Using PRO system prompt for material or Pro product.`);
      systemPrompt = SYS_MSG_MPCFFULL_PRO;
    }
    const entityType = productId ? 'product' : 'material';
    const linkedProductId = targetData.linked_product?.id || null;
    let productChain = "";

    if (materialId) {
      productChain = targetData.product_chain || '(unknown chain)';
    }

    let extraInfoString = "";
    if (productId) {
      const pExtraInfo = targetData.extra_information;
      if (pExtraInfo) {
        extraInfoString = `\nExtra Information: \n${pExtraInfo}`;
      }
    } else { // materialId is present
      const mExtraInfo = targetData.extra_information;
      if (mExtraInfo) {
        extraInfoString = `\nExtra Information (This PCMI):\n${mExtraInfo}`;
      }

      if (targetData.linked_product) {
        const pSnap = await targetData.linked_product.get();
        if (pSnap.exists) {
          const pData = pSnap.data() || {};
          const pExtraInfo = pData.extra_information;
          if (pExtraInfo) {
            extraInfoString += `\n\nExtra Information (Overall Parent / End Product):\n${pExtraInfo}`;
          }
        }
      }
    }


    const prodName = (targetData.name || "").trim();
    const prodMass = targetData.mass ?? null;
    const massUnit = (targetData.mass_unit || "Unknown").trim();

    /* ╭── 2. Generate Peer Materials string ONCE ──────────────────────────────────╮ */
    let peerMaterialsString = "";
    if (materialId) {
      let peerMaterialsSnap;
      if (targetData.parent_material) {
        peerMaterialsSnap = await db.collection("materials")
          .where("parent_material", "==", targetData.parent_material)
          .get();
      } else if (targetData.linked_product) {
        peerMaterialsSnap = await db.collection("materials")
          .where("linked_product", "==", targetData.linked_product)
          .where("tier", "==", 1)
          .get();
      }

      if (peerMaterialsSnap && !peerMaterialsSnap.empty) {
        const peerLines = [];
        let i = 1;
        for (const peerDoc of peerMaterialsSnap.docs) {
          if (peerDoc.id === materialId) continue; // Skip self
          const peerData = peerDoc.data() || {};

          peerLines.push(`material_${i}_name: ${peerData.name || 'Unknown'}`);
          peerLines.push(`material_${i}_supplier_name: ${peerData.supplier_name || 'Unknown'}`);

          // Conditionally add the supplier address if it exists and isn't "Unknown"
          if (peerData.supplier_address && peerData.supplier_address.toLowerCase() !== 'unknown') {
            peerLines.push(`material_${i}_supplier_address: ${peerData.supplier_address}`);
          }

          peerLines.push(`material_${i}_description: ${peerData.description || 'No description provided.'}`);
          i++;
        }
        if (peerLines.length > 0) {
          peerMaterialsString = "\n\nPeer Materials:\n" + peerLines.join('\n');
        }
      }
    }

    let materialContextString = "";
    let parentCFLine = ""; // <-- Add this line

    if (materialId) {
      // --- START: New logic to fetch parent CF ---
      let parentDocRef;
      if (targetData.parent_material) { // Scenario 2: Parent is a material
        parentDocRef = targetData.parent_material;
      } else if (targetData.linked_product) { // Scenario 1: Parent is a product
        parentDocRef = targetData.linked_product;
      }

      if (parentDocRef) {
        const parentSnap = await parentDocRef.get();
        if (parentSnap.exists) {
          const pCF = parentSnap.data().cf_full;
          if (typeof pCF === 'number' && isFinite(pCF)) {
            parentCFLine = `\nTop-Level CF (cradle-to-gate (A1-A3)) Calculation for Parent PCMI: ${pCF}`;
          }
        }
      }
      // --- END: New logic ---

      const contextLines = [];

      if (targetData.supplier_name) {
        contextLines.push(`Supplier Name: ${targetData.supplier_name}`);
      }

      if (targetData.supplier_address && targetData.supplier_address !== "Unknown") {
        contextLines.push(`Manufacturer / Supplier Address: ${targetData.supplier_address}`);
      } else if (targetData.country_of_origin && targetData.country_of_origin !== "Unknown") {
        if (targetData.coo_estimated === true) {
          contextLines.push(`Estimated Country of Origin: ${targetData.country_of_origin}`);
        } else {
          contextLines.push(`Country of Origin: ${targetData.country_of_origin}`);
        }
      }

      if (contextLines.length > 0) {
        materialContextString = `\n${contextLines.join('\n')}`;
      }
    }

    const descriptionLine = targetData.description ? `\nProduct Description: ${targetData.description}` : "";

    const aName = `Product Details:
Product Name: ${prodName}${parentCFLine}${productChain ? `\nProduct Chain: ${productChain}` : ""}${descriptionLine}
Mass: ${prodMass ?? "Unknown"}
Mass Unit: ${massUnit}${materialContextString}${extraInfoString ? `\n${extraInfoString.trim()}` : ""}${peerMaterialsString}`;

    const USER_MSG = "...";

    /* ╭── 5. Gemini 2.5-pro single-pass reasoning  ──────────────────────────────╮ */
    /* ╭── 5. Conditional AI Logic: Pro -> Flash -> Pro (Auditor Loop) for Products ───╮ */
    let assistant, thoughts, cost, totalTokens, searchQueries, model, rawConversation;

    if (productId) {
      logger.info(`[cf15] Starting Pro-Flash-Pro auditor loop for product ${productId}.`);

      const proConfig = {
//
//
//
//
//
      };

      const SYS_MSG_FLASH_AUDITOR = "...";
      const auditorConfig = {
//
//
//
//
//
          includeThoughts: true,
          thinkingBudget: 32768
        },
      };

      // --- STEP 1: PRE-LOOP "Go Again" Refinement (from cf15) ---
      logger.info(`[cf15] Running initial "Go Again" refinement before entering Auditor Loop.`);
      const followUpPrompt = "..."

      // Use runChatLoop which handles the multi-turn conversation
      let turnIndex = 0;
      const chatResult = await runChatLoop({
        model: 'aiModel',
        generationConfig: proConfig,
        initialPrompt: USER_MSG,
        followUpPrompt: followUpPrompt,
        maxFollowUps: 1,
        collectedUrls,
        onTurnComplete: async (history, answer) => {
          if (turnIndex === 0) {
            await targetRef.update({ cf_calculation_initial: true });
          } else if (turnIndex === 1) {
            await targetRef.update({ cf_calculation_second: true });
          }
          turnIndex++;
        }
      });

      // Initialize Loop Variables with Chat Result
      let loopCount = 0;
      const MAX_AUDIT_LOOPS = 2;
      let stopLoop = false;
      let auditorFeedback = "";

      let proCost = chatResult.cost;
      let proTokens = chatResult.tokens;
      let proQueries = chatResult.searchQueries;
      let proGeneratedThoughts = chatResult.history.map(turn => {
        const role = turn.role === 'user' ? '👤 User' : '🤖 AI';
        const content = turn.parts.map(part => {
          if (part.text) return '';
          if (part.functionCall) return `[TOOL CALL]: \n${JSON.stringify(part.functionCall, null, 2)} `;
          const thoughtText = JSON.stringify(part, null, 2);
          if (thoughtText && thoughtText !== '{}') return `[AI THOUGHT]: \n${thoughtText} `;
          return '';
        }).filter(Boolean).join('\n');
        return `-- - ${role} ---\n${content} `;
      }).join('\n\n');

      let proRawConv = chatResult.rawConversation;
      let lastProAnswer = chatResult.finalAnswer;
      let lastProModel = chatResult.model;

      let flashInteractions = [];

      // --- STEP 2: AUDITOR LOOP ---
      while (loopCount < MAX_AUDIT_LOOPS && !stopLoop) {
        loopCount++;
        logger.info(`[cf15] Loop Iteration ${loopCount} of ${MAX_AUDIT_LOOPS} `);

        if (loopCount === 1) {
          // Iteration 1: Using the result from the "Go Again" pre-loop step.
          logger.info(`[cf15] Using result from "Go Again" refinement for first audit.`);
        } else {
          // Iteration 2+: Run PRO model with feedback
          logger.info(`[cf15] Calling PRO model(Attempt ${loopCount}) with feedback...`);

          let encodedProPrompt = "...";

          const proResult = await runGeminiStream({
            model: 'aiModel',
            generationConfig: proConfig,
            user: encodedProPrompt,
            history: proRawConv,
            collectedUrls
          });

          lastProAnswer = proResult.answer;
          lastProModel = proResult.model;
          proCost += proResult.cost;
          proTokens.input += proResult.totalTokens.input;
          proTokens.output += proResult.totalTokens.output;
          proQueries.push(...(proResult.searchQueries || []));

          proGeneratedThoughts += `\n\n-- - PRO Iteration ${loopCount} ---\n${proResult.thoughts} `;

          proRawConv = proResult.rawConversation;
        }

        logger.info(`[cf15] Calling FLASH Auditor(Attempt ${loopCount})...`);

        let auditorUserPrompt = "...";

        if (loopCount > 1) {
          auditorUserPrompt = "The AI has had another go. Shown below is its response.\n" + auditorUserPrompt;
        }

        const flashResult = await runGeminiStream({
          model: 'aiModel',
          generationConfig: auditorConfig,
          user: auditorUserPrompt,
          collectedUrls
        });

        flashInteractions.push({
          cost: flashResult.cost,
          tokens: flashResult.totalTokens,
          model: flashResult.model,
          thoughts: flashResult.thoughts,
          answer: flashResult.answer,
          rawConversation: flashResult.rawConversation,
          sys: SYS_MSG_FLASH_AUDITOR,
          user: auditorUserPrompt
        });

        const ratingMatch = flashResult.answer.match(/.*/);
        const ratingReasoningMatch = flashResult.answer.match(/.*/);

        const rating = ratingMatch ? ratingMatch[1].trim() : "Pass";
        const reasoning = ratingReasoningMatch ? ratingReasoningMatch[1].trim() : "No reasoning provided.";

        logger.info(`[cf15] Auditor Rating: ${rating}`);

        if (rating.toLowerCase() === 'pass') {
          logger.info(`[cf15] Auditor passed the calculation. Stopping loop.`);
          stopLoop = true;
        } else {
          auditorFeedback = reasoning;
          logger.info(`[cf15] Auditor requested refinement. Feedback: ${auditorFeedback.substring(0, 100)}...`);
        }
      }

      if (!stopLoop && auditorFeedback) {
        logger.info(`[cf15] Max loops reached ending on Refine. Triggering FINAL Pro run.`);

        const finalProPrompt = "...";

        const finalProResult = await runGeminiStream({
          model: 'aiModel',
          generationConfig: proConfig,
          user: finalProPrompt,
          history: proRawConv,
          collectedUrls
        });

        lastProAnswer = finalProResult.answer;
        lastProModel = finalProResult.model;
        proCost += finalProResult.cost;
        proTokens.input += finalProResult.totalTokens.input;
        proTokens.output += finalProResult.totalTokens.output;
        proQueries.push(...(finalProResult.searchQueries || []));
        proGeneratedThoughts += `\n\n--- PRO Final Iteration ---\n${finalProResult.thoughts}`;
        proRawConv = finalProResult.rawConversation;
      }

      assistant = lastProAnswer;
      thoughts = proGeneratedThoughts;
      cost = proCost;
      totalTokens = proTokens;
      searchQueries = proQueries;
      model = lastProModel;
      rawConversation = proRawConv;

      for (let i = 0; i < flashInteractions.length; i++) {
        const fi = flashInteractions[i];
        const fName = `cf15-Auditor-${i + 1}`;

        await logAITransaction({
          cfName: fName,
          productId,
          cost: fi.cost,
          totalTokens: fi.tokens,
          modelUsed: fi.model,
        });

        await logAIReasoning({
          sys: fi.sys,
          user: fi.user,
          thoughts: fi.thoughts,
          answer: fi.answer,
          cloudfunction: fName,
          productId: productId,
          rawConversation: fi.rawConversation
        });
      }

    } else { // This is the original logic for materials
      logger.info(`[cf15] Starting single-pass call for material ${materialId}.`);
      const vGenerationConfig = {
//
//
//
//
//
          includeThoughts: true,
          thinkingBudget: 32768
        },
      };

      const streamResult = await runGeminiStream({
        model: 'aiModel', //pro
        generationConfig: vGenerationConfig,
        user: USER_MSG,
        collectedUrls
      });
      await targetRef.update({ cf_calculation_initial: true });

      // Assign results to the shared variables
      assistant = streamResult.answer;
      thoughts = streamResult.thoughts;
      cost = streamResult.cost;
      totalTokens = streamResult.totalTokens;
      searchQueries = streamResult.searchQueries;
      model = streamResult.model;
      rawConversation = streamResult.rawConversation;
    }

    await logAITransaction({
      cfName: 'cf15',
      productId: entityType === 'product' ? productId : linkedProductId,
      materialId: materialId,
      cost,
      totalTokens,
      searchQueries: searchQueries,
      modelUsed: model,
    });

    await logAIReasoning({
      sys: systemPrompt,
      user: USER_MSG,
      thoughts: thoughts,
      answer: assistant,
      cloudfunction: 'cf15',
      productId: entityType === 'product' ? productId : linkedProductId,
      materialId: materialId,
      rawConversation: rawConversation,
    });

    let aiCalc = null;
    const lastCfIndex = assistant.lastIndexOf('*cf_value =');

    if (lastCfIndex !== -1) {
      // If we found at least one occurrence, parse from the last one forward.
      const lastAnswerBlock = assistant.substring(lastCfIndex);
      aiCalc = parseCfValue(lastAnswerBlock);
    } else {
      // Fallback for cases where the AI might not use the exact format, but has a value.
      aiCalc = parseCfValue(assistant);
    }

    /* ╭── 6. Persist to Firestore (if successful) ─────────────────────╮ */
    if (aiCalc !== null) {
      logger.info(`[cf15] ✅ AI call succeeded with cf_value: ${aiCalc}`);
      const batch = db.batch();

      if (productId) {
        // Logic for a top-level product
        const update = {
          estimated_cf: admin.firestore.FieldValue.increment(aiCalc),
          cf_full: admin.firestore.FieldValue.increment(aiCalc)
        };
        batch.update(targetRef, update);
        logger.info(`[cf15] Queued update for product ${targetRef.path}`);
      } else {
        // Logic for a material and propagating the value up its pmChain
        const materialUpdate = {
          estimated_cf: admin.firestore.FieldValue.increment(aiCalc),
          cf_full: admin.firestore.FieldValue.increment(aiCalc)
        };
        batch.update(targetRef, materialUpdate);
        logger.info(`[cf15] Queued update for target material ${targetRef.path}`);

        const pmChain = targetData.pmChain || [];
        logger.info(`[cf15] Found ${pmChain.length} documents in pmChain to update.`);

        for (const link of pmChain) {
          if (!link.documentId || !link.material_or_product) continue;
          let parentRef;
          if (link.material_or_product === "Product") {
            parentRef = db.collection("products_new").doc(link.documentId);
          } else {
            parentRef = db.collection("materials").doc(link.documentId);
          }
          batch.update(parentRef, { estimated_cf: admin.firestore.FieldValue.increment(aiCalc) });
          logger.info(`[cf15] Queued estimated_cf increment for ${link.material_or_product} ${parentRef.path}`);
        }
      }

      await batch.commit();
      logger.info(`[cf15] 🏁 Firestore updates committed for value: ${aiCalc}`);
    } else {
      logger.warn("[cf15] ⚠️ AI did not return a numeric cf_value. No updates will be made.");
    }

    /* ── persist evidence URLs ───────────── */
    if (collectedUrls.size) {
      if (productId) {
        await saveURLs({
          urls: Array.from(collectedUrls),
          productId,
          pMPCFData: true,
          sys: systemPrompt,
          user: USER_MSG,
          thoughts: thoughts,
          answer: assistant,
          cloudfunction: 'cf15',
        });
      } else {
        const linkedProductId = targetData.linked_product?.id || null;
        await saveURLs({
          urls: Array.from(collectedUrls),
          materialId,
          productId: linkedProductId,
          mMPCFData: true,
          sys: systemPrompt,
          user: USER_MSG,
          thoughts: thoughts,
          answer: assistant,
          cloudfunction: 'cf15',
        });
      }
    }

    logger.info(`[cf15] Triggering uncertainty calculation...`);

    /*
     
    if (productId) {
      await callCF("cf50", {
        productId: productId,
        calculationLabel: "cf15"
      });
      logger.info(`[cf15] Completed uncertainty calculation for product ${productId}.`);
    } else if (materialId) {
      await callCF("cf50", {
        materialId: materialId,
        calculationLabel: "cf15"
      });
      logger.info(`[cf15] Completed uncertainty calculation for material ${materialId}.`);
    }
      */

    /******************** 8. Trigger Other Metrics Calculation (Conditional) ********************/
    logger.info(`[cf15] Checking if other metrics calculation is needed...`);

    if (productId) {
      // targetData already holds the product data from the initial fetch
      if (targetData.otherMetrics === true) {
        logger.info(`[cf15] otherMetrics flag is true for product ${productId}. Triggering calculation.`);
        await callCF("cf30", {
          productId: productId,
          calculationLabel: "cf15"
        });
      }
    } else if (materialId) {
      // targetData holds the material data
      const linkedProductRef = targetData.linked_product;
      if (linkedProductRef) {
        const mpSnap = await linkedProductRef.get();
        if (mpSnap.exists) {
          const mpData = mpSnap.data() || {};
          if (mpData.otherMetrics === true) {
            logger.info(`[cf15] otherMetrics flag is true for linked product ${linkedProductRef.id}. Triggering calculation for material ${materialId}.`);
            await callCF("cf30", {
              materialId: materialId,
              calculationLabel: "cf15"
            });
          }
        }
      } else {
        logger.warn(`[cf15] No linked product found for material ${materialId}, skipping other metrics calculation.`);
      }
    }
    if (productId) {
      logger.info(`[cf15] Running refine calculation check for product ${productId}.`);

      // 1. Fetch data for prompts
      const latestProductSnap = await targetRef.get();
      const latestProductData = latestProductSnap.data() || {};
      const cf_full = latestProductData.cf_full || 0;
      let originalReasoning = "No reasoning found.";
      const reasoningQuery = targetRef.collection("pn_reasoning")
        .where("cloudfunction", "==", "cf15")
        .orderBy("createdAt", "desc").limit(1);
      const reasoningSnap = await reasoningQuery.get();
      if (!reasoningSnap.empty) {
        originalReasoning = reasoningSnap.docs[0].data().reasoningOriginal || "";
      }

      // 2. Define prompts
      const SYS_MSG_RC = "...";
      const rcUserPrompt = "...";

      // 3. Configure and execute AI call (with potential retry)
      const rcConfig = {
//
//
//
//
      };

      let { answer: rcAnswer, ...rcAiResults } = await runGeminiStream({
        model: 'openai/gpt-oss-120b-maas', //gpt-oss-120b
        generationConfig: rcConfig,
        user: rcUserPrompt,
      });

      let beneficialMatch = rcAnswer.match(/.*/);
      let reasoningMatch = rcAnswer.match(/.*/);

      if (!beneficialMatch || !reasoningMatch) {
        logger.warn("[cf15] Refine check AI failed format. Retrying once.");
        const retryPrompt = "...";
        const retryResults = await runGeminiStream({
          model: 'openai/gpt-oss-120b-maas', //gpt-oss-120b
          generationConfig: rcConfig,
          user: `${rcUserPrompt} \n\nPrevious invalid response: \n${rcAnswer} \n\n${retryPrompt} `,
        });

        // Aggregate results from both attempts
        rcAnswer = retryResults.answer;
        rcAiResults.thoughts += `\n\n-- - RETRY ATTEMPT-- -\n\n${retryResults.thoughts} `;
        rcAiResults.rawConversation.push(...retryResults.rawConversation);
        rcAiResults.cost += retryResults.cost;
        rcAiResults.totalTokens.input += retryResults.totalTokens.input;
        rcAiResults.totalTokens.output += retryResults.totalTokens.output;
        rcAiResults.totalTokens.toolCalls += retryResults.totalTokens.toolCalls;

        // Re-parse after retry
        beneficialMatch = rcAnswer.match(/.*/);
        reasoningMatch = rcAnswer.match(/.*/);
      }

      // 4. Log transaction and reasoning
      await logAITransaction({
        cfName: 'cf15-RefineCheck',
        productId: productId,
        cost: rcAiResults.cost,
        totalTokens: rcAiResults.totalTokens,
        modelUsed: rcAiResults.model,
      });

      await logAIReasoning({
        sys: SYS_MSG_RC,
        user: rcUserPrompt,
        thoughts: rcAiResults.thoughts,
        answer: rcAnswer,
        cloudfunction: 'cf15-RefineCheck',
        productId: productId,
        rawConversation: rcAiResults.rawConversation,
      });

      // 5. Update Firestore with the final decision
      if (beneficialMatch && reasoningMatch) {
        const isBeneficial = /.*/.test(beneficialMatch[1].trim());
        const reasoningText = reasoningMatch[1].trim();
        const updatePayload = { rcPossible: isBeneficial, rcReasoning: reasoningText };
        await targetRef.update(updatePayload);
        logger.info(`[cf15] Saved refine check result for product ${productId}: `, updatePayload);
      } else {
        await targetRef.update({
          rcPossible: false,
          rcReasoning: "AI failed to determine if a refined calculation would be beneficial after a retry.",
        });
        logger.error("[cf15] Failed to parse refine check AI response after retry. Defaulting rcPossible to false.");
      }
    }

    await incrementChildProgress(materialId, 'cf15');
    await targetRef.update({ apcfMPCF_done: true });
    res.json("Done");

  } catch (err) {
    console.error("[cf15] Uncaught error:", err);
    await incrementChildProgress(materialId, 'cf15');
    res.status(500).json({ error: String(err) });
  }
});