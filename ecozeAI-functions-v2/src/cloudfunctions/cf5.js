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

exports.cf5 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf5] Invoked");

  try {
    /******************** 1. Argument validation ********************/
    const { childMaterialId, reasoningCCF, reasoningTCF } = req.body;

    if (!childMaterialId || (!reasoningCCF && !reasoningTCF)) {
      res.status(400).json({ error: "Provide childMaterialId and at least one reasoning field" });
      return;
    }

    /******************** 2. Data Fetching ********************/
    const cmRef = db.collection("materials").doc(childMaterialId); // This is mDoc
    const cmSnap = await cmRef.get();
    if (!cmSnap.exists) {
      res.status(404).json({ error: `Child material ${childMaterialId} not found` });
      return;
    }
    const cmData = cmSnap.data() || {};

    // Find ppDoc (the ultimate parent product)
    const ppDocRef = cmData.linked_product || null;
    if (!ppDocRef) {
      throw new Error(`Child material ${childMaterialId} has no linked_product reference.`);
    }

    // This section is for constructing the prompt, so it needs parent data, which could be another material.
    // We find the immediate parent document to build the prompt context.
    let parentRef;
    if (cmData.parent_material) {
      parentRef = cmData.parent_material;
    } else {
      parentRef = ppDocRef; // If no parent material, the immediate parent is the product
    }

    const parentSnap = await parentRef.get();
    const parentData = parentSnap.exists ? parentSnap.data() : {};
    const parentName = parentData.name || "Unknown";
    const parentDescription = parentData.description || "No description provided.";
    const parentMass = (parentData.mass && parentData.mass_unit) ? `${parentData.mass} ${parentData.mass_unit}` : "Unknown";
    const parentSupplyChain = parentData.product_chain || "";
    const ecf = (typeof parentData.estimated_cf === 'number') ? `${parentData.estimated_cf} kgCO2e` : "Unknown";
    const scf = (typeof parentData.supplier_cf === 'number') ? `${parentData.supplier_cf} kgCO2e` : "";
    const cf_full = parentData.cf_full || 0;
    const transport_cf = parentData.transport_cf || 0;
    const picf = (cf_full + transport_cf > 0) ? `${cf_full + transport_cf} kgCO2e` : "Unknown";


    /******************** 3. Prompt Construction ********************/
    const childName = cmData.name || "Unknown";
    const childDescription = cmData.description || "No description provided.";
    const childMass = (cmData.mass && cmData.mass_unit) ? `${cmData.mass} ${cmData.mass_unit}` : "Unknown";
    const childCalculatedCF = (typeof cmData.cf_full === 'number') ? `${cmData.cf_full} kgCO2e` : "Unknown";
    const childTransportCF = (typeof cmData.transport_cf === 'number') ? `${cmData.transport_cf} kgCO2e` : "Unknown";

    const childDetailLines = [
      `material_name: ${childName}`,
      `material_description: ${childDescription}`,
      `material_supplier_name: ${cmData.supplier_name || 'Unknown'}`,
    ];

    if (cmData.supplier_address && cmData.supplier_address !== "Unknown") {
      childDetailLines.push(`material_assembly_address: ${cmData.supplier_address}`);
    } else if (cmData.country_of_origin && cmData.country_of_origin !== "Unknown") {
      childDetailLines.push(cmData.coo_estimated ? `material_estimated_coo: ${cmData.country_of_origin}` : `material_coo: ${cmData.country_of_origin}`);
    }

    childDetailLines.push(`material_mass: ${childMass}`);
    childDetailLines.push(`material_calculated_cf: ${childCalculatedCF}`);
    childDetailLines.push(`material_transport_cf: ${childTransportCF}`);
    const childDetailsString = childDetailLines.join('\n');

    let userPrompt = "...";
    if (scf) userPrompt += `\nOfficial Manufacturer Disclosed CF: ${scf}`;
    if (parentSupplyChain) userPrompt += `\nParent Supply Chain: ${parentSupplyChain}`;
    userPrompt += `\nProduct Description: ${parentDescription}`;
    userPrompt += `\n\nChild PCMI:\n\n${childDetailsString}`;

    let previousReasoningString = "";
    const responseMarker = "Response:";
    if (reasoningTCF) {
      const tcfSnap = await cmRef.collection("m_reasoning").where("cloudfunction", "==", "cf49").orderBy("createdAt", "desc").get();
      if (!tcfSnap.empty) {
        previousReasoningString += "\nTransport Reasoning:\n";
        tcfSnap.docs.forEach((doc, i) => {
          const original = doc.data().reasoningOriginal || "";
          const index = original.indexOf(responseMarker);
          previousReasoningString += `TR${i + 1}:\n${index !== -1 ? original.substring(index + responseMarker.length).trim() : original}\n\n`;
        });
      }
    }
    if (reasoningCCF) {
      const ccfSnap = await cmRef.collection("m_reasoning").where("cloudfunction", "==", "cf15").orderBy("createdAt", "desc").limit(1).get();
      if (!ccfSnap.empty) {
        previousReasoningString += "\nCalculated Reasoning:\n";
        const original = ccfSnap.docs[0].data().reasoningOriginal || "";
        const index = original.indexOf(responseMarker);
        previousReasoningString += `${index !== -1 ? original.substring(index + responseMarker.length).trim() : original}\n`;
      }
    }
    if (previousReasoningString) userPrompt += `\n\n-----\n\nPrevious Calculation Reasoning:${previousReasoningString}`;
    userPrompt += `\n-----\n\nCorrection Reasoning:\n`;
    if (reasoningCCF) userPrompt += `${reasoningCCF}\n`;
    if (reasoningTCF) userPrompt += `${reasoningTCF}\n`;

    /******************** 4. Define System Prompt & AI Call ********************/
    const sysPrompt = "...";

    const vGenerationConfig = {
//
//
//
//
//
    };

    const collectedUrls = new Set();

    const { answer, thoughts, cost, totalTokens, searchQueries, model, rawConversation } = await runGeminiStream({
      model: 'aiModel', //flash3
      generationConfig: vGenerationConfig,
      user: userPrompt,
      collectedUrls,
    });

    if (collectedUrls.size) {
      await saveURLs({
        urls: Array.from(collectedUrls),
        productId: ppDocRef.id,
        materialId: childMaterialId,
        pCFAR: true,
        mCFAR: true,
        sys: sysPrompt,
        user: userPrompt,
        thoughts: thoughts,
        answer: answer,
        cloudfunction: 'cf5',
      });
    }

    /******************** 5. Process AI Response & Conditional Logic ********************/
    if (answer.trim().toLowerCase() === "done") {
      logger.info("[cf5] AI returned 'Done'. No changes will be made.");
      res.json("Done");
      return;
    }

    const corrections = parseCFAmendments(answer);
    if (corrections.calculated_cf === null && corrections.transport_cf === null) {
      logger.warn("[cf5] AI response did not contain parsable corrections. No changes made.");
      res.json("Done");
      return;
    }

    const calculatedChanged = corrections.calculated_cf !== null;
    const transportChanged = corrections.transport_cf !== null;

    // Helper for deleting uncertainty documents
    /*
    const deleteUncertaintyDoc = async (docRef, label, matRef) => {
      const uncertaintyQuery = docRef.collection("pn_uncertainty")
        .where("cloudfunction", "==", label)
        .where("material", "==", matRef);
      const snapshot = await uncertaintyQuery.get();
      if (!snapshot.empty) {
        const batch = db.batch();
        snapshot.docs.forEach(doc => batch.delete(doc.ref));
        await batch.commit();
        logger.info(`[cf5] Deleted ${snapshot.size} old '${label}' uncertainty doc(s) for material ${matRef.id}.`);
      }
    };
    */

    if (transportChanged && calculatedChanged) {
      // CASE: Both changed
      logger.info("[cf5] Both transport and calculated CF changed.");
      await logAITransaction({ cfName: 'cf5-full', materialId: childMaterialId, productId: ppDocRef.id, cost, totalTokens, searchQueries, modelUsed: model });
      await logAITransaction({ cfName: 'cf5-transport', materialId: childMaterialId, productId: ppDocRef.id, cost, totalTokens, searchQueries, modelUsed: model });

      const answerFull = answer.replace( /.*/, '').trim();
      const answerTransport = answer.replace( /.*/, '').trim();

      await logAIReasoning({ sys: sysPrompt, user: userPrompt, thoughts, answer: answerFull, cloudfunction: 'cf5-full', materialId: childMaterialId, rawConversation });
      await logAIReasoning({ sys: sysPrompt, user: userPrompt, thoughts, answer: answerTransport, cloudfunction: 'cf5-transport', materialId: childMaterialId, rawConversation });

      //await deleteUncertaintyDoc(ppDocRef, "cf49", cmRef);
      //await callCF("cf50", { materialId: childMaterialId, calculationLabel: "cf49" });

      //await deleteUncertaintyDoc(ppDocRef, "cf15", cmRef);
      //await callCF("cf50", { materialId: childMaterialId, calculationLabel: "cf15" });

    } else if (transportChanged) {
      // CASE: Only Transport changed
      logger.info("[cf5] Only transport CF changed.");
      await logAITransaction({ cfName: 'cf5-transport', materialId: childMaterialId, productId: ppDocRef.id, cost, totalTokens, searchQueries, modelUsed: model });
      await logAIReasoning({ sys: sysPrompt, user: userPrompt, thoughts, answer, cloudfunction: 'cf5-transport', materialId: childMaterialId, rawConversation });
      //await deleteUncertaintyDoc(ppDocRef, "cf49", cmRef);
      //await callCF("cf50", { materialId: childMaterialId, calculationLabel: "cf49" });

    } else if (calculatedChanged) {
      // CASE: Only Calculated changed
      logger.info("[cf5] Only calculated CF changed.");
      await logAITransaction({ cfName: 'cf5-full', materialId: childMaterialId, productId: ppDocRef.id, cost, totalTokens, searchQueries, modelUsed: model });
      await logAIReasoning({ sys: sysPrompt, user: userPrompt, thoughts, answer, cloudfunction: 'cf5-full', materialId: childMaterialId, rawConversation });
      //await deleteUncertaintyDoc(ppDocRef, "cf15", cmRef);
      //await callCF("cf50", { materialId: childMaterialId, calculationLabel: "cf15" });
    }

    /******************** 6. Update DB with CF values ********************/
    const oldCalculatedCF = cmData.cf_full || 0;
    const oldTransportCF = cmData.transport_cf || 0;
    const newCalculatedCF = calculatedChanged ? corrections.calculated_cf : oldCalculatedCF;
    const newTransportCF = transportChanged ? corrections.transport_cf : oldTransportCF;
    const cfDelta = (newCalculatedCF - oldCalculatedCF) + (newTransportCF - oldTransportCF);

    if (cfDelta !== 0) {
      await db.runTransaction(async (transaction) => {
        const cmUpdatePayload = {
          estimated_cf: admin.firestore.FieldValue.increment(cfDelta),
          cf_full: newCalculatedCF,
          transport_cf: newTransportCF,
          updatedAt: admin.firestore.FieldValue.serverTimestamp()
        };
        transaction.update(cmRef, cmUpdatePayload);
        logger.info(`[cf5] Queued update for child ${childMaterialId}. Delta: ${cfDelta}`);

        const pmChain = cmData.pmChain || [];
        for (const link of pmChain) {
          if (!link.documentId || !link.material_or_product) continue;
          const parentDocRef = db.collection(link.material_or_product === 'Product' ? 'products_new' : 'materials').doc(link.documentId);
          transaction.update(parentDocRef, { estimated_cf: admin.firestore.FieldValue.increment(cfDelta) });
          logger.info(`[cf5] Queued propagation to ${link.material_or_product} ${link.documentId}.`);
        }
      });
      logger.info("[cf5] Transaction successfully committed.");
    } else {
      logger.info("[cf5] No net change in CF. No updates needed.");
    }

    res.json("Done");

  } catch (err) {
    logger.error("[cf5] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});