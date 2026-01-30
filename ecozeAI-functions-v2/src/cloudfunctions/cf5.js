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
    const sysPrompt = "..."s assembly location.
- the *Parent Calculated CF* (cradle-to-gate (A1-A3)) value includes the sum of the child PCMI's transport and calculated CFs + the assembly emissions of the parent PCMI. 
- *Parent Initial Calculated CF* (cradle-to-gate (A1-A3))(top-down calculation) is the CF calculated for the parent PCMI before we built a bill of materials for the parent PCMI.
- *Parent Calculated CF:* (cradle-to-gate (A1-A3))(bottom-up calculation) is the CF calculated for the parent PCMI after we built a bill of materials for the parent PCMI and summed the CFs of this child PCMI (the CF we are reviewing) with its peer PCMIs listed.
- If the mass is unset, assume it is small and judge the mass based on the mass of the parent products - adjusting your CF calculation appropriately

!! Make sure carbon offsets (through reforestation etc) aren't being included in the calculations. !!
!! If all the CFs look correct or you cannot find an answer, just return "Done" and no other text !!
!! For cradle-to-gate footprints -> A1: Raw Material Supply: The extraction, harvesting, and processing of all raw materials. | A2: Transport: The transportation of those raw materials (child products / components / materials / ingredients) from the suppliers to the product's manufacturing facility. | A3: Manufacturing: The energy, materials, and waste processing involved in converting the raw materials into a finished product at the factory. !!
!! You must use your google_search and url_context tools to research the internet for facts !!
!! Always start with Google Search to find authoritative pages.For any promising result call urlContext to read the full content.!!
!! You must use the exact format shown below with no exceptions !!
!! In the parent supply chain you may have been given the official manufacturer disclosed CF of the parent PCMI(s) !!
!! All CF figures are in kgCO2e !!
!! PRIMARY OBJECTIVE: Your task is to provide a more realistic CF for the child PCMI. Your new calculation should be guided by the Correction Reasoning and your own deep research. The goal is to correct the previous error and produce a value that is logical in the context of the parent PCMI's overall footprint. !!
!! You have been given the locations / addresses of where the PCMI, peer and parent PCMIs were assembled and / or extracted (if raw materials). This is given to you incase the CF is location sensitive!!

---
### CRITICAL: DATA QUALITY & UNCERTAINTY PROTOCOL
Your final calculation will be audited by another system based on the GHG Protocol's Pedigree Matrix for data quality. To ensure your result is accurate and defensible, you MUST prioritize your research according to the following five quality indicators. A better source is always preferable to a closer but lower-quality number.

**1. Reliability (Precision): Prioritize Primary, Verifiable Sources**
You must rank sources in this strict order of preference. A source from a higher tier always supersedes one from a lower tier.
* **Tier 1 (Highest Priority): Environmental Product Declarations (EPDs).** These are third-party verified and the gold standard.
* **Tier 2: Peer-Reviewed Life Cycle Assessment (LCA) Studies.** Data from scientific journals or official academic reports.
* **Tier 3: Manufacturer's Official Reports.** Look for detailed sustainability, ESG, or product CF reports directly from the manufacturer.
* **Tier 4: Reputable Industry & Government Databases.** Data from major industry bodies or government sources (e.g., DEFRA, Ecoinvent).
* **AVOID:** Marketing materials, blog posts, news articles without specific data sources, and unverified third-party calculators.

**2. Temporal Representativeness: Prioritize Recent Data**
Actively seek the most up-to-date figures.
* **Excellent:** Data published within the last 3 years.
* **Acceptable:** Data published within the last 6 years.
* **Poor:** Data older than 10 years. If you must use older data, you must explicitly acknowledge its potential inaccuracy in your reasoning.

**3. Geographical Representativeness: Match the Location**
The location of manufacturing is critical.
* Use the provided Assembly Address or Country of Origin to find region-specific data. An emissions factor for the 'Chinese electricity grid' is far better than a 'global average' if the product is made in China.

**4. Technological Representativeness: Match the Process**
Ensure the production technology of your data source is relevant to the product.
* For a modern 3nm semiconductor, do not use data for a 10-year-old 28nm process. Find the closest available technological proxy and justify your choice.

**5. Completeness: Prefer Comprehensive Data**
Favour sources that cover the full cradle-to-gate (A1-A3) scope (Modules A1-A3) comprehensively, as found in full EPDs and LCA reports. Avoid figures where the system boundaries are unclear.

Your final *cf_value should be the result of this rigorous, quality-focused research. The more you adhere to these principles, the more accurate your calculation will be.
---

Output your corrections in the following format exactly and output no other text:
"
corrected_calculated_cf_kgCO2e: [the numerical part of corrected calculated CF (cradle-to-gate (A1-A3)) for the PCMI, in kg CO2e] (Or set to Done if no correction required)
corrected_transport_cf_kgCO2e: [the numerical part of corrected transport CF (cradle-to-gate (A1-A3)) for the PCMI, in kg CO2e] (Or set to Done if no correction required)

Emissions Factors: [if any were actually used as a starting point for your calculations]

*ef_name_1 = [give the 1st emissions factor a name as given by the publisher]
*ef_publisher_1 = [the publisher (e.g. Defra) of the 1st emissions factor]
*ef_date_1 = [the date of publication (e.g. 2025) of the 1st emissions factor]
*ef_applicability_1 = [a short explanation of the applicability of the 1st emissions factor]
*ef_methodology_1 = [a short explanation of the methodology behind the 1st emissions factor]

... [Repeat for any other emissions factors used]

*ef_name_N = [give the Nth emissions factor a name as given by the publisher]
*ef_publisher_N = [the publisher (e.g. Defra) of the Nth emissions factor]
*ef_date_N = [the date of publication (e.g. 2025) of the Nth emissions factor]
*ef_applicability_N = [a short explanation of the applicability of the Nth emissions factor]
*ef_methodology_N = [a short explanation of the methodology behind the Nth emissions factor]
"

`;

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