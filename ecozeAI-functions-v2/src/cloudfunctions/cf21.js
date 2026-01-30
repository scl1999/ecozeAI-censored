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

exports.cf21 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  try {
    const collectedUrls = new Set();
    /* ╭── 0. validate input ───────────────────────────────────────────────────────╮ */
    const materialId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || null;
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || null;
    const entityType = productId ? 'product' : 'material';

    if ((materialId && productId) || (!materialId && !productId)) {
      res.status(400).json({ error: "Provide exactly one of materialId OR productId" });
      return;
    }

    const parseCfValue = txt => {
      // The only change is adding a '?' after the '\*' to make the asterisk optional
      const m = txt.match( /.*/);
      if (!m) return null;
      const n = parseFloat(
        m[1]
          .replace( /.*/, "")   // keep digits, dot, e/E, minus
          .replace( /.*/, "")           // strip thousands sep
      );
      return isFinite(n) ? n : null;
    };
    /* ╭── 1. locate target doc ──────────────────────────────────────╮ */
    let targetRef, targetSnap, targetData;
    let SYS_MSG; // Will be set based on input type
    let productChain = "";
    let linkedProductId = null;

    if (productId) {
      targetRef = db.collection("products_new").doc(productId);
      SYS_MSG =
        `...`;
    } else { // materialId is present
      targetRef = db.collection("materials").doc(materialId);
      SYS_MSG =
        `...`;
    }

    targetSnap = await targetRef.get();
    if (!targetSnap.exists) {
      res.status(404).json({ error: `Document not found` });
      return;
    }
    targetData = targetSnap.data() || {};

    if (materialId) {
      productChain = targetData.product_chain || '(unknown chain)';
      linkedProductId = targetData.linked_product?.id || null;
    }

    const prodName = (targetData.name || "").trim();
    const prodMass = targetData.mass ?? null;
    const massUnit = (targetData.mass_unit || "Unknown").trim();

    let peerMaterialsString = "";
    let locationContextString = "";
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

      const locationContextLines = [];
      if (targetData.supplier_name) {
        locationContextLines.push(`Supplier Name: ${targetData.supplier_name}`);
      }

      if (targetData.supplier_address && targetData.supplier_address !== "Unknown") {
        locationContextLines.push(`Manufacturer / Supplier Address: ${targetData.supplier_address}`);
      } else if (targetData.country_of_origin && targetData.country_of_origin !== "Unknown") {
        if (targetData.coo_estimated === true) {
          locationContextLines.push(`Estimated Country of Origin: ${targetData.country_of_origin}`);
        } else {
          locationContextLines.push(`Country of Origin: ${targetData.country_of_origin}`);
        }
      }
      locationContextString = locationContextLines.length > 0
        ? `\n${locationContextLines.join('\n')}`
        : "";
    }

    // 2. Build the "Product Details" string that will be used for aName
    // This includes the special "(Processing EFs)" suffix only for product-level calls.
    const productNameLine = productId
      ? `Product Name: ${prodName} (Processing EFs)`
      : `Product Name: ${prodName}`;

    // Get description from the target document and create the line for the prompt
    const description = targetData.description;
    const descriptionLine = description ? `\nProduct Description: ${description}` : "";

    const aName = `Product Details:
${productNameLine}${productChain ? `\nProduct Chain: ${productChain}` : ''}${descriptionLine}
Mass: ${prodMass ?? "Unknown"}
Mass Unit: ${massUnit}${locationContextString}${peerMaterialsString}`;

    let childMaterialsString = "";
    let childMaterialsSnap;

    if (productId) {
      childMaterialsSnap = await db.collection("materials")
        .where("linked_product", "==", targetRef)
        .where("tier", "==", 1)
        .get();
    } else { // materialId must be present
      childMaterialsSnap = await db.collection("materials")
        .where("parent_material", "==", targetRef)
        .get();
    }

    if (childMaterialsSnap && !childMaterialsSnap.empty) {
      const childLines = [];
      let i = 1;
      for (const childDoc of childMaterialsSnap.docs) {
        const childData = childDoc.data() || {};
        const cf_full = (typeof childData.cf_full === 'number') ? childData.cf_full : "Unknown";
        childLines.push(
          `child_pcmi_${i}: ${childData.name || 'Unknown'}`,
          `child_pcmi_cf_${i}: ${cf_full}`
        );
        i++;
      }
      if (childLines.length > 0) {
        childMaterialsString = "\n\nChild PCMIs:\n\n" + childLines.join('\n');
      }
    }

    const USER_MSG = "..." + childMaterialsString;

    /* ╭── 4. Gemini 2.5-pro single-pass reasoning  ──────────────────────────────╮ */
    const modelUsed = 'aiModel'; //flash3
    const vGenerationConfig = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 32768 // Correct for flash model
      },
    };

    // Get all results from the AI, including thoughts
    const { answer: assistant, thoughts, cost, totalTokens, searchQueries, model, rawConversation } = await runGeminiStream({
      model: modelUsed,
      generationConfig: vGenerationConfig,
      user: USER_MSG,
      collectedUrls
    });

    // Log the AI transaction cost
    await logAITransaction({
      cfName: 'cf21',
      productId: entityType === 'product' ? productId : linkedProductId,
      materialId: materialId,
      cost,
      totalTokens,
      searchQueries: searchQueries,
      modelUsed: model
    });

    // Log the reasoning
    await logAIReasoning({
      sys: SYS_MSG,
      user: USER_MSG,
      thoughts: thoughts,
      answer: assistant,
      cloudfunction: 'cf21',
      productId: productId,
      materialId: materialId,
      rawConversation: rawConversation,
    });

    const aiCalc = parseCfValue(assistant);

    /* ╭── 5. persist to Firestore ──────────────────────────────────────────────────╮ */
    if (aiCalc !== null) {
      const batch = db.batch();

      if (productId) {
        // --- This is the original logic for product-level calls, which remains unchanged ---
        const update = {
          estimated_cf: admin.firestore.FieldValue.increment(aiCalc),
          cf_processing: admin.firestore.FieldValue.increment(aiCalc)
        };
        batch.update(targetRef, update);
        logger.info(`[cf21] Queued update for product ${targetRef.path}:`, JSON.stringify(update));

      } else {
        // --- START: This is the new logic for material-level calls ---
        const cfFullToSubtract = targetData.cf_full || 0;
        // The delta is the new value being added minus the old one being removed from the total.
        const cfDelta = aiCalc - cfFullToSubtract;

        logger.info(`[cf21] Material ${materialId}: Removing old cf_full (${cfFullToSubtract}) from total, adding new cf_processing (${aiCalc}). Net delta: ${cfDelta}`);

        // 1. Update the material document itself ('mDoc')
        const materialUpdate = {
          // Apply the net change to its estimated_cf.
          estimated_cf: admin.firestore.FieldValue.increment(cfDelta),
          // Add the new processing value.
          cf_processing: admin.firestore.FieldValue.increment(aiCalc)
        };
        batch.update(targetRef, materialUpdate);
        logger.info(`[cf21] Queued self-update for material ${targetRef.path}`);

        // 2. & 3. Iterate through the parent chain and update each one
        const parentChain = targetData.pmChain || [];
        if (parentChain.length > 0) {
          logger.info(`[cf21] Propagating net change up the pmChain (${parentChain.length} items).`);
          for (const parent of parentChain) {
            if (!parent.documentId || !parent.material_or_product) continue;

            const collectionName = parent.material_or_product === 'Product' ? 'products_new' : 'materials';
            const parentRef = db.collection(collectionName).doc(parent.documentId);

            batch.update(parentRef, {
              estimated_cf: admin.firestore.FieldValue.increment(cfDelta)
            });
            logger.info(` -> Queued update for ${collectionName}/${parent.documentId}`);
          }
        }
        // --- END: New logic for material-level calls ---
      }

      // Commit all queued operations
      await batch.commit();
      logger.info("[cf21] 🏁 Firestore batch commit successful.");

    } else {
      logger.warn("[cf21] ⚠️ Gemini did not supply a numeric *cf_value*. No updates made.");
    }

    /* ── persist evidence URLs ───────────── */
    if (collectedUrls.size) {
      if (productId) {
        await saveURLs({
          urls: Array.from(collectedUrls),
          productId,
          pMPCFPData: true,
          sys: SYS_MSG,
          user: USER_MSG,
          thoughts: thoughts,
          answer: assistant,
          cloudfunction: 'cf21',
        });
      } else {
        await saveURLs({
          urls: Array.from(collectedUrls),
          materialId,
          productId: linkedProductId,
          mMPCFPData: true,
          sys: SYS_MSG,
          user: USER_MSG,
          thoughts: thoughts,
          answer: assistant,
          cloudfunction: 'cf21',
        });
      }
    }

    logger.info(`[cf21] Starting uncertainty recalculation for 'cf21'...`);
    /*
        if (productId) {
          // 1. Delete the old 'cf15' uncertainty doc for this product.
          const uncertaintyQuery = targetRef.collection("pn_uncertainty")
            .where("cloudfunction", "==", "cf15")
            .where("material", "==", null);
    
          const oldUncertaintySnap = await uncertaintyQuery.get();
          if (!oldUncertaintySnap.empty) {
            const batch = db.batch();
            oldUncertaintySnap.docs.forEach(doc => {
              batch.delete(doc.ref);
            });
            await batch.commit();
            logger.info(`[cf21] Deleted ${oldUncertaintySnap.size} old 'cf15' uncertainty doc(s) for product ${productId}.`);
          }
    
          // 2. Trigger the new uncertainty calculation.
          logger.info(`[cf21] Triggering cf50 for product ${productId}.`);
          await callCF("cf50", {
            productId: productId,
            calculationLabel: "cf21"
          });
    
        } else if (materialId) {
          const linkedProductRef = targetData.linked_product;
          if (linkedProductRef) {
            // 1. Delete the old 'cf15' uncertainty doc for this material.
            const uncertaintyQuery = linkedProductRef.collection("pn_uncertainty")
              .where("cloudfunction", "==", "cf15")
              .where("material", "==", targetRef); // targetRef is the material ref
    
            const oldUncertaintySnap = await uncertaintyQuery.get();
            if (!oldUncertaintySnap.empty) {
              const batch = db.batch();
              oldUncertaintySnap.docs.forEach(doc => {
                batch.delete(doc.ref);
              });
              await batch.commit();
              logger.info(`[cf21] Deleted ${oldUncertaintySnap.size} old 'cf15' uncertainty doc(s) for material ${materialId}.`);
            }
    
            // 2. Trigger the new uncertainty calculation.
            logger.info(`[cf21] Triggering cf50 for material ${materialId}.`);
            await callCF("cf50", {
              materialId: materialId,
              calculationLabel: "cf21"
            });
          } else {
            logger.warn(`[cf21] Material ${materialId} has no linked_product, skipping uncertainty calculation.`);
          }
        }
          */

    /******************** 7. Trigger Other Metrics Calculation (Conditional) ********************/
    logger.info(`[cf21] Checking if other metrics calculation is needed...`);

    if (productId) {
      // Re-fetch the latest data to check the flag
      const pSnap = await targetRef.get();
      const pData = pSnap.data() || {};

      if (pData.otherMetrics === true) {
        logger.info(`[cf21] otherMetrics flag is true for product ${productId}. Running post-processing.`);

        // 1. Delete the old 'cf15' otherMetrics doc for this product.
        const metricsQuery = targetRef.collection("pn_otherMetrics")
          .where("cloudfunction", "==", "cf15")
          .where("material", "==", null);

        const oldMetricsSnap = await metricsQuery.get();
        if (!oldMetricsSnap.empty) {
          const batch = db.batch();
          oldMetricsSnap.docs.forEach(doc => {
            batch.delete(doc.ref);
          });
          await batch.commit();
          logger.info(`[cf21] Deleted ${oldMetricsSnap.size} old 'cf15' otherMetrics doc(s) for product ${productId}.`);
        }

        // 2. Trigger the new otherMetrics calculation.
        logger.info(`[cf21] Triggering cf30 for product ${productId}.`);
        await callCF("cf30", {
          productId: productId,
          calculationLabel: "cf21"
        });
      }
    } else if (materialId) {
      const linkedProductRef = targetData.linked_product;
      if (linkedProductRef) {
        const linkedProductSnap = await linkedProductRef.get();
        if (linkedProductSnap.exists) {
          const linkedProductData = linkedProductSnap.data() || {};
          if (linkedProductData.otherMetrics === true) {
            logger.info(`[cf21] otherMetrics flag is true for linked product ${linkedProductRef.id}.`);

            // 1. Delete the old 'cf15' otherMetrics doc for this material.
            const metricsQuery = linkedProductRef.collection("pn_otherMetrics")
              .where("cloudfunction", "==", "cf15")
              .where("material", "==", targetRef); // targetRef is the material ref

            const oldMetricsSnap = await metricsQuery.get();
            if (!oldMetricsSnap.empty) {
              const batch = db.batch();
              oldMetricsSnap.docs.forEach(doc => {
                batch.delete(doc.ref);
              });
              await batch.commit();
              logger.info(`[cf21] Deleted ${oldMetricsSnap.size} old 'cf15' otherMetrics doc(s) for material ${materialId}.`);
            }

            // 2. Trigger the new otherMetrics calculation.
            logger.info(`[cf21] Triggering cf30 for material ${materialId}.`);
            await callCF("cf30", {
              materialId: materialId,
              calculationLabel: "cf21"
            });
          }
        }
      } else {
        logger.warn(`[cf21] Material ${materialId} has no linked_product, skipping other metrics calculation.`);
      }
    }

    await targetRef.update({ apcfMPCF_done: true });
    res.json("Done");

  } catch (err) {
    console.error("[cf21] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});