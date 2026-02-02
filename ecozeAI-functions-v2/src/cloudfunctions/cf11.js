//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf11 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf11] bootstrap invocation");
  logger.info(`[cf11] HEADERS DEBUG: ${JSON.stringify(req.headers)}`);

  try {
    // --- 0. Cloud Task Retry Check ---
    // If this is a retry from Cloud Tasks (likely due to timeout), abort to prevent duplicates.
    const ctRetryCount = req.headers['x-cloud-tasks-taskretrycount'];
    if (ctRetryCount && Number(ctRetryCount) > 0) {
      logger.warn(`[cf11] Cloud Task Retry detected (count: ${ctRetryCount}). Aborting to prevent duplicate executions.`);
      res.status(200).send("Retry aborted.");
      return;
    }

    // --- 1. Argument Parsing ---
    const product_name = (req.method === "POST" ? req.body?.product_name : req.query.product_name) || "";
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || "";
    const userId = (req.method === "POST" ? req.body?.userId : req.query.userId) || null;
    const orgId = (req.method === "POST" ? req.body?.orgId : req.query.orgId) || null;
    const ecozeAIPro = (req.method === "POST" ? req.body?.ecozeAIPro : req.query.ecozeAIPro);
    const ecozeAIProBool = ecozeAIPro === "true";
    const otherMetrics = (req.method === "POST" ? req.body?.otherMetrics : req.query.otherMetrics);
    const otherMetricsBool = otherMetrics === true || otherMetrics === "true";
    const includePackaging = (req.method === "POST" ? req.body?.includePackaging : req.query.includePackaging);
    const includePackagingBool = includePackaging === true || includePackaging === "true";

    let prodRef;
    let pData;

    // --- 1b. Duplicate Check ---
    if (product_name) {
      logger.info(`[cf11] Performing duplicate check for '${product_name}'`);

      // 1. Preprocessing
      const normalize = (str) => str.toLowerCase().replace(/.*/, '').split(/.*/).filter(Boolean);
      const inputWords = new Set(normalize(product_name));

      // 2. Firestore Query
      const candidatesSnap = await db.collection("products_new")
        .where("tu_id", "==", userId)
        .where("..._done", "==", false)
        .where("...", "==", false)
        .where("ef_pn", "==", false)
        .get();

      // 3. Local Filtering & Prompt Construction
      const potentialMatches = [];
      candidatesSnap.forEach(doc => {
        const data = doc.data();
        const docName = data.name || "";
        const docWords = normalize(docName);

        // Check for at least one matching word (basic filter to reduce prompt size)
        const hasOverlap = docWords.some(word => inputWords.has(word));
        if (hasOverlap) {
          potentialMatches.push(data);
        }
      });

      if (potentialMatches.length > 0) {
        let candidatesPrompt = "...";
        potentialMatches.forEach((p, index) => {
          candidatesPrompt += `*product_${index + 1}: ${p.name}\n`;
        });

        const SYS_MSG_DUPLICATE_NEW = "..."

        const vGenerationConfigDuplicate = {
//
//
//
        };

        try {
          const { answer: duplicateResponse } = await runOpenModelStream({
            model: 'openai/gpt-oss-120b-maas',
            generationConfig: vGenerationConfigDuplicate,
            user: candidatesPrompt,
          });

          if (duplicateResponse.trim() !== "Done") {
            const productNameMatch = duplicateResponse.match( /.*/);
            if (productNameMatch && productNameMatch[1]) {
              const matchedName = productNameMatch[1].trim();
              logger.info(`[cf11] Duplicate found: '${matchedName}'.Duplicating record.`);

              // Find the exact document from our pre-fetched candidates (avoiding re-query)
              // Note: potentialMatches are data objects. We need the doc ID or reference. 
              // Let's re-find it in the snapshot for safety to get the Ref.
              const matchingDoc = candidatesSnap.docs.find(d => d.data().name === matchedName);

              if (matchingDoc) {
                const epDocRef = matchingDoc.ref;
                const epData = matchingDoc.data();

                // Double check flags (though query ensured them, good for robustness)
                if (epData.apcfInitial2_done === false && epData.apcfMPCFFullNew_started === false) {
                  const newProdRef = db.collection("products_new").doc();

                  epData.id = newProdRef.id; // Ensure new ID is used
                  epData.tu_id = userId;
                  epData.createdAt = admin.firestore.FieldValue.serverTimestamp();
                  epData.updatedAt = admin.firestore.FieldValue.serverTimestamp();

                  await newProdRef.set(epData);

                  const subColls = ['pn_data', 'pn_reasoning', 'pn_uncertainty'];
                  for (const coll of subColls) {
                    const subSnap = await epDocRef.collection(coll).get();
                    for (const subDoc of subSnap.docs) {
                      await newProdRef.collection(coll).doc(subDoc.id).set(subDoc.data());
                    }
                  }

                  logger.info(`[cf11] Successfully duplicated product ${ matchingDoc.id } to ${ newProdRef.id } for user ${ userId }`);
                  res.json("Done");
                  return;
                }
              } else {
                logger.warn(`[cf11] AI matched name '${matchedName}' but could not map back to source doc.`);
              }
            }
          }

        } catch (err) {
          logger.error(`[cf11] Error during duplicate check AI call: ${ err.message } `);
        }
      } else {
        logger.info(`[cf11] check: No candidates found with word overlap.`);
      }
    }

    // --- 2. Workflow Selection: Create New Product vs. Initialize Existing ---
    if (productId) {
      // --- INITIALIZATION WORKFLOW ---
      logger.info(`[cf11] Initializing existing product with ID: ${ productId } `);
      prodRef = db.collection("products_new").doc(productId);
      const pSnap = await prodRef.get();
      if (!pSnap.exists) {
        res.status(404).json({ error: `Product with ID ${ productId } not found.` });
        return;
      }

      pData = pSnap.data() || {};

      // --- NEW: AI Step: Generate Emissions Factor Tags ---
      if (pData.ef_pn === true) {
        logger.info(`[cf11] ef_pn is true and no tags exist for ${ productId }.Generating tags.`);

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

        // Construct the prompt with current tags, if they exist
        const currentTags = pData.eai_ef_tags || [];
        let userPromptForTags;

        if (currentTags.length > 0) {
          const tagsString = currentTags.join('\n');
          userPromptForTags = `Product or Activity: ${ pData.name } \n\nCurrent Tags: \n${ tagsString } `;
        } else {
          userPromptForTags = `Product or Activity: ${ pData.name } `;
        }

        const { answer: tagsResponse, ...tagsAiResults } = await runGeminiStream({
          model: 'aiModel', //flash
          generationConfig: vGenerationConfigTags,
          user: userPromptForTags,
        });

        // Log the AI call
        await logAITransaction({
          cfName: 'cf11-TagGeneration',
          productId: prodRef.id,
          cost: tagsAiResults.cost,
          totalTokens: tagsAiResults.totalTokens,
          searchQueries: tagsAiResults.searchQueries,
          modelUsed: tagsAiResults.model
        });

        await logAIReasoning({
          sys: TAG_GENERATION_SYS,
          user: userPromptForTags,
          thoughts: tagsAiResults.thoughts,
          answer: tagsResponse,
          cloudfunction: 'cf11-TagGeneration',
          productId: prodRef.id
        });

        // Parse and save the tags
        const tags = tagsResponse.split('\n')
          .map(line => line.match( /.*/))
          .filter(Boolean)
          .map(match => match[1].trim());

        if (tags.length > 0) {
          logger.info(`[cf11] Found ${ tags.length } tags to add.`);
          await prodRef.update({
            eai_ef_tags: admin.firestore.FieldValue.arrayUnion(...tags)
          });
        } else {
          logger.warn(`[cf11] Tag generation AI did not return any parsable tags.`);
        }
        // ADDED: Else block to log when tag generation is skipped
      } else if (pData.ef_pn === true) {
        logger.info(`[cf11] Skipping tag generation for ${ productId } as 'eai_ef_tags' already contains data.`);
      }

      let orgRef = null;
      if (orgId) {
        orgRef = db.collection("organisations").doc(orgId.trim());
        if (!(await orgRef.get()).exists) {
          res.status(400).json({ error: `Organisation ${ orgId } not found` });
          return;
        }
      }

      const updatePayload = {
        organisation: orgRef,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        data_sources: [],
        status: "In-Progress",
        z_ai_history: "",
        ai_loop: 0,
        estimated_cf: 0,
        current_tier: 1,
        total_cf: 0,
        transport_cf: 0,
        apcfIMaterialsNum: 0,
        apcfMPCF_done: false,
        supplier_cf_found: false,
        apcfBOM_done: false,
        ecozeAI_Pro: ecozeAIProBool,
        ecozeAI_lite: false,
        apcfMFSF_done: false,
        apcfMSF_done: false,
        apcfMM_done: false,
        apcfMaterials_done: false,
        apcfMassReview_done: false,
        apcfCFReview_done: false,
        rcOn: false,
        apcfMPCFFullNew_done: false,
        apcfMPCFFullNew_started: false,
        apcfMPCFFullDR_started: false,
        apcfMPCFFullDR_done: false,
        cf_calculation_initial: false,
        cf_calculation_second: false,
        apcfInitial_done: false,
        apcfInitial2_done: false,
        apcfSupplierAddress_done: false,
        apcfSupplierFinderDR_started: false,
        apcfSupplierFinder_done: false,
        apcfTransportCF_done: false,
        apcfProductTotalMass_done: false,
        apcfSupplierDisclosedCF_done: false,
        apcfInitial2_paused: false,
        child_materials_progress: [
          { cloudfunction: 'cf11', number_done: 0 },
          { cloudfunction: 'cf11', number_done: 0 },
          { cloudfunction: 'cf11', number_done: 0 },
          { cloudfunction: 'cf11', number_done: 0 },
          { cloudfunction: 'cf11', number_done: 0 }
        ]
      };

      if (userId) {
        updatePayload.tu_id = userId;
      }

      if (pData.includePackaging === undefined) {
        updatePayload.includePackaging = includePackagingBool;
      }

      if (otherMetricsBool) {
        updatePayload.otherMetrics = true;
      }

      if (!pData.eai_ef_docs) {
        updatePayload.eai_ef_docs = [];
      }

      await prodRef.update(updatePayload);
      pData = (await prodRef.get()).data();

    } else if (product_name) {
      // --- CREATION WORKFLOW ---
      logger.info(`[cf11] Creating new product: ${ product_name } `);
      let orgRef = null;
      if (orgId) {
        orgRef = db.collection("organisations").doc(orgId.trim());
        if (!(await orgRef.get()).exists) {
          res.status(400).json({ error: `Organisation ${ orgId } not found` });
          return;
        }
      }

      prodRef = db.collection("products_new").doc();
      const createPayload = {
        name: product_name.trim(),
        organisation: orgRef,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        data_sources: [],
        eai_ef_docs: [],
        status: "In-Progress",
        z_ai_history: "",
        ai_loop: 0,
        estimated_cf: 0,
        current_tier: 1,
        total_cf: 0,
        transport_cf: 0,
        apcfIMaterialsNum: 0,
        apcfMPCF_done: false,
        supplier_cf_found: false,
        in_collection: false,
        ecozeAI_Pro: ecozeAIProBool,
        ecozeAI_lite: false,
        ef_pn: false,
//...
        child_materials_progress: [
          { cloudfunction: 'cf11', number_done: 0 },
          { cloudfunction: 'cf11', number_done: 0 },
          { cloudfunction: 'cf11', number_done: 0 },
          { cloudfunction: 'cf11', number_done: 0 },
          { cloudfunction: 'cf11', number_done: 0 }
        ]
      };

      if (userId) {
        createPayload.tu_id = userId;
      }

      if (otherMetricsBool) {
        createPayload.otherMetrics = true;
      }

      createPayload.includePackaging = includePackagingBool;

      await prodRef.set(createPayload);
      pData = createPayload; // The payload is the data for a new doc

    } else {
      res.status(400).json({ error: "Missing required argument: please provide either 'productId' or 'product_name'." });
      return;
    }

    // --- 3. Research & Fact-Check Product Description ---
    logger.info(`[cf11] Triggering productDescription for product ${ prodRef.id }`);
    await productDescription({ productId: prodRef.id });

    let isSpecialCase = false;

    if (productId) {
      if (pData.ef_pn !== true) {
        // --- 4. AI Step: Product Name Enhancement (only for existing products) ---
        const SYS_MSG_ENHANCE = "..."

        const pDataForEnhance = (await prodRef.get()).data(); // Get latest data

        const vGenerationConfigEnhance = {
//
//
//
//
//
        };

        const { answer: enhanceResponse, ...enhanceAiResults } = await runGeminiStream({
          model: 'aiModel', //flash
          generationConfig: vGenerationConfigEnhance,
          user: pDataForEnhance.name,
        });

        await logAITransaction({ cfName: 'cf11-EnhanceName', productId: prodRef.id, cost: enhanceAiResults.cost, totalTokens: enhanceAiResults.totalTokens, searchQueries: enhanceAiResults.searchQueries, modelUsed: enhanceAiResults.model });
        await logAIReasoning({ sys: SYS_MSG_ENHANCE, user: pDataForEnhance.name, thoughts: enhanceAiResults.thoughts, answer: enhanceResponse, cloudfunction: 'cf11-EnhanceName', productId: prodRef.id });

        const responseText = enhanceResponse.trim();
        const newNameMatch = responseText.match(/.*/);
        const identifiedMatch = responseText.match(/.*/);

        if (newNameMatch && newNameMatch[1]) {
          const newProductName = newNameMatch[1].trim();
          logger.info(`[cf11] Enhancing product name from '${pDataForEnhance.name}' to '${newProductName}'`);
          await prodRef.update({ name: newProductName });
          isSpecialCase = false;
        } else if (identifiedMatch && identifiedMatch[1]) {
          const identification = identifiedMatch[1].trim().toLowerCase();
          switch (identification) {
            case 'activity':
              logger.info(`[cf11] Detected an activity for product ${prodRef.id}.`);
              isSpecialCase = true;
              await prodRef.update({ activityNotProduct: true });
              await callCF("cf16", { productId: prodRef.id });
              break;
            case 'generic':
              logger.info(`[cf11] Detected a generic product for ${prodRef.id}.`);
              isSpecialCase = true;
              await prodRef.update({ generic_product: true });
              await callCF("cf18", { productId: prodRef.id });
              break;
            case 'done':
              logger.info(`[cf11] Product name is already specific. No enhancement needed.`);
              isSpecialCase = false;
              break;
            default:
              logger.warn(`[cf11] Unhandled identification type: "${identification}"`);
              isSpecialCase = false;
              break;
          }
        } else {
          logger.warn(`[cf11] EnhanceName AI response was not in a recognized format: "${responseText}"`);
          isSpecialCase = false;
        }
      } else {
        logger.info(`[cf11] Skipping duplicate check and name enhancement because 'ef_pn' is true.`);
      }
    }

    // --- 5. Call Helper Cloud Functions ---
    if (!isSpecialCase) {
      // If a product ID was given and it already has EF documents linked, skip these steps.
      if (productId && pData.eai_ef_docs && Array.isArray(pData.eai_ef_docs) && pData.eai_ef_docs.length > 0) {
        logger.info(`[cf11] Skipping helper functions for product ${prodRef.id} as 'eai_ef_docs' is already populated.`);
      } else {
        logger.info(`[cf11] Starting standard helper function calls for product ${prodRef.id}`);
        await callCF("cf33", { productId: prodRef.id });
        await callCF("cf42", { productId: prodRef.id });
        logger.info("[cf11] SupplierDisclosedCF and ProductTotalMass finished");

        logger.info("[cf11] Waiting 5 seconds for SDCF to complete...");
        await sleep(5000); // 5-second delay to allow SDCF to write to Firestore

        const latestPData = (await prodRef.get()).data() || {};

        const shouldRunMpcf = latestPData.ef_pn !== true && !latestPData.supplier_cf_found;

        if (shouldRunMpcf) {
          logger.info(`[cf11] ecozeAIPro is FALSE and Supplier CF Not Found. Conditions met. Calling cf15 for product ${prodRef.id}`);
          await callCF("cf15", { productId: prodRef.id });
        } else {
          logger.info(`[cf11] Skipping cf15 because 'ef_pn' is true OR 'supplier_cf_found' is true.`);
        }
      }
    }

    logger.info(`[cf11] Resetting estimated_cf to 0 for product ${prodRef.id}`);
    await prodRef.update({ estimated_cf: 0 });

    // --- 6. Aggregate Uncertainty & Finalize ---
    /*
    const uncertaintySnap = await prodRef.collection("pn_uncertainty").get();
    let uSum = 0;
 
    if (!uncertaintySnap.empty) {
      uncertaintySnap.forEach(doc => {
        const uncertaintyValue = doc.data().co2e_uncertainty_kgco2e;
        if (typeof uncertaintyValue === 'number' && isFinite(uncertaintyValue)) {
          uSum += uncertaintyValue;
        }
      });
    }
    logger.info(`[cf11] Calculated total uncertainty for product ${prodRef.id}: ${uSum}`);
    */



    // Combine final updates
    const finalUpdatePayload = {
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      status: "Done",
      apcfInitial_done: true,
      //total_uncertainty: uSum, // Add the calculated sum
    };

    // Conditionally aggregate other metrics
    const finalProductData = (await prodRef.get()).data() || {};
    if (finalProductData.otherMetrics === true) {
      logger.info(`[cf11] otherMetrics flag is true for ${prodRef.id}. Aggregating totals.`);
      const metricsSnap = await prodRef.collection("pn_otherMetrics").get();

      const totals = { ap_total: 0, ep_total: 0, adpe_total: 0, gwp_f_total: 0, gwp_b_total: 0, gwp_l_total: 0 };
      const fieldsToSum = [
        { from: 'ap_value', to: 'ap_total' }, { from: 'ep_value', to: 'ep_total' },
        { from: 'adpe_value', to: 'adpe_total' }, { from: 'gwp_f_value', to: 'gwp_f_total' },
        { from: 'gwp_b_value', to: 'gwp_b_total' }, { from: 'gwp_l_value', to: 'gwp_l_total' },
      ];

      if (!metricsSnap.empty) {
        metricsSnap.forEach(doc => {
          const data = doc.data();
          fieldsToSum.forEach(field => {
            if (typeof data[field.from] === 'number' && isFinite(data[field.from])) {
              totals[field.to] += data[field.from];
            }
          });
        });
      }
      logger.info(`[cf11] Calculated totals for ${prodRef.id}:`, totals);
      Object.assign(finalUpdatePayload, totals);
    }

    await prodRef.update(finalUpdatePayload);
    logger.info(`[cf11] Process completed for product ${prodRef.id}`);
    res.json({ status: "ok", docId: prodRef.id });

  } catch (err) {
    logger.error("[cf11] Uncaught error:", err);
    res.status(500).json({ error: String(err) });
  }
});