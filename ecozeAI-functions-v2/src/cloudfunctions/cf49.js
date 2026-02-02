//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf49 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,      // same 60-min budget
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {
  /* ╭─────────────────────────────── 0. Input validation ───────────────────────────╮ */
  const materialId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || null;
  const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || null;
  try {
    let entityType = productId ? 'product' : 'material';
    let linkedProductId = null;
    console.log("[DBG-01] Raw args →", { materialId, productId });

    if ((materialId && productId) || (!materialId && !productId)) {
      res.status(400).json({ error: "Provide exactly one of materialId OR productId" });
      return;
    }

    const collectedUrls = new Set();              // ⬅️ data sources accumulator

    /* ╭─────────────────────────────── 1. Fetch core docs ────────────────────────────╮ */
    let productRef = null;      // /products_new/{…}   (always populated for URL push)
    let contextDocRef = null;      // destination for new transport sub-docs
    let subcollection = "";        // "pn_transport" | "materials_transport"
    let productName = "";
    let companyStart = "", addressStart = "";
    let companyFinal = "", addressFinal = "";
    let productMass = null, massUnit = "Unknown";     // for CF calc
    let productChain = "";

    /* ——— CASE 1: top-level product-only input ——————————————— */
    if (productId) {
      const pSnap = await db.collection("products_new").doc(productId).get();
      if (!pSnap.exists) {
        res.status(404).json({ error: `Product ${productId} not found` });
        return;
      }
      const pData = pSnap.data() || {};
      productRef = pSnap.ref;
      contextDocRef = pSnap.ref;
      subcollection = "pn_transport";

      productName = (pData.name || "").trim();
      companyStart = (pData.manufacturer_name || "").trim();
      addressStart = (pData.supplier_address || "").trim();
      if (!addressStart || addressStart === "Unknown") {
        addressStart = (pData.country_of_origin || "").trim();
      }
      productMass = pData.mass ?? null;
      massUnit = (pData.mass_unit || "Unknown").trim();

      /* pull organisation → address/ name (company B) */
      const orgRef = pData.organisation || null;
      if (!orgRef) {
        res.status(400).json({ error: "Product has no organisation reference" });
        return;
      }
      const orgSnap = await orgRef.get();
      const orgData = orgSnap.data() || {};
      companyFinal = (orgData.name || "").trim();
      addressFinal = (orgData.address || "").trim();

      /* ——— CASE 2 / 3: material-level input ———————————————— */
    } else {
      const mSnap = await db.collection("materials").doc(materialId).get();
      if (!mSnap.exists) {
        res.status(404).json({ error: `Material ${materialId} not found` });
        return;
      }
      const mData = mSnap.data() || {};
      linkedProductId =
        mData.linked_product && mData.linked_product.id
          ? mData.linked_product.id
          : null;
      contextDocRef = mSnap.ref;
      subcollection = "materials_transport";

      productName = (mData.name || "").trim();
      companyStart = (mData.supplier_name || "").trim();
      addressStart = (mData.supplier_address || "").trim();
      if (!addressStart || addressStart === "Unknown") {
        addressStart = (mData.country_of_origin || "").trim();
      }
      productMass = mData.mass ?? null;
      massUnit = (mData.mass_unit || "Unknown").trim();
      /* build product-chain string */
      productChain = mData.product_chain || '(unknown chain)';

      /* CASE 2 → no parent_material → use linked_product */
      if (!mData.parent_material) {
        const p2Ref = mData.linked_product;
        const p2Snap = await p2Ref.get();
        const p2Data = p2Snap.data() || {};

        productRef = p2Ref;                          // for URL pushes
        companyFinal = (p2Data.manufacturer_name || "").trim();
        addressFinal = (p2Data.supplier_address || "").trim();
        if (!addressFinal || addressFinal === "Unknown") {
          addressFinal = (p2Data.country_of_origin || "").trim();
        }

        /* CASE 3 → parent_material exists */
      } else {
        const mpSnap = await mData.parent_material.get();
        const mpData = mpSnap.data() || {};

        productRef = mData.linked_product || null;   // may be null, fine
        companyFinal = (mpData.supplier_name || "").trim();
        addressFinal = (mpData.supplier_address || "").trim();
        if (!addressFinal || addressFinal === "Unknown") {
          addressFinal = (mpData.country_of_origin || "").trim();
        }
      }
    }

    /* ╭─────────────────────────────── 2. AI - legs discovery ───────────────────────╮ */

    // ADD these lines to define the standard AI configuration

    const SYS_LEG = "..."

    const USER_LEG =
      `...`;

    function parseLegs(text) {
      const clean = text.replace( /.*/, '*$1:');
      const PAT = /.*/;

      const out = [];
      let m;
      while ((m = PAT.exec(clean)) !== null) {
        const raw = m[4].replace( /.*/, "");
        const km = parseFloat(raw);
        out.push({
          leg: Number(m[1]),
          transport_method: m[2].trim() || "Unknown",
          country: m[3].trim(),
          distance_km: isFinite(km) ? km : null,
          transport_company: m[5].trim(),
          estimated_transport_method: /TRUE/i.test(m[6]),
          estimated_leg: /TRUE/i.test(m[7])
        });
      }
      return out;
    }

    const modelForLegs = 'aiModel'; //pro
    const vGenerationConfig = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 32768 // Correct budget for pro model
      },
    };

    // NEW: Get the pre-calculated cost and totalTokens object from the helper
    const { answer: assistant, thoughts, cost, totalTokens, searchQueries: legSearchQueries, model: legModel, rawConversation: rawConversation1 } = await runGeminiStream({
      model: modelForLegs,
      generationConfig: vGenerationConfig,
      user: USER_LEG,
      collectedUrls
    });

    // NEW: Call the new, simpler logger with pre-calculated values
    await logAITransaction({
      cfName: 'cf49',
      productId: entityType === 'product' ? productId : linkedProductId,
      materialId: materialId,
      cost,
      totalTokens,
      searchQueries: legSearchQueries,
      modelUsed: legModel
    });

    await logAIReasoning({
      sys: SYS_LEG,
      user: USER_LEG,
      thoughts: thoughts,
      answer: assistant,
      cloudfunction: 'cf49',
      productId: productId,
      materialId: materialId,
      rawConversation: rawConversation1,
    });


    // 2️⃣  Bail out early if Gemini found nothing useful.
    if (!assistant || /^Unknown$/i.test(assistant)) {
      logger.warn("[cf49] Gemini returned no legs - exiting early");
      await contextDocRef.update({ apcfTransportCF_done: true });
      res.json("Done");
      return;
    }

    // 3️⃣  Parse the assistant text into leg objects.
    const legs = parseLegs(assistant);
    if (!legs.length) {
      logger.warn("[cf49] parseLegs() found 0 matches - exiting");
      await contextDocRef.update({ apcfTransportCF_done: true });
      res.json("Done");
      return;
    }

    // 4️⃣  Persist the legs exactly as before.
    const batch = db.batch();
    for (const L of legs) {
      const docRef = contextDocRef.collection(subcollection).doc();
      batch.set(docRef, {
        leg: L.leg,
        transport_method: L.transport_method,
        country: L.country,
        distance_km: L.distance_km,
        estimated_transport_method: L.estimated_transport_method,
        transport_company: L.transport_company,
        estimated_leg: L.estimated_leg,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp()
      });
    }
    await batch.commit();
    logger.info(`[cf49] 📄 committed ${ legs.length } leg doc(s)`);

    // 5️⃣  Store citation URLs just like before.
    if (collectedUrls.size) {
      if (materialId) {
        await saveURLs({
          urls: Array.from(collectedUrls),
          materialId,
          productId: linkedProductId,
          mTransportData: true,
          sys: SYS_LEG,
          user: USER_LEG,
          thoughts: thoughts,
          answer: assistant,
          cloudfunction: 'cf49',
        });
      } else {
        await saveURLs({
          urls: Array.from(collectedUrls),
          productId,
          pTransportData: true,
          sys: SYS_LEG,
          user: USER_LEG,
          thoughts: thoughts,
          answer: assistant,
          cloudfunction: 'cf49',
        });
      }
    }

    logger.info("[cf49] ✅ Legs discovery complete.");

    /* ╭────────────────────────────── 3. Fetch persisted leg docs ───────────────────╮ */
    const legsSnap = await contextDocRef.collection(subcollection)
      .orderBy("leg")
      .get();

    /* ╭────────────────────────────── 4. Per-leg emissions loop ─────────────────────╮ */

    for (const legDoc of legsSnap.docs) {
      const L = legDoc.data();

      /* 4.4  ── Gemini prompt craft ******************************************* */
      const SYS_CF = "...";

logger.debug(`[cf49] 💬🧮 USER_CF for leg ${L.leg}:\n${USER_CF}`);

const modelForEmissions = 'aiModel'; //flash
const vGenerationConfig2 = {
//
//
//
//
  // Add the thinkingConfig object here
//
    includeThoughts: true,    // Set to true to request thinking process
    thinkingBudget: 24576     // The token budget for the model to "think"
  },
};

// NEW: Get the pre-calculated cost and totalTokens object
const { answer: cfAssistant, thoughts: cfThoughts, cost, totalTokens, searchQueries: cfSearchQueries, model: cfModel, rawConversation: rawConversation2 } = await runGeminiStream({
  model: modelForEmissions,
  generationConfig: vGenerationConfig2,
  user: USER_CF,
  collectedUrls
});

// NEW: Call the new, simpler logger
await logAITransaction({
  cfName: 'cf49',
  productId: entityType === 'product' ? productId : linkedProductId,
  materialId: materialId,
  cost,
  totalTokens,
  searchQueries: cfSearchQueries,
  modelUsed: cfModel
});

await logAIReasoning({
  sys: SYS_CF,
  user: USER_CF,
  thoughts: cfThoughts,
  answer: cfAssistant,
  cloudfunction: 'cf49',
  productId: productId,
  materialId: materialId,
  rawConversation: rawConversation2,
});

/* 4.4b ── extract *cf_value* ********************************************* */
let cfValue = parseCfValue(cfAssistant);
if (cfValue === null) {
  logger.warn(
    `[cf49]🧮 Leg ${L.leg}: Gemini did not yield a numeric cf_value\n` +
    `└─ Assistant said:\n${cfAssistant}`
  );
} else {
  logger.info(`[cf49]✅🧮 Leg ${L.leg}: cf_value = ${cfValue}`);
}

/* 4.5  ── persist the result + EF refs *********************************** */
const update = {
  emissions_kgco2e: cfValue ?? null,
};
await legDoc.ref.update(update);
logger.info(
  `[cf49] 🖊️ leg ${L.leg} updated (cf=${cfValue ?? "null"})`
);
    }

/* ╭────────────────── 5. Final Aggregation and Update ─────────────────────╮ */
logger.info(`[cf49] Starting final aggregation for ${productId ? 'product' : 'material'} ${productId || materialId}`);

// Re-fetch all leg documents from the subcollection to ensure we have the latest data
const allLegsSnapshot = await contextDocRef.collection(subcollection).get();

// Sum the emissions from all leg documents
let totalTransportCF = 0;
allLegsSnapshot.forEach(doc => {
  const legEmissions = doc.data().emissions_kgco2e;
  // Ensure we only add valid numbers to the sum
  if (typeof legEmissions === 'number' && isFinite(legEmissions)) {
    totalTransportCF += legEmissions;
  }
});

logger.info(`[cf49] Total transport emissions calculated: ${totalTransportCF} kgCO2e`);

// Use a transaction to safely update the documents
await db.runTransaction(async (transaction) => {
  if (productId) {
    // --- CASE 1: Function was called with a productId (No changes here) ---
    const pDocRef = db.collection("products_new").doc(productId);
    transaction.update(pDocRef, {
      transport_cf: totalTransportCF,
      estimated_cf: admin.firestore.FieldValue.increment(totalTransportCF)
    });
    logger.info(`[cf49] Aggregated transport_cf to product ${productId}.`);

  } else { // materialId must be present
    // --- CASE 2: Function was called with a materialId (NEW LOGIC) ---
    const mDocRef = db.collection("materials").doc(materialId);

    // 1. READ FIRST: Get the document so you can access its pmChain.
    const mDocSnap = await transaction.get(mDocRef);
    const mDocData = mDocSnap.data() || {};
    const pmChain = mDocData.pmChain;

    // 2. NOW WRITE: Update the current material document.
    transaction.update(mDocRef, {
      transport_cf: totalTransportCF,
      estimated_cf: admin.firestore.FieldValue.increment(totalTransportCF)
    });
    logger.info(`[cf49] Queued update for current material ${materialId}.`);

    // 3. WRITE AGAIN: Iterate through the chain and update all parent documents.
    if (Array.isArray(pmChain) && pmChain.length > 0) {
      logger.info(`[cf49] Found pmChain with ${pmChain.length} items. Propagating updates.`);
      for (const chainItem of pmChain) {
        if (chainItem.material_or_product === "Product") {
          const productDocRef = db.collection("products_new").doc(chainItem.documentId);
          transaction.update(productDocRef, {
            estimated_cf: admin.firestore.FieldValue.increment(totalTransportCF)
          });
          logger.info(`[cf49] Queued update for parent product ${chainItem.documentId}.`);
        } else if (chainItem.material_or_product === "Material") {
          const materialDocRef = db.collection("materials").doc(chainItem.documentId);
          transaction.update(materialDocRef, {
            estimated_cf: admin.firestore.FieldValue.increment(totalTransportCF)
          });
          logger.info(`[cf49] Queued update for parent material ${chainItem.documentId}.`);
        }
      }
    } else {
      logger.warn(`[cf49] No pmChain found for material ${materialId}. Only updating the material itself.`);
    }
  }
});

logger.info(`[cf49] All database updates committed successfully.`);

logger.info(`[cf49] Triggering uncertainty calculation...`);
/*
 
if (productId) {
  await callCF("cf50", {
    productId: productId,
    calculationLabel: "cf49"
  });
  logger.info(`[cf49] Completed uncertainty calculation for product ${productId}.`);
} else if (materialId) {
  await callCF("cf50", {
    materialId: materialId,
    calculationLabel: "cf49"
  });
  logger.info(`[cf49] Completed uncertainty calculation for material ${materialId}.`);
}
  */

/******************** 6. Trigger Other Metrics Calculation (Conditional) ********************/
logger.info(`[cf49] Checking if other metrics calculation is needed...`);
if (productId) {
  const pSnap = await productRef.get(); // productRef was defined at the start
  const pData = pSnap.data() || {};
  if (pData.otherMetrics === true) {
    logger.info(`[cf49] otherMetrics flag is true for product ${productId}. Triggering calculation.`);
    await callCF("cf30", {
      productId: productId,
      calculationLabel: "cf49"
    });
  }
} else if (materialId) {
  // productRef was set to the linked_product earlier in the function
  if (productRef) {
    const mpSnap = await productRef.get();
    if (mpSnap.exists) {
      const mpData = mpSnap.data() || {};
      if (mpData.otherMetrics === true) {
        logger.info(`[cf49] otherMetrics flag is true for linked product ${productRef.id}. Triggering calculation for material ${materialId}.`);
        await callCF("cf30", {
          materialId: materialId,
          calculationLabel: "cf49"
        });
      }
    }
  } else {
    logger.warn(`[cf49] No linked product found for material ${materialId}, skipping other metrics calculation.`);
  }
}

await incrementChildProgress(materialId, 'cf49');
await contextDocRef.update({
  apcfTransportCF_done: true
});
res.json("Done");

  } catch (err) {
  console.error("[cf49] Uncaught error:", err);
  await incrementChildProgress(materialId, 'cf49');
  res.status(500).json({ error: String(err) });
}
});

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------
/****************************************************************************************
 * cf10  - v2  (o3 + searchExistingEmissionsFactors + verbose logs)
 ****************************************************************************************/