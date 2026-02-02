//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf10 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT,
  memory: MEM,
  secrets: SECRETS,
}, async (req, res) => {

  /* wrap everything so we can always respond 200, even on error */
  try {
    /* ── 0. input ─────────────────────────────────────────────────────── */
    const aName =
      (req.method === "POST" ? req.body?.aName : req.query.aName) || "";
    if (!aName.trim()) {
      res.status(400).json({ error: "Missing aName" });
      return;
    }

    const materialId = (req.method === "POST" ? req.body?.materialId : req.query.materialId) || null;
    const productId = (req.method === "POST" ? req.body?.productId : req.query.productId) || null;

    let linkedProductId = null;

    let targetRef = null;
    if (productId) {
      targetRef = db.collection("products_new").doc(productId);
    } else if (materialId) {
      targetRef = db.collection("materials").doc(materialId);
    }

    /* ── 1. system & user messages (UNCHANGED) ────────────────────────── */
    const SYS =
      `...`;

    let history = [
      { role: "system", content: SYS },
      { role: "user", content: aName }
    ];

    const allDocIds = [];            // collect every ID we see
    const EXIST_RE = /.*/;
    const NEW_RE = /.*/;

    /* ── 2. call Gemini 2.5-pro one-shot ─────────────────────────────── */
    const collectedUrls = new Set();

    // (2-A) Call the NEW AI-powered function to search the database.
    const { response: existingFactorsResponse, model: searchModel } =
      await searchExistingEmissionsFactorsWithAI({ query: aName, productId, materialId });

    // Extract only the relevant part for the next prompt.
    // The AI is prompted to return '[Relevant Emissions Factors]' block, or 'Unknown'.
    const existingBlock = existingFactorsResponse.includes("[Relevant Emissions Factors]")
      ? existingFactorsResponse
      : "[Relevant Emissions Factors]\n*None found*";

    const USER_PROMPT = "..." +
      `[New Emissions Factors]\n` +
      `*(fill as per spec above)*`;

    const modelUsed = 'aiModel'; //flash
    const vGenerationConfig = {
//
//
//
//
//
        includeThoughts: true,
        thinkingBudget: 24576 // Correct budget for the pro model
      },
    };

    // NEW: Get the pre-calculated cost and totalTokens object from the helper
    const { answer: assistant, thoughts, cost, totalTokens, searchQueries, model: newFactorsModel } = await runGeminiStream({
      model: modelUsed,
      generationConfig: vGenerationConfig,
      sys: SYS,
      user: USER_PROMPT,
      collectedUrls,
    });

    // NEW: Call the new, simpler logger, but only if there's a doc to log to
    if (targetRef) {
      const targetData = (await targetRef.get()).data() || {};
      linkedProductId = materialId ? targetData.linked_product?.id : null;
      await logAITransaction({
        cfName: 'cf10',
        productId: productId || linkedProductId,
        materialId: materialId,
        cost,
        totalTokens, // Pass the single token object for this call
        searchQueries: searchQueries,
        modelUsed: newFactorsModel,
      });

      await logAIReasoning({
        sys: SYS,
        user: USER_PROMPT,
        thoughts: thoughts,
        answer: assistant,
        cloudfunction: 'cf10',
        productId: productId,
        materialId: materialId,
      });
    }

    /* Guard-rail: if Gemini returns nothing or just “Unknown” we bail. */
    if (!assistant || /^Unknown$/i.test(assistant.trim())) {
      res.json({ efDocs: [] });
      return;
    }


    /********************************************************************
     * 1️⃣  Gather every *documentId_N: line (existing EF references)   *
     ********************************************************************/
    let m;
    while ((m = EXIST_RE.exec(assistant)) !== null) {
      const id = m[2].trim();
      if (id) allDocIds.push(id);
    }

    /********************************************************************
    * 2️⃣  Parse & STORE the [New Emissions Factors] block             *
    ********************************************************************/

    while ((m = NEW_RE.exec(assistant)) !== null) {
      const name = m[2].trim();
      const value = parseFloat(m[3].replace( /.*/, "").trim());
      const value_unit = m[4].trim();
      const provider = m[5].trim();
      const conversion = m[6].trim();
      const yearString = m[7].trim();
      const yearForCheck = parseInt(yearString, 10);
      const description = m[8].trim();
      const url = m[9].trim();

      /* skip if key fields are missing or the EF is older than 2016 */
      if (!name || !Number.isFinite(value) || !Number.isFinite(yearForCheck) || yearForCheck < 2016) continue;

      const newData = {
        name,
        value,
        value_unit,
        provider,
        conversion,
        year: yearString,
        description, // <-- Saves the new description field
        url,
        source_activity: aName,
        vertexAISearchable: false,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      };

      const docRef = await db.collection("emissions_factors").add(newData);
      console.log(`[cf10] ➕ stored new EF “${name}” ⇒ ${docRef.id}`);
      allDocIds.push(docRef.id);      // treat it exactly like an existing factor
    }

    /* ── 3. de-dupe, existence-check, log & return  ─────────────────── */
    console.log(
      "[cf10] 📝 Raw EF ID list before filtering:",
      JSON.stringify(allDocIds)
    );

    const unique = Array.from(new Set(allDocIds));
    const valid = [];
    for (const id of unique) {
      try {
        const snap = await db.collection("emissions_factors").doc(id).get();
        if (snap.exists) valid.push(id);
      } catch (e) {
        console.warn(`[cf10] ⚠️ ID check failed for "${id}":`, e);
      }
    }

    if (valid.length === 0) {
      console.warn("[cf10] No valid EF IDs after filtering");
    }
    console.log("[cf10] ✅ valid EF IDs:", JSON.stringify(valid));

    console.log(
      "[cf10] 🏁 FINAL conversation history:\n" + pretty(history)
    );

    // --- NEW: Save valid emissions factor references back to the source document ---
    if (targetRef && valid.length > 0) {
      try {
        // Convert the array of string IDs into an array of DocumentReference objects
        const efRefs = valid.map(id => db.collection("emissions_factors").doc(id));

        logger.info(`[cf10] Saving ${efRefs.length} emissions factor reference(s) to ${targetRef.path}.`);

        // Use arrayUnion to add the references without creating duplicates
        await targetRef.update({
          ecf_efs_used: admin.firestore.FieldValue.arrayUnion(...efRefs)
        });

        logger.info(`[cf10] Successfully updated ${targetRef.path} with emissions factor references.`);

      } catch (err) {
        // Log the error but don't stop the function. Its main job is to return text.
        logger.error(`[cf10] Failed to save EF references to ${targetRef.path}:`, err);
      }
    }

    let cleanedResponse = "";
    const relevantStartIndex = assistant.indexOf('[Relevant Emissions Factors]');
    const newStartIndex = assistant.indexOf('[New Emissions Factors]');

    // If the [Relevant Emissions Factors] block exists, grab it.
    if (relevantStartIndex !== -1) {
      const relevantEndIndex = (newStartIndex !== -1) ? newStartIndex : assistant.length;
      cleanedResponse += assistant.substring(relevantStartIndex, relevantEndIndex).trim();
    }

    // If the [New Emissions Factors] block exists, grab it and append it.
    if (newStartIndex !== -1) {
      if (cleanedResponse) cleanedResponse += "\n\n";
      cleanedResponse += assistant.substring(newStartIndex).trim();
    }

    if (!cleanedResponse) {
      // Fallback if no data blocks were found in the AI response
      res.status(200).send("Unknown");
    } else {
      res.send(cleanedResponse);
    }
    return;

  } catch (err) {
    console.error("[cf10] top-level error:", err);
    /* respond gracefully so callers get a JSON they can parse */
    res.status(200).send("Unknown");
  }
});