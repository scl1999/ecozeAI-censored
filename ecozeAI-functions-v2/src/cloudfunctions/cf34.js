//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf34 = onRequest({
  region: REGION,
  timeoutSeconds: 540, // 9-minute timeout for potentially large queries and file generation
  memory: "2GiB",
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf34] Invoked");

  try {
    // --- 0. Argument Validation ---
    const { userName, userId, pnCollection } = req.method === "POST" ? req.body : req.query;

    if (!userName || !userId || !pnCollection) {
      res.status(400).send("Error: Missing required arguments. userName, userId, and pnCollection are all required.");
      return;
    }

    // --- 1. Find all relevant products in Firestore ---
    logger.info(`[cf34] Querying products for userId: ${userId} in collection: ${pnCollection} `);
    const productsQuery = db.collection('products_new')
      .where('tu_id', '==', userId)
      .where('pn_collection', '==', pnCollection);
    const querySnapshot = await productsQuery.get();

    // --- Helper functions for sanitizing Excel data ---
    const EXCEL_CELL_MAX = 32767;

    const sanitize = (value) => {
      if (value == null) return null; // Keep nulls and undefined as null
      let stringValue = String(value);
      // Remove illegal XML control characters (allowed chars are \t, \n, \r)
      stringValue = stringValue.replace( /.*/, '');
      // Truncate the string to Excel's character limit for a single cell
      if (stringValue.length > EXCEL_CELL_MAX) {
        stringValue = stringValue.slice(0, EXCEL_CELL_MAX);
      }
      return stringValue;
    };

    // Helper to format numbers consistently as strings or return null
    const formatNumber = (num) => (typeof num === 'number' && isFinite(num)) ? num.toFixed(2) : null;

    if (querySnapshot.empty) {
      logger.warn("[cf34] No matching products found.");
      res.status(404).send("No products found for the specified user and collection name.");
      return;
    }
    logger.info(`[cf34] Found ${querySnapshot.size} products to process.`);

    // --- 3. Define Excel Headers ---
    const headers = [
      'Name', 'Description', 'Main Category', 'Secondary Category', 'Tertiary Category',
      'createdAt', 'Manufacturer Name', 'Assembly Address', 'Country of Origin', 'Mass',
      'Mass Unit', 'Manufacturer Disclosed CF', 'MDCF Sources', 'ecozeAI CF (kgCO2e)',
      'CF Reasoning', 'Data Sources'
    ];

    // Initialize the data structure for Excel with the header row first
    const excelData = [headers];

    // --- 4. Process each product and build the sanitized data row-by-row ---
    for (const pDoc of querySnapshot.docs) {
      const pData = pDoc.data() || {};

      // Fetch subcollection data in parallel
      const pnDataPromise = pDoc.ref.collection('pn_data').get();
      const reasoningPromise = pDoc.ref.collection('pn_reasoning')
        .where('cloudfunction', '==', 'cf15')
        .orderBy('createdAt', 'desc')
        .limit(1)
        .get();
      const [pnDataSnap, reasoningSnap] = await Promise.all([pnDataPromise, reasoningPromise]);

      // Process Data Sources
      const allUrls = [];
      const sdcfUrls = [];
      pnDataSnap.forEach(doc => {
        const data = doc.data();
        if (data.url) {
          allUrls.push(data.url);
          if (data.type === 'sdCF') {
            sdcfUrls.push(data.url);
          }
        }
      });

      // Process Reasoning Text
      let reasoningText = "";
      if (!reasoningSnap.empty) {
        const reasoningData = reasoningSnap.docs[0].data() || {};
        // Prioritize using the amended reasoning if it exists and is not empty
        if (reasoningData.reasoningAmended) {
          reasoningText = reasoningData.reasoningAmended;
        } else if (reasoningData.reasoningOriginal) {
          // Fallback to the original reasoning if amended one is not available
          const originalReasoning = reasoningData.reasoningOriginal;
          const responseMarker = "Response:";
          const markerIndex = originalReasoning.indexOf(responseMarker);
          if (markerIndex !== -1) {
            reasoningText = originalReasoning.substring(markerIndex + responseMarker.length).trim();
          }
        }
      }

      // Construct the row, applying sanitization to every text field
      excelData.push([
        sanitize(pData.name),
        sanitize(pData.description),
        sanitize(pData.category_main),
        sanitize(pData.category_secondary),
        sanitize(pData.category_tertiary),
        sanitize(pData.createdAt?.toDate().toISOString()),
        sanitize(pData.manufacturer_name),
        sanitize(pData.supplier_address),
        sanitize(pData.country_of_origin),
        formatNumber(pData.mass),
        sanitize(pData.mass_unit),
        formatNumber(pData.supplier_cf),
        sanitize(sdcfUrls.join(', ')),
        formatNumber(pData.cf_full),
        sanitize(reasoningText),
        sanitize(allUrls.join(', ')),
      ]);
    }

    // --- 2 & 5. Create Excel file, upload to GCS, and get a download link ---
    const worksheet = xlsx.utils.aoa_to_sheet(excelData);
    const workbook = xlsx.utils.book_new();
    xlsx.utils.book_append_sheet(workbook, worksheet, 'ecozeAI Products');
    const buffer = xlsx.write(workbook, { type: 'buffer', bookType: 'xlsx', compression: false });

    const storage = new Storage();
    const bucket = storage.bucket("projectId.appspot.com");
    const filePath = `eai_companies/${userName.trim()}/${userId}_ecozeAI_products.xlsx`;
    const file = bucket.file(filePath);

    await file.save(buffer, {
      metadata: { contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }
    });
    logger.info(`[cf34] Excel file successfully saved to ${filePath}`);

    // Generate a signed URL valid for 15 minutes
    const signedUrlOptions = {
      version: 'v4',
      action: 'read',
      expires: Date.now() + 15 * 60 * 1000, // 15 minutes
    };
    const [downloadUrl] = await file.getSignedUrl(signedUrlOptions);
    logger.info(`[cf34] Generated signed URL successfully.`);

    // --- 6. Return the downloadable link ---
    res.status(200).send(downloadUrl);

  } catch (err) {
    logger.error("[cf34] Uncaught error:", err);
    res.status(500).send("An internal error occurred while generating the product report.");
  }
});