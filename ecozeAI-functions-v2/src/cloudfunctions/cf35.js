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

exports.cf35 = onRequest({
  region: REGION,
  timeoutSeconds: TIMEOUT, // Allow a long timeout for processing and queueing
  memory: "2GiB",
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf35] Invoked");

  try {
    // --- 0. Argument Validation ---
    const { collectionPN, userName, userId, filePath } = req.method === "POST" ? req.body : req.query;

    if (!collectionPN || !userName || !userId || !filePath) {
      res.status(400).send("Error: Missing required arguments: collectionPN, userName, userId, and filePath.");
      return;
    }

    // --- 1. Read and Parse the Excel File from Cloud Storage ---
    const storage = new Storage();
    const bucket = storage.bucket("projectId.appspot.com");
    const file = bucket.file(filePath);

    const [exists] = await file.exists();
    if (!exists) {
      res.status(404).send(`Error: The specified file does not exist at path: ${filePath}`);
      return;
    }

    const [buffer] = await file.download();
    const workbook = xlsx.read(buffer);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const dataArray = xlsx.utils.sheet_to_json(worksheet, { header: 1 });

    // AMENDED: This section is now case-insensitive
    const expectedHeaders = ['Name', 'Description', 'Main Category', 'Secondary Category', 'Tertiary Category'];
    let headerRowIndex = -1;
    for (let i = 0; i < dataArray.length; i++) {
      const row = dataArray[i].map(h => (typeof h === 'string' ? h.trim() : ''));
      const lowerCaseRow = row.map(h => h.toLowerCase());
      const lowerCaseExpected = expectedHeaders.map(h => h.toLowerCase());

      if (lowerCaseRow.length >= lowerCaseExpected.length && lowerCaseExpected.every(header => lowerCaseRow.includes(header))) {
        headerRowIndex = i;
        break;
      }
    }

    if (headerRowIndex === -1) {
      throw new Error("Could not find the required header row in the Excel file.");
    }

    // Convert rows after the header to an array of objects
    const productsToCreate = xlsx.utils.sheet_to_json(worksheet, { range: headerRowIndex });

    // --- 2. Create products_new documents in a batch ---
    const batch = db.batch();
    const newProductRefs = [];
    productsToCreate.forEach(product => {
      const docRef = db.collection("products_new").doc();
      // AMENDED: Access properties case-insensitively by checking both casings
      batch.set(docRef, {
        name: product.Name || product.name || "Unnamed Product",
        description: product.Description || product.description || "",
        category_main: product['Main Category'] || product['main category'] || "",
        category_secondary: product['Secondary Category'] || product['secondary category'] || "",
        category_tertiary: product['Tertiary Category'] || product['tertiary category'] || "",
        tu_id: userId,
        ecozeAI_Pro: false,
        in_collection: true,
        pn_collection: collectionPN,
        // Add other initial fields from cf11 here
        status: "In-Progress",
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        estimated_cf: 0,
        total_cf: 0,
        transport_cf: 0,
      });
      newProductRefs.push(docRef);
    });
    await batch.commit();
    const newProductIds = newProductRefs.map(ref => ref.id);
    logger.info(`[cf35] Successfully created ${newProductIds.length} products in Firestore.`);

    // --- 3. Update Vertex AI Search Data Store ---
    logger.info("[cf35] Allowing 30 seconds for Vertex AI Search to begin automatic ingestion...");
    await sleep(30000); // 30-second delay

    // --- 4. Queue cf11 tasks in batches ---
    const tasksClient = new CloudTasksClient();
    const project = process.env.GCP_PROJECT_ID || 'projectId';
    const queue = 'apcf-product-uploads';
    const location = REGION;
    const queuePath = tasksClient.queuePath(project, location, queue);
    const functionUrl = `https://${REGION}-${project}.cloudfunctions.net/cf11`;

    const chunkArray = (arr, size) => arr.length > 0 ? [arr.slice(0, size), ...chunkArray(arr.slice(size), size)] : [];
    const batches = chunkArray(newProductIds, 5);

    logger.info(`[cf35] Starting to queue ${newProductIds.length} tasks in ${batches.length} batches.`);

    for (let i = 0; i < batches.length; i++) {
      const currentBatch = batches[i];
      const taskPromises = currentBatch.map(productId => {
        const payload = {
          productId: productId,
          userId: userId, // Pass userId to cf11
          otherMetrics: false
        };
        // Construct a deterministic task name
        // Note: Task names must be "projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID/tasks/TASK_ID"
        const taskName = `${queuePath}/tasks/init-${productId}-${Date.now()}`;

        const task = {
          name: taskName, // <--- ADD THIS LINE
          httpRequest: {
            httpMethod: 'POST',
            url: functionUrl,
            headers: { 'Content-Type': 'application/json' },
            body: Buffer.from(JSON.stringify(payload)).toString('base64'),
          },
        };
        return tasksClient.createTask({ parent: queuePath, task });
      });

      await Promise.all(taskPromises);
      logger.info(`[cf35] Successfully queued batch ${i + 1} of ${batches.length}.`);

      if (i < batches.length - 1) {
        logger.info("[cf35] Waiting 1 minute before next batch...");
        await sleep(60000); // 60-second delay
      }
    }

    // --- 5. Delete the file from Cloud Storage ---
    await file.delete();
    logger.info(`[cf35] Successfully deleted processed file: ${filePath} `);

    // --- 6. End the function ---
    res.status(200).send("Success");

  } catch (err) {
    logger.error("[cf35] Uncaught error:", err);
    const fileToDelete = new Storage().bucket("projectId.appspot.com").file(filePath);
    await fileToDelete.delete().catch(delErr => logger.error(`[cf35] Could not delete file after error: ${delErr.message} `));
    res.status(500).send("An internal error occurred during the upload process.");
  }
});