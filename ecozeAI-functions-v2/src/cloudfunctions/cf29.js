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

exports.cf29 = onRequest({
  region: REGION,
  timeoutSeconds: 60, // A short timeout is sufficient
  memory: "1GiB",     // Minimal memory is needed
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf29] Invoked");

  try {
    // 1. Get and validate the userName argument
    const userName = req.method === "POST" ? req.body?.userName : req.query.userName;

    if (!userName || typeof userName !== 'string' || !userName.trim()) {
      logger.warn("[cf29] Missing or invalid userName argument.");
      res.status(400).json({ error: "The 'userName' argument is required and must be a non-empty string." });
      return;
    }
    const sanitizedUserName = userName.trim();

    // 2. Initialize Cloud Storage client and define the path
    const storage = new Storage();
    const bucket = storage.bucket("projectId.appspot.com");
    // In Cloud Storage, "directories" are placeholder objects ending with a '/'
    const directoryPath = `eai_companies/${sanitizedUserName}/`;
    const directoryFile = bucket.file(directoryPath);

    // 3. Check if the directory placeholder object already exists
    const [exists] = await directoryFile.exists();

    if (exists) {
      // If it exists, the function's job is done.
      logger.info(`[cf29] Directory '${directoryPath}' already exists. No action taken.`);
      res.status(200).json({
        status: "exists",
        message: `Directory for user '${sanitizedUserName}' already exists.`,
      });
      return;
    }

    // 4. If it does not exist, create it by saving an empty placeholder object
    await directoryFile.save('');

    logger.info(`[cf29] Successfully created directory: '${directoryPath}'`);
    res.status(201).json({
      status: "created",
      message: `Successfully created directory for user '${sanitizedUserName}'.`,
      path: directoryPath
    });

  } catch (err) {
    logger.error("[cf29] Uncaught error:", err);
    res.status(500).json({
      status: "error",
      message: "An internal error occurred.",
      error: String(err)
    });
  }
});