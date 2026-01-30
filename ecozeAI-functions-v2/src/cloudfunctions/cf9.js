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

exports.cf9 = onMessagePublished({
  topic: "datastore-import-job-status-check", // Listens to the topic from the initiator
  region: REGION,
  timeoutSeconds: 540,
  memory: "1GiB", // Can be smaller
}, async (event) => {
  const { operationName, documentIds } = event.data.message.json;

  if (!operationName || !documentIds || documentIds.length === 0) {
    logger.error("Received invalid message for status check:", event.data.message.json);
    return;
  }

  logger.info(`Checking status for import operation: ${operationName}`);

  try {
    // Get the status of the long-running operation
    const [operation] = await discoveryEngineClient.checkImportDocumentsProgress(operationName);

    // If the operation is not done, we can simply let the function end.
    // Pub/Sub can be configured with a retry policy to check again later.
    if (operation.done === false) {
      logger.info(`Operation ${operationName} is still in progress. Will retry later.`);
      // Throwing an error will cause Pub/Sub to automatically retry the message later.
      throw new Error(`Operation not complete, triggering retry for ${operationName}`);
    }

    // If we get here, the operation is done.
    logger.info(`Operation ${operationName} is complete.`);

    // Check if the completed operation had an error.
    if (operation.error) {
      logger.error(`Operation ${operationName} finished with an error:`, operation.error);
      return; // Stop processing this message
    }

    // Operation was successful, update Firestore.
    const firestoreBatch = db.batch();
    documentIds.forEach(docId => {
      const docRef = db.collection("emissions_factors").doc(docId);
      firestoreBatch.update(docRef, { vertexAISearchable: true });
    });

    await firestoreBatch.commit();
    logger.info(`Successfully updated 'vertexAISearchable' flag for ${documentIds.length} documents in Firestore for operation ${operationName}.`);

  } catch (err) {
    // This will catch the "not complete" error and other issues
    logger.error(`Failed to process operation ${operationName}:`, err);
    // Re-throw the error to ensure Pub/Sub retries it.
    throw err;
  }
});

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------

/****************************************************************************************
 * ecozeAI Lite $$$
 ****************************************************************************************/