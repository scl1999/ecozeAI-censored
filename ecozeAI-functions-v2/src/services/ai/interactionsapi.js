// Helper to Create Interaction (REST) - supports Streaming
const { logger, fetch } = require('../../config/firebase');
const { INTERACTIONS_API_BASE } = require('../../config/constants');
const { getFormattedDate } = require('../../utils/time');

async function createInteraction(payload, isStreaming = false) {
  // NEW: Inject Date into Input if present
  if (payload && typeof payload.input === 'string') {
    const dateStr = getFormattedDate();
    payload.input = `[Today is ${dateStr}]\n\n${payload.input}`;
  }
  const apiKey = process.env.GOOGLE_API_KEY;
  if (!apiKey) throw new Error("GOOGLE_API_KEY not found in environment.");

  const url = `${INTERACTIONS_API_BASE}?key=${apiKey}`;
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    const txt = await response.text();
    throw new Error(`Interactions API Create Failed: ${response.status} - ${txt}`);
  }

  logger.info(`[createInteraction] Response OK. Streaming=${isStreaming}, Status=${response.status}`);

  // If streaming is requested, return the readable stream directly
  if (isStreaming) {
    return response.body;
  }

  return await response.json();
}

// Helper to Get Interaction (REST) - supports Streaming for Resume
async function getInteraction(id, options = {}) {
  const apiKey = process.env.GOOGLE_API_KEY;
  if (!apiKey) throw new Error("GOOGLE_API_KEY not found.");

  let url = `${INTERACTIONS_API_BASE}/${id}?key=${apiKey}`;

  // Add streaming query params if needed for resume
  if (options.stream) {
    url += "&stream=true";
  }
  if (options.last_event_id) {
    url += `&last_event_id=${options.last_event_id}`;
  }

  const response = await fetch(url, {
    method: 'GET',
    headers: { 'Content-Type': 'application/json' }
  });

  if (!response.ok) {
    const txt = await response.text();
    throw new Error(`Interactions API Get Failed: ${response.status} - ${txt}`);
  }

  // If streaming is requested, return the body stream
  if (options.stream) {
    return response.body;
  }

  return await response.json();
}

async function getInteractionChain(interactionId) {
  const chain = [];
  let currentId = interactionId;
  const ai = getGeminiClient();

  // Safety break to prevent infinite loops or excessive API calls
  let depth = 0;
  const MAX_DEPTH = 50;

  while (currentId && depth < MAX_DEPTH) {
    try {
      // Fetch the interaction
      const interaction = await runWithRetryI(() => ai.interactions.get(currentId));
      chain.push(interaction);

      // Move to parent
      // Note: We check if the API returns 'previous_interaction_id' or similar. 
      // Based on standard linked list patterns for turns.
      currentId = interaction.previous_interaction_id;
      depth++;
    } catch (err) {
      logger.warn(`[getInteractionChain] Broken chain at ${currentId}: ${err.message}`);
      break;
    }
  }
  return chain.reverse(); // Chronological order
}
module.exports = {
  createInteraction,
  getInteraction,
  extractUrlsFromInteraction
};