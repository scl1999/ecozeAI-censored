async function isValidUrl(url) {
  try {
    const res = await fetch(url, { method: "HEAD", timeout: 5000 });
    return res.ok;
  } catch (e) {
    return false;
  }
}

function harvestMetadataUrls(chunk, bucket) {
  if (!chunk.candidates || !Array.isArray(chunk.candidates)) return;

  chunk.candidates.forEach(candidate => {
    // 1. Check for the modern 'groundingMetadata' format
    const gm = candidate.groundingMetadata;
    if (gm && Array.isArray(gm.groundingChunks)) {
      gm.groundingChunks.forEach(gc => {
        if (gc.web && gc.web.uri) {
          bucket.add(gc.web.uri);
        } else if (gc.maps && gc.maps.uri) {
          bucket.add(gc.maps.uri);
        }
      });
    }

    // 2. Check for the older 'citationMetadata' format
    const cm = candidate.citationMetadata;
    if (cm && Array.isArray(cm.citations)) {
      cm.citations.forEach(citation => {
        if (citation.uri) {
          bucket.add(citation.uri);
        }
      });
    }

    // 3. Check for the 'url_context_metadata' format
    const ucm = candidate.url_context_metadata || candidate.urlContextMetadata;
    const urlMetadata = ucm?.url_metadata || ucm?.urlMetadata;

    if (ucm && Array.isArray(urlMetadata)) {
      urlMetadata.forEach(um => {
        if (um.retrieved_url) {
          bucket.add(um.retrieved_url);
        } else if (um.retrievedUrl) {
          bucket.add(um.retrievedUrl);
        }
      });
    }
  });
}

function harvestFallbackTextUrls(chunk, bucket) {
  if (!chunk.candidates || !Array.isArray(chunk.candidates)) return;

  chunk.candidates.forEach(candidate => {
    // 4. Fallback: Scan text content for Markdown links or plain URLs
    if (candidate.content && candidate.content.parts) {
      candidate.content.parts.forEach(part => {
        if (part.text) {
          // Regex to capture https URLs
          const urlRegex = /https?:\/\/[^\s)"']+/g;
          const matches = part.text.match(urlRegex);
          if (matches) {
            matches.forEach(url => {
              // Basic cleaning of trailing punctuation
              let clean = url.replace(/[.,;>]+$/, '');
              bucket.add(clean);
            });
          }
        }
      });
    }
  });
}

function harvestUrls(chunk, bucket) {
  harvestMetadataUrls(chunk, bucket);
  harvestFallbackTextUrls(chunk, bucket);
}

function harvestUrlsFromText(text, bucket) {
  if (!text || typeof text !== 'string') return;
  // Regex to capture https URLs
  const urlRegex = /https?:\/\/[^\s)"']+/g;
  const matches = text.match(urlRegex);
  if (matches) {
    matches.forEach(url => {
      // Basic cleaning of trailing punctuation often caught by regex
      let clean = url.replace(/[.,;>]+$/, '');
      // Basic length check
      if (clean && clean.length > 7) {
        bucket.add(clean);
      }
    });
  }
}

async function annotateAndCollectSources(answerForTurn, rawChunksForTurn, urlCitationMap, citationCounter) {
  if (!rawChunksForTurn || rawChunksForTurn.length === 0) {
    return { annotatedAnswer: answerForTurn, newSourcesList: [] };
  }

  const redirectUris = new Set();
  for (const chunk of rawChunksForTurn) {
    if (chunk.candidates) {
      for (const candidate of chunk.candidates) {
        // 1. Check for groundingMetadata (Google Search)
        if (candidate.groundingMetadata?.groundingChunks) {
          for (const gc of candidate.groundingMetadata.groundingChunks) {
            if (gc.web?.uri) redirectUris.add(gc.web.uri);
            else if (gc.maps?.uri) redirectUris.add(gc.maps.uri);
          }
        }
        // 2. Check for url_context_metadata (URL Context tool)
        if (candidate.url_context_metadata?.url_metadata) {
          for (const um of candidate.url_context_metadata.url_metadata) {
            if (um.retrieved_url) redirectUris.add(um.retrieved_url);
          }
        }
      }
    }
  }

  if (redirectUris.size === 0) {
    return { annotatedAnswer: answerForTurn, newSourcesList: [] };
  }

  const redirectUriArray = Array.from(redirectUris);
  const unwrappedUris = await Promise.all(redirectUriArray.map(uri => unwrapVertexRedirect(uri)));
  const redirectMap = new Map(redirectUriArray.map((uri, i) => [uri, unwrappedUris[i]]));

  let currentCounter = citationCounter;
  unwrappedUris.forEach(url => {
    if (url && !urlCitationMap.has(url)) {
      urlCitationMap.set(url, currentCounter++);
    }
  });

  const injections = [];
  for (const chunk of rawChunksForTurn) {
    if (chunk.candidates) {
      for (const candidate of chunk.candidates) {
        const gm = candidate.groundingMetadata;
        if (gm?.groundingSupports && gm?.groundingChunks) {
          for (const support of gm.groundingSupports) {
            if (!support.segment || !support.groundingChunkIndices) continue;

            const citationMarkers = [...new Set(
              support.groundingChunkIndices
                .map(chunkIndex => {
                  const groundingChunk = gm.groundingChunks[chunkIndex];
                  const redirectUri = groundingChunk?.web?.uri || groundingChunk?.maps?.uri;
                  if (!redirectUri) return '';
                  const unwrappedUri = redirectMap.get(redirectUri);
                  return (unwrappedUri && urlCitationMap.has(unwrappedUri)) ? `[${urlCitationMap.get(unwrappedUri)}]` : '';
                })
                .filter(Boolean)
            )].join('');

            if (citationMarkers) {
              injections.push({ index: support.segment.endIndex, text: ` ${citationMarkers}` });
            }
          }
        }
      }
    }
  }

  let annotatedAnswer = answerForTurn;
  if (injections.length > 0) {
    const uniqueInjections = Array.from(new Map(injections.map(item => [`${item.index}-${item.text}`, item])).values());
    uniqueInjections.sort((a, b) => b.index - a.index);
    let answerParts = answerForTurn.split('');
    for (const injection of uniqueInjections) {
      answerParts.splice(injection.index, 0, injection.text);
    }
    annotatedAnswer = answerParts.join('');
  }

  const newSourcesList = [];
  for (const [url, number] of urlCitationMap.entries()) {
    // Only add sources that were newly added in this turn
    if (number >= citationCounter) {
      newSourcesList[number - citationCounter] = `[${number}] = ${url}`;
    }
  }

  return { annotatedAnswer, newSourcesList: newSourcesList.filter(Boolean) };
}

// Helper to check if URL is reachable (not 404)
async function isReachableUrl(url) {
  try {
    const res = await fetch(url, {
      method: "HEAD",
      agent: keepAliveAgent,
      timeout: 5000,
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
      }
    });
    // Filter only clear "Not Found" signals. Accept 403 (Forbidden), 500 (Server Error), etc.
    return res.status !== 404 && res.status !== 410;
  } catch (e) {
    return false;
  }
}

async function saveURLs({
  urls = [],
  materialId = null,
  productId = null,
  eaiefId = null,
  sys = null,
  user = null,
  thoughts = null,
  answer = null,
  mMassData = false, pMassData = false,
  mSupplierData = false, pSupplierData = false,
  mTransportData = false, pTransportData = false,
  mSDCFData = false, pSDCFData = false,
  mMPCFData = false, pMPCFData = false,
  mBOMData = false, pBOMData = false,
  mMPCFPData = false, pMPCFPData = false,
  mMassReviewData = false, pMassReviewData = false,
  mCFAR = false, pCFAR = false,
  eEAIEFData = false,
  cloudfunction = null,
}) {
  if (!urls.length) return;

  const unwrappedUrls = [];
  const filteredUrls = urls.filter(u => typeof u === "string" && u.trim());

  /* Parallelized unwrap & check */
  const checkUrl = async (u) => {
    const uw = await unwrapVertexRedirect(u.trim());
    if (await isReachableUrl(uw)) return uw;
    return null;
  };
  const results = await Promise.all(filteredUrls.map(checkUrl));
  const validResults = results.filter(u => u !== null);
  unwrappedUrls.push(...validResults);

  const clean = Array.from(new Set(unwrappedUrls));

  if (!clean.length) return;

  const type =
    (eEAIEFData) ? "EAIEF" :
      (mSupplierData || pSupplierData) ? "Supplier" :
        (mMassData || pMassData) ? "Mass" :
          (mSDCFData || pSDCFData) ? "sdCF" :
            (mMPCFPData || pMPCFPData) ? "mpcfp" :
              (mMPCFData || pMPCFData) ? "mpcf" :
                (mBOMData || pBOMData) ? "BOM" :
                  (mTransportData || pTransportData) ? "Transport" :
                    (mMassReviewData || pMassReviewData) ? "Mass Review" :
                      (mCFAR || pCFAR) ? "CF AR" :
                        "Other";

  const createdDocs = [];

  async function pushUrls(parentRef, subColl) {
    if (!parentRef) return;
    const last = await parentRef.collection(subColl)
      .orderBy("index", "desc")
      .limit(1)
      .get();
    let idx = last.empty ? 0 : (last.docs[0].get("index") || 0);

    for (const u of clean) {
      idx += 1;
      const newDocRef = parentRef.collection(subColl).doc();

      const newDocPayload = {
        index: idx,
        type,
        url: u,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      };

      if (cloudfunction) {
        newDocPayload.cloudfunction = cloudfunction;
      }

      await newDocRef.set(newDocPayload);
      createdDocs.push({ ref: newDocRef, url: u });
    }
  }

  if (materialId) {
    await pushUrls(db.collection("materials").doc(materialId), "m_data");
  }
  if (productId) {
    await pushUrls(db.collection("products_new").doc(productId), "pn_data");
  }
  if (eaiefId) {
    await pushUrls(db.collection("eaief_inputs").doc(eaiefId), "e_data");
  }

  logger.info(
    `[saveURLs] stored ${clean.length} URL(s)` +
    (materialId ? ` → materials/${materialId}/m_data` : "") +
    (productId ? ` → products_new/${productId}/pn_data` : "") +
    (eaiefId ? ` → eaief_inputs/${eaiefId}/e_data` : "")
  );



}

async function saveURLsI({
  interactionId,
  materialId = null,
  productId = null,
  eaiefId = null,
  mMassData = false, pMassData = false,
  mSupplierData = false, pSupplierData = false,
  mTransportData = false, pTransportData = false,
  mSDCFData = false, pSDCFData = false,
  mMPCFData = false, pMPCFData = false,
  mBOMData = false, pBOMData = false,
  mMPCFPData = false, pMPCFPData = false,
  mMassReviewData = false, pMassReviewData = false,
  mCFAR = false, pCFAR = false,
  eEAIEFData = false,
  cloudfunction = null,
}) {
  if (!interactionId) {
    logger.warn("[saveURLsI] No interactionId provided. Cannot fetch URLs.");
    return;
  }

  // 1. Fetch History
  let historyChain = [];
  try {
    historyChain = await getInteractionChain(interactionId);
  } catch (err) {
    logger.error(`[saveURLsI] Failed to fetch history for ${interactionId}`, err);
    return; // Cannot proceed without history
  }

  if (!historyChain || historyChain.length === 0) return;

  // 2. Extract URLs from History
  const gatheredUrls = new Set();

  // Iterate through all interactions
  for (const interaction of historyChain) {
    if (interaction.outputs) {
      for (const output of interaction.outputs) {
        // Check for grounding metadata
        const gm = output.grounding_metadata;
        if (gm) {
          // Grounding Chunks
          if (gm.grounding_chunks) {
            for (const gc of gm.grounding_chunks) {
              if (gc.web?.uri) gatheredUrls.add(gc.web.uri);
              else if (gc.maps?.uri) gatheredUrls.add(gc.maps.uri);
            }
          }
          // Retrieval Queries/URLs? (Vertex Search specific fields might differ, check documentation patterns)
          // Assuming similar structure or 'retrieved_urls' if distinct.
          // User mentioned "Url context handles itself through total_tool_use_tokens" for cost, 
          // but for saving URLs we need the actual URIs.
        }
        // Also check for url_context_metadata if present in Interactions API output
        if (output.url_context_metadata?.url_metadata) {
          for (const um of output.url_context_metadata.url_metadata) {
            if (um.retrieved_url) gatheredUrls.add(um.retrieved_url);
          }
        }
      }
    }
  }

  const urls = Array.from(gatheredUrls);

  if (!urls.length) return;

  const unwrappedUrls = [];
  const filteredUrls = urls.filter(u => typeof u === "string" && u.trim());

  for (const url of filteredUrls) {
    const unwrapped = await unwrapVertexRedirect(url.trim());
    unwrappedUrls.push(unwrapped);
  }

  const clean = Array.from(new Set(unwrappedUrls));

  if (!clean.length) return;

  const type =
    (eEAIEFData) ? "EAIEF" :
      (mSupplierData || pSupplierData) ? "Supplier" :
        (mMassData || pMassData) ? "Mass" :
          (mSDCFData || pSDCFData) ? "sdCF" :
            (mMPCFPData || pMPCFPData) ? "mpcfp" :
              (mMPCFData || pMPCFData) ? "mpcf" :
                (mBOMData || pBOMData) ? "BOM" :
                  (mTransportData || pTransportData) ? "Transport" :
                    (mMassReviewData || pMassReviewData) ? "Mass Review" :
                      (mCFAR || pCFAR) ? "CF AR" :
                        "Other";

  const createdDocs = [];

  async function pushUrls(parentRef, subColl) {
    if (!parentRef) return;
    const last = await parentRef.collection(subColl)
      .orderBy("index", "desc")
      .limit(1)
      .get();
    let idx = last.empty ? 0 : (last.docs[0].get("index") || 0);

    for (const u of clean) {
      idx += 1;
      const newDocRef = parentRef.collection(subColl).doc();

      const newDocPayload = {
        index: idx,
        type,
        url: u,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      };

      if (cloudfunction) {
        newDocPayload.cloudfunction = cloudfunction;
      }

      await newDocRef.set(newDocPayload);
      createdDocs.push({ ref: newDocRef, url: u });
    }
  }

  if (materialId) {
    await pushUrls(db.collection("materials").doc(materialId), "m_data");
  }
  if (productId) {
    await pushUrls(db.collection("products_new").doc(productId), "pn_data");
  }
  if (eaiefId) {
    await pushUrls(db.collection("eaief_inputs").doc(eaiefId), "e_data");
  }

  logger.info(
    `[saveURLsI] stored ${clean.length} URL(s) for interaction ${interactionId}` +
    (materialId ? ` → materials/${materialId}/m_data` : "") +
    (productId ? ` → products_new/${productId}/pn_data` : "") +
    (eaiefId ? ` → eaief_inputs/${eaiefId}/e_data` : "")
  );

}

function extractUrlsFromInteraction(outputs) {
  const foundUrls = new Set();
  if (!outputs || !Array.isArray(outputs)) return foundUrls;

  for (const output of outputs) {
    if (output.type === 'google_search_result' && output.result?.web_search_results) {
      const results = output.result.web_search_results;
      if (Array.isArray(results)) {
        results.forEach(r => {
          if (r.url) foundUrls.add(r.url);
          if (r.link) foundUrls.add(r.link);
        });
      }
    }
    if (output.type === 'url_context_result' && output.result?.visited_urls) {
      if (Array.isArray(output.result.visited_urls)) {
        output.result.visited_urls.forEach(u => foundUrls.add(u));
      }
    }
  }
  return foundUrls;
}

async function unwrapVertexRedirect(url) {
  // Only process Vertex redirect links
  if (!VERTEX_REDIRECT_RE.test(url)) {
    return url;
  }

  try {
    const rsp = await fetch(url, {
      method: "HEAD",
      redirect: "manual",
      agent: keepAliveAgent
    });

    const loc = rsp.headers.get("location");
    if (loc && /^https?:\/\//i.test(loc)) {
      // Log the successful conversion
      //logger.info(`[unwrapVertexRedirect] SUCCESS: Converted to -> ${loc}`);
      return loc;
    }

    // Log if the fetch worked but there was no 'location' header
    // This is common for certain Vertex grounding URLs and is non-fatal
    logger.debug(`[unwrapVertexRedirect] No Location Header (expected for some grounding URLs): ${url}`);
    return url; // Fallback to original URL

  } catch (err) {
    // Log if the fetch itself failed (e.g., timeout, expired link)
    logger.warn(`[unwrapVertexRedirect] FAILED (Fetch Error for ${url}):`, err.message || err);
    return url; // Graceful fallback
  }
}