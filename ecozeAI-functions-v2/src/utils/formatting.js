const { logger } = require('../config/firebase');
const { BOM_RE } = require('../config/constants');

function parseCfValue(txt = "") {
  const m = txt.match(/\*?cf_value\s*=\s*([0-9.,eE+\-]+)/i);
  if (!m) return null;
  const v = parseFloat(m[1].replace(/,/g, ""));
  return Number.isFinite(v) ? v : null;
}

async function* parseNDJSON(readableStream) {
  const textDecoder = new TextDecoder();
  let buffer = '';

  for await (const chunk of readableStream) {
    // Node-fetch returns Buffer for `for await`, convert to string
    const len = chunk.length || 0;
    logger.info("[parseNDJSON] Received chunk bytes: " + len);
    if (len < 500) {
      const textAsString = typeof chunk === 'string' ? chunk : new TextDecoder().decode(chunk);
      logger.info("[parseNDJSON] Small chunk content: " + textAsString);
    }
    const chunkText = typeof chunk === 'string' ? chunk : textDecoder.decode(chunk, { stream: true });
    buffer += chunkText;

    const lines = buffer.split('\n');
    // The last line might be incomplete, keep it in the buffer
    buffer = lines.pop();

    for (const line of lines) {
      if (line.trim()) {
        try {
          // Interactions API stream returns "data: {json}"? Or just raw JSON objects?
          // Standard SSE is "data: ...".
          // The documentation usage example loop expects `chunk`.
          // Let's assume standard REST stream might be NDJSON or SSE.
          // Python `for chunk of stream` usually implies the client handles decoding.
          // In basic Node `fetch` body, we get buffers.
          // Interactions API usually follows standard SSE or simple JSON arrays.
          // "The API returns a stream of JSON objects" -> NDJSON usually.
          // If it starts with 'data:', strip it.
          const cleanLine = line.startsWith('data: ') ? line.substring(6) : line;
          if (cleanLine.trim() === '[DONE]') continue; // SSE end marker if present

          yield JSON.parse(cleanLine);
        } catch (e) {
          // Ignore parse errors for partial/keepalive lines
        }
      }
    }
  }
  if (buffer.trim()) {
    try {
      const cleanLine = buffer.startsWith('data: ') ? buffer.substring(6) : buffer;
      if (cleanLine.trim() !== '[DONE]') yield JSON.parse(cleanLine);
    } catch (e) { }
  }
}

module.exports = {
  parseCfValue,
  parseNDJSON,
  parseBom,
  parseMPCFFullProducts,
  parseOtherMetrics,
  parseMassCorrections,
  parseCFCorrections,
  parseCFAmendments,
  parseUncertaintyScores,
  parseProductCBAM,
  parseMaterialCBAM,
  parseProductCBAMProcessing,
  parseSupplierSources,
  getLabelForStep
};

function parseBom(text) {
  const out = [];
  let m;
  while ((m = BOM_RE.exec(text)) !== null) {
    const rawMass = m[5].trim(); // mass is now in group 5
    let massVal = null;
    let unit = "Unknown";

    if (rawMass.toLowerCase() !== "unknown") {
      const num = rawMass.match(/([0-9.,eE+\-]+)/);
      if (num) massVal = parseFloat(num[1].replace(/,/g, ""));
      const rem = rawMass.replace(num ? num[0] : "", "").trim();
      if (rem) unit = rem.toLowerCase();
    }

    // Normalize the index to ensure "1" matches "01" etc.
    const rawIndex = m[1];
    const normalizedIndex = String(parseInt(rawIndex, 10));

    out.push({
      index: normalizedIndex,
      mat: m[2].trim(),
      supp: m[3].trim(),
      desc: (m[4] || "").trim(),
      mass: massVal,
      unit: unit,
      // Safely handle the now-optional URL group (m[6])
      urls: (m[6] || "").split(",").map(u => u.trim()).filter(Boolean),
    });
  }
  return out;
}

function parseMPCFFullProducts(text) {
  const products = {
    existing: [],
    new: [],
  };

  // Regex for Existing Products (now only captures the name)
  const existingRegex = /\*?pa_existing_name_(\d+):\s*([^\r\n]+)/gi;
  let match;
  while ((match = existingRegex.exec(text)) !== null) {
    products.existing.push({
      name: match[2].trim(),
    });
  }

  // Regex for New Products (reasoning part removed)
  const newRegex = /\*?pa_name_(\d+):\s*([^\r\n]+)\r?\n\*?pa_carbon_footprint_\1:\s*([^\r\n]+)\r?\n\*?official_cf_sources_\1:\s*(.*?)(?=\r?\n\s*\r?\n\*?pa_name_|\s*$)/gi;
  while ((match = newRegex.exec(text)) !== null) {
    const name = match[2].trim();
    const carbonFootprintRaw = match[3].trim();
    const officialCfSources = match[4].trim();
    const carbonFootprint = parseFloat(carbonFootprintRaw);

    products.new.push({
      name: name,
      supplier_cf: Number.isFinite(carbonFootprint) ? carbonFootprint : null,
      official_cf: !!officialCfSources,
      official_cf_sources: officialCfSources || null,
    });
  }

  return products;
}

function parseOtherMetrics(text) {
  const metrics = {
    ap_value: null,
    ep_value: null,
    adpe_value: null,
    gwp_f_value: null,
    gwp_b_value: null,
    gwp_l_value: null,
  };

  const fields = Object.keys(metrics);

  fields.forEach(field => {
    // Regex to find "field: value" and capture the value on the same line
    const regex = new RegExp(`\\*?${field}:\\s*([^\\r\\n]+)`, "i");
    const match = text.match(regex);

    if (match && match[1]) {
      const rawValue = match[1].trim();
      if (rawValue.toLowerCase() !== 'unknown') {
        const parsedValue = parseFloat(rawValue);
        if (isFinite(parsedValue)) {
          metrics[field] = parsedValue;
        }
      }
    }
  });

  return metrics;
}

function parseMassCorrections(text) {
  const corrections = [];
  const regex = /\*material_(\d+):\s*([^\r\n]+)[\s\S]*?\*material_\1_new_mass:\s*([^\r\n]+)[\s\S]*?\*material_\1_new_mass_unit:\s*([^\r\n]+)[\s\S]*?\*material_\1_reasoning:\s*([\s\S]+?)(?=\n\*\s*material_|$)/gi;

  let match;
  while ((match = regex.exec(text)) !== null) {
    const newMass = parseFloat(match[3].trim());
    corrections.push({
      name: match[2].trim(),
      newMass: isFinite(newMass) ? newMass : null,
      newUnit: match[4].trim(),
      reasoning: match[5].trim(),
    });
  }
  return corrections;
}

function parseCFCorrections(text) {
  const corrections = [];
  // This regex captures the name and the two separate reasoning blocks
  const regex = /\*?material_(\d+):\s*([^\r\n]+)[\s\S]*?\*?material_\1_ccf_reasoning:\s*([\s\S]+?)\r?\n\*?material_\1_tcf_reasoning:\s*([\s\S]+?)(?=\r?\n\*?\s*material_|$)/gi;

  let match;
  while ((match = regex.exec(text)) !== null) {
    corrections.push({
      name: match[2].trim(),
      ccf_reasoning: match[3].trim(),
      tcf_reasoning: match[4].trim(),
    });
  }
  return corrections;
}

function parseCFAmendments(text) {
  const corrected_calculated_cf_match = text.match(/corrected_calculated_cf_kgCO2e:\s*([\d.]+)/i);
  const corrected_transport_cf_match = text.match(/corrected_transport_cf_kgCO2e:\s*([\d.]+)/i);

  const calculated_cf = corrected_calculated_cf_match ? parseFloat(corrected_calculated_cf_match[1]) : null;
  const transport_cf = corrected_transport_cf_match ? parseFloat(corrected_transport_cf_match[1]) : null;

  return {
    calculated_cf: Number.isFinite(calculated_cf) ? calculated_cf : null,
    transport_cf: Number.isFinite(transport_cf) ? transport_cf : null,
  };
}

function parseUncertaintyScores(text) {
  const scores = {};
  const regexMap = {
    precision: /precision_score:\s*([\d.]+)/i,
    completeness: /completeness_score:\s*([\d.]+)/i,
    temporal: /temporal_representativeness_score:\s*([\d.]+)/i,
    geographical: /geographical_representativeness_score:\s*([\d.]+)/i,
    technological: /technological_representativeness_score:\s*([\d.]+)/i,
  };

  for (const key in regexMap) {
    const match = text.match(regexMap[key]);
    const value = match ? parseFloat(match[1]) : null;
    scores[key] = Number.isFinite(value) ? value : null;
  }
  return scores;
}

function parseProductCBAM(text) {
  const inScopeMatch = text.match(/\*cbam_in_scope:\s*(TRUE|FALSE)/i);
  const reasoningMatch = text.match(/\*cbam_in_scope_reasoning:\s*([\s\S]+?)(?=\r?\n\*|$)/i);
  const cnCodeMatch = text.match(/\*cn_code:\s*([^\r\n]*)/i);
  const estCostMatch = text.match(/\*cbam_est_cost:\s*([^\r\n]*)/i);
  const carbonPriceMatch = text.match(/\*carbon_price_paid:\s*([^\r\n]*)/i);

  return {
    inScope: inScopeMatch ? /true/i.test(inScopeMatch[1]) : null,
    reasoning: reasoningMatch ? reasoningMatch[1].trim() : null,
    cnCode: cnCodeMatch ? cnCodeMatch[1].trim() : null,
    estCost: estCostMatch ? estCostMatch[1].trim() : null,
    carbonPrice: carbonPriceMatch ? carbonPriceMatch[1].trim() : null,
  };
}

function parseMaterialCBAM(text) {
  const materials = [];
  const regex = /\*cbam_material_(\d+):\s*([^\r\n]+)[\s\S]*?\*cbam_cn_code_\1:\s*([^\r\n]+)/gi;
  let match;
  while ((match = regex.exec(text)) !== null) {
    materials.push({
      name: match[2].trim(),
      cn_code: match[3].trim(),
    });
  }
  return materials;
}

function parseProductCBAMProcessing(text) {
  const processNameMatch = text.match(/^\s*process_name:\s*([^\r\n]+)/im);

  const fuels = [];
  const fuelRegex = /\*fuel_or_electricity_used_(\d+):\s*([^\r\n]+)[\s\S]*?\*fe_amount_\1:\s*([^\r\n]+)[\s\S]*?\*fe_amount_unit_\1:\s*([^\r\n]+)[\s\S]*?\*fe_scope_\1:\s*([^\r\n]+)[\s\S]*?\*fe_co2e_kg_\1:\s*([^\r\n]+)/gi;
  let fuelMatch;
  while ((fuelMatch = fuelRegex.exec(text)) !== null) {
    const amount = parseFloat(fuelMatch[3]);
    const co2e = parseFloat(fuelMatch[6]);
    fuels.push({
      name: fuelMatch[2].trim(),
      amount: isFinite(amount) ? amount : null,
      amount_unit: fuelMatch[4].trim(),
      scope: fuelMatch[5].trim(),
      co2e_kg: isFinite(co2e) ? co2e : null,
    });
  }

  const wastes = [];
  const wasteRegex = /\*pcmi_waste_(\d+):\s*([^\r\n]+)[\s\S]*?\*pcmi_waste_amount_\1:\s*([^\r\n]+)[\s\S]*?\*pcmi_waste_co2e_kg_\1:\s*([^\r\n]+)/gi;
  let wasteMatch;
  while ((wasteMatch = wasteRegex.exec(text)) !== null) {
    const co2e = parseFloat(wasteMatch[4]);
    wastes.push({
      material_name: wasteMatch[2].trim(),
      amount: wasteMatch[3].trim(), // Amount is a string like "0.05 kg"
      co2e_kg: isFinite(co2e) ? co2e : null,
    });
  }

  return {
    processName: processNameMatch ? processNameMatch[1].trim() : null,
    fuels: fuels,
    wastes: wastes,
  };
}

function parseSupplierSources(text) {
  const sources = [];
  const regex = /\*?material_(\d+):\s*([^\r\n]+)\r?\n\*?material_url_\1:\s*([^\r\n]+)\r?\n\*?material_url_used_info_\1:\s*([\s\S]+?)(?=\r?\n\*?material_|$)/gi;

  let match;
  while ((match = regex.exec(text)) !== null) {
    sources.push({
      name: match[2].trim(),
      url: match[3].trim(),
      info_used: match[4].trim(),
    });
  }
  return sources;
}

function getLabelForStep(step) {
  switch (step) {
    case "2.1": return "Send initial outreach message / email";
    case "2.2":
    case "2.3":
    case "2.4": return "Outreach chase-up email";

    case "3.1": return "Send meeting message / email";
    case "3.2":
    case "3.3":
    case "3.4": return "Meeting chase-up email";

    case "4.1": return "Ask why not active on ecozeAI";
    case "4.2":
    case "4.3":
    case "4.4": return "Active user chase-up email";

    case "5.1": return "Pilot email";
    case "5.2":
    case "5.3":
    case "5.4": return "Pilot chase-up email";

    default: return "Review Prospect Task"; // A safe default
  }
}