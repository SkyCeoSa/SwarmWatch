/**
 * SwarmWatch Zero-Token Mathematical Entropy & Lexical Similarity Engine.
 * Implemented in pure native TypeScript with zero external dependencies.
 */

/**
 * Computes normalized Shannon entropy H_norm in range [0.0, 1.0] for input text tokens.
 * H = - sum(p * log2(p)) / log2(N)
 * Normalized against maximum theoretical entropy log2(N).
 * Low entropy indicates repetitive loops or semantic deadlock.
 */
export function computeShannonEntropy(text: string): number {
  if (!text || typeof text !== "string") {
    return 1.0;
  }

  const tokens = text.toLowerCase().match(/\b\w+\b/g) || [];
  const n = tokens.length;
  if (n <= 1) {
    return 1.0;
  }

  const counts: Record<string, number> = {};
  for (const token of tokens) {
    counts[token] = (counts[token] || 0) + 1;
  }

  let entropy = 0.0;
  for (const count of Object.values(counts)) {
    const p = count / n;
    entropy -= p * Math.log2(p);
  }

  const maxEntropy = Math.log2(n);
  if (maxEntropy <= 0.0) {
    return 1.0;
  }

  const normalized = entropy / maxEntropy;
  return Math.max(0.0, Math.min(1.0, normalized));
}

/**
 * Computes token-level Jaccard similarity in range [0.0, 1.0].
 * High similarity across turns flags repetitive loops.
 */
export function computeLexicalSimilarity(textA: string, textB: string): number {
  if (!textA && !textB) return 1.0;
  if (!textA || !textB) return 0.0;

  const tokensA = new Set(textA.toLowerCase().match(/\b\w+\b/g) || []);
  const tokensB = new Set(textB.toLowerCase().match(/\b\w+\b/g) || []);

  if (tokensA.size === 0 && tokensB.size === 0) return 1.0;
  if (tokensA.size === 0 || tokensB.size === 0) return 0.0;

  let intersectionCount = 0;
  for (const token of tokensA) {
    if (tokensB.has(token)) {
      intersectionCount++;
    }
  }

  const unionSize = tokensA.size + tokensB.size - intersectionCount;
  return unionSize > 0 ? intersectionCount / unionSize : 0.0;
}
