//! BM25 inverted-index for lexical retrieval over vector metadata text.
//!
//! The index is built at query-server startup from the ingested corpus (full
//! `VectorRecord` list including metadata).  Text is extracted from the
//! optional JSON metadata attached to each record: all string leaf values are
//! concatenated into a single document string per vector.
//!
//! Scoring uses the standard Okapi BM25 formula:
//!
//! ```text
//! score(q, d) = Σ_t  IDF(t) × TF_norm(t, d)
//!
//! where
//!   IDF(t)         = ln((N - df(t) + 0.5) / (df(t) + 0.5) + 1)
//!   TF_norm(t, d)  = tf(t,d) × (k1 + 1) / (tf(t,d) + k1 × (1 − b + b × |d| / avgdl))
//!   k1             = 1.2
//!   b              = 0.75
//! ```

use std::collections::HashMap;

use shardlake_core::types::{VectorId, VectorRecord};

const K1: f32 = 1.2;
const B: f32 = 0.75;

/// A BM25 inverted index built from a corpus of vector records.
#[derive(Debug, Clone)]
pub struct BM25Index {
    /// term → list of (doc_index, term_frequency)
    inverted: HashMap<String, Vec<(usize, u32)>>,
    /// doc_index → (VectorId, document_token_length)
    doc_info: Vec<(VectorId, u32)>,
    /// Total number of documents.
    num_docs: usize,
    /// Average document length in tokens.
    avg_dl: f32,
}

impl BM25Index {
    /// Build a BM25 index from a slice of [`VectorRecord`]s.
    ///
    /// Text is extracted from `record.metadata`: all string leaf values are
    /// concatenated with spaces. Records whose metadata is `None` (or
    /// contains no string values) are assigned an empty document and will
    /// not match any lexical query.
    pub fn from_records(records: &[VectorRecord]) -> Self {
        let mut inverted: HashMap<String, Vec<(usize, u32)>> = HashMap::new();
        let mut doc_info: Vec<(VectorId, u32)> = Vec::with_capacity(records.len());

        for (doc_idx, rec) in records.iter().enumerate() {
            let text = extract_text(&rec.metadata);
            let tokens = tokenize(&text);
            let dl = tokens.len() as u32;
            doc_info.push((rec.id, dl));

            // Count term frequencies for this document.
            let mut tf: HashMap<&str, u32> = HashMap::new();
            for tok in &tokens {
                *tf.entry(tok.as_str()).or_insert(0) += 1;
            }
            for (term, freq) in tf {
                inverted
                    .entry(term.to_owned())
                    .or_default()
                    .push((doc_idx, freq));
            }
        }

        let num_docs = doc_info.len();
        let avg_dl = if num_docs == 0 {
            1.0
        } else {
            doc_info.iter().map(|(_, dl)| *dl as f32).sum::<f32>() / num_docs as f32
        };

        Self {
            inverted,
            doc_info,
            num_docs,
            avg_dl,
        }
    }

    /// Score every document in the index against `query_text` and return a
    /// `Vec<(VectorId, bm25_score)>` ordered by descending score.
    ///
    /// Documents with no matching terms receive a score of `0.0` and are
    /// omitted from the result unless the corpus is empty.
    pub fn score_all(&self, query_text: &str) -> Vec<(VectorId, f32)> {
        if self.num_docs == 0 {
            return Vec::new();
        }

        let query_tokens = tokenize(query_text);
        // de-duplicate query terms
        let mut query_terms: Vec<String> = query_tokens;
        query_terms.sort();
        query_terms.dedup();

        let mut scores: Vec<f32> = vec![0.0; self.num_docs];

        for term in &query_terms {
            let Some(postings) = self.inverted.get(term) else {
                continue;
            };
            let df = postings.len() as f32;
            let n = self.num_docs as f32;
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();

            for &(doc_idx, tf) in postings {
                let dl = self.doc_info[doc_idx].1 as f32;
                let tf_norm =
                    (tf as f32 * (K1 + 1.0)) / (tf as f32 + K1 * (1.0 - B + B * dl / self.avg_dl));
                scores[doc_idx] += idf * tf_norm;
            }
        }

        // Collect non-zero results.
        let mut results: Vec<(VectorId, f32)> = self
            .doc_info
            .iter()
            .enumerate()
            .filter_map(|(i, (vid, _))| {
                if scores[i] > 0.0 {
                    Some((*vid, scores[i]))
                } else {
                    None
                }
            })
            .collect();

        // Sort descending by score (best first).
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Score `query_text` and return the top-`k` results as
    /// [`shardlake_core::types::SearchResult`]s.
    ///
    /// The `score` field is set to `-bm25_score` so that lower = better,
    /// consistent with the vector search convention.
    pub fn search(&self, query_text: &str, k: usize) -> Vec<shardlake_core::types::SearchResult> {
        let mut scored = self.score_all(query_text);
        scored.truncate(k);
        scored
            .into_iter()
            .map(|(id, bm25)| shardlake_core::types::SearchResult {
                id,
                score: -bm25, // negate: lower = better
                metadata: None,
            })
            .collect()
    }

    /// Return the number of documents in the index.
    pub fn num_docs(&self) -> usize {
        self.num_docs
    }
}

// ---------------------------------------------------------------------------
// Text helpers
// ---------------------------------------------------------------------------

/// Recursively extract all string leaf values from a JSON value.
pub fn extract_text(metadata: &Option<serde_json::Value>) -> String {
    match metadata {
        None => String::new(),
        Some(v) => collect_strings(v),
    }
}

pub fn collect_strings(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Object(map) => map
            .values()
            .map(collect_strings)
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(" "),
        serde_json::Value::Array(arr) => arr
            .iter()
            .map(collect_strings)
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(" "),
        _ => String::new(),
    }
}

/// Lowercase whitespace-split tokenisation.
fn tokenize(text: &str) -> Vec<String> {
    text.split_whitespace()
        .map(|t| t.to_lowercase())
        .map(|t| {
            // strip leading/trailing punctuation
            t.trim_matches(|c: char| !c.is_alphanumeric()).to_owned()
        })
        .filter(|t| !t.is_empty())
        .collect()
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use shardlake_core::types::{VectorId, VectorRecord};

    fn record(id: u64, text: &str) -> VectorRecord {
        VectorRecord {
            id: VectorId(id),
            data: vec![0.0],
            metadata: Some(serde_json::json!({ "text": text })),
        }
    }

    #[test]
    fn test_basic_scoring() {
        let corpus = vec![
            record(1, "the quick brown fox"),
            record(2, "the lazy dog"),
            record(3, "quick brown dog"),
        ];
        let idx = BM25Index::from_records(&corpus);
        let results = idx.score_all("quick brown");
        // doc 1 and doc 3 contain both terms; doc 2 contains neither
        assert!(results.iter().any(|(id, _)| id.0 == 1));
        assert!(results.iter().any(|(id, _)| id.0 == 3));
        // doc 2 should not appear or score lower
        let score_2 = results.iter().find(|(id, _)| id.0 == 2).map(|r| r.1);
        assert!(score_2.is_none() || score_2.unwrap() < results[0].1);
    }

    #[test]
    fn test_no_match_omitted() {
        let corpus = vec![record(1, "hello world"), record(2, "foo bar")];
        let idx = BM25Index::from_records(&corpus);
        let results = idx.score_all("xyz");
        assert!(results.is_empty());
    }

    #[test]
    fn test_empty_corpus() {
        let idx = BM25Index::from_records(&[]);
        assert_eq!(idx.score_all("anything"), vec![]);
    }

    #[test]
    fn test_no_metadata_records() {
        let records = vec![VectorRecord {
            id: VectorId(1),
            data: vec![1.0],
            metadata: None,
        }];
        let idx = BM25Index::from_records(&records);
        // No text → no match, but index should build without panic
        assert_eq!(idx.num_docs(), 1);
        assert!(idx.score_all("anything").is_empty());
    }

    #[test]
    fn test_search_returns_negated_score() {
        let corpus = vec![record(1, "quick brown fox")];
        let idx = BM25Index::from_records(&corpus);
        let results = idx.search("quick", 5);
        assert_eq!(results.len(), 1);
        assert!(results[0].score < 0.0, "score should be negated BM25");
    }

    #[test]
    fn test_top_k_truncated() {
        let corpus: Vec<VectorRecord> = (0..10).map(|i| record(i, "shared term")).collect();
        let idx = BM25Index::from_records(&corpus);
        let results = idx.search("shared", 3);
        assert_eq!(results.len(), 3);
    }
}
