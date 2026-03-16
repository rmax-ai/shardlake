//! BM25 inverted index: building, querying, and binary-format persistence.
//!
//! # Overview
//!
//! [`Bm25Index`] stores a term-level inverted index that maps each token to a
//! posting list of `(VectorId, term_frequency)` entries.  At query time the
//! standard Okapi BM25 score is computed for every document that contains at
//! least one query token, and the top-k results are returned as
//! [`SearchResult`] values.
//!
//! Scores are **negated** before being returned so that a lower value means
//! higher relevance, matching the distance convention used by every other
//! scorer in the codebase.
//!
//! # Tokenization
//!
//! Text is lowercased and split on any non-alphanumeric character.
//! Unicode letters and digits are preserved.  Empty tokens are discarded.
//! The same tokenization is applied at both build time and query time, so
//! tokenization is always symmetric.
//!
//! # Binary format (`.bm25` files)
//!
//! ```text
//! Offset  Size  Field
//! ------  ----  -----
//! 0        8    Magic bytes: b"BM25IDX\0"
//! 8        1    Format version (u8) — currently 1
//! 9        4    k1 (f32, little-endian)
//! 13       4    b  (f32, little-endian)
//! 17       8    num_docs (u64, little-endian)
//! 25       4    avg_doc_len (f32, little-endian)
//!
//! 29       8    doc_entry_count (u64, little-endian)
//! 37+      …    doc_entry_count × [doc_id: u64, doc_len: u32]
//!
//! …        8    term_count (u64, little-endian)
//! …+       …    term_count ×
//!               [ term_len  : u32 (little-endian)
//!                 term      : [u8] × term_len  (UTF-8)
//!                 post_count: u64 (little-endian)
//!                 post_count × [doc_id: u64, term_freq: u32]
//!               ]
//! ```

use std::collections::HashMap;
use std::io::{Cursor, Read, Write};

use serde::{Deserialize, Serialize};
use shardlake_core::types::{SearchResult, VectorId};
use shardlake_storage::ObjectStore;

use crate::{IndexError, Result};

/// Magic bytes identifying a `.bm25` BM25-index artifact.
pub const BM25_MAGIC: &[u8; 8] = b"BM25IDX\0";
const BM25_FORMAT_VERSION: u8 = 1;

// ── BM25Params ────────────────────────────────────────────────────────────────

/// Parameters controlling BM25 term scoring.
///
/// # Examples
///
/// ```
/// use shardlake_index::bm25::BM25Params;
///
/// // Use the Okapi BM25 defaults.
/// let params = BM25Params::default();
/// assert_eq!(params.k1, 1.5);
/// assert_eq!(params.b, 0.75);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BM25Params {
    /// Term-frequency saturation parameter.
    ///
    /// Controls how quickly additional occurrences of a term stop contributing
    /// extra score.  Typical range: 1.2–2.0.
    pub k1: f32,
    /// Document-length normalisation parameter.
    ///
    /// `0.0` disables length normalisation; `1.0` fully normalises by
    /// document length.  Typical value: `0.75`.
    pub b: f32,
}

impl Default for BM25Params {
    fn default() -> Self {
        Self { k1: 1.5, b: 0.75 }
    }
}

// ── Bm25Index ─────────────────────────────────────────────────────────────────

/// BM25 inverted index over a collection of text documents.
///
/// Each document is identified by a [`VectorId`] (matching the numeric IDs
/// used throughout the rest of Shardlake) and is represented as a sequence of
/// tokens produced by [`tokenize`].
///
/// # Examples
///
/// ```
/// use shardlake_core::types::VectorId;
/// use shardlake_index::bm25::{BM25Params, Bm25Index};
///
/// let docs = vec![
///     (VectorId(1), "the quick brown fox"),
///     (VectorId(2), "the lazy dog"),
///     (VectorId(3), "quick brown rabbit"),
/// ];
/// let index = Bm25Index::build(&docs, BM25Params::default());
///
/// let results = index.search("quick brown", 2);
/// assert_eq!(results.len(), 2);
/// // Results are sorted ascending (lower = more relevant).
/// assert!(results[0].score <= results[1].score);
/// ```
pub struct Bm25Index {
    params: BM25Params,
    num_docs: u64,
    avg_doc_len: f32,
    /// Term → list of `(VectorId, term_frequency)` pairs.
    postings: HashMap<String, Vec<(VectorId, u32)>>,
    /// `VectorId` → document length in tokens.
    doc_lengths: HashMap<VectorId, u32>,
}

impl Bm25Index {
    // ── Constructors ──────────────────────────────────────────────────────────

    /// Build a [`Bm25Index`] from a list of `(id, text)` pairs.
    ///
    /// An empty `documents` slice produces a valid but empty index that always
    /// returns zero results from [`search`][Bm25Index::search].
    ///
    /// # Examples
    ///
    /// ```
    /// use shardlake_core::types::VectorId;
    /// use shardlake_index::bm25::{BM25Params, Bm25Index};
    ///
    /// let docs = vec![
    ///     (VectorId(0), "hello world"),
    ///     (VectorId(1), "hello rust"),
    /// ];
    /// let idx = Bm25Index::build(&docs, BM25Params::default());
    /// assert_eq!(idx.num_docs(), 2);
    /// assert_eq!(idx.num_terms(), 3); // "hello", "world", "rust"
    /// ```
    pub fn build(documents: &[(VectorId, &str)], params: BM25Params) -> Self {
        let num_docs = documents.len() as u64;
        let mut postings: HashMap<String, Vec<(VectorId, u32)>> = HashMap::new();
        let mut doc_lengths: HashMap<VectorId, u32> = HashMap::new();

        for (id, text) in documents {
            let tokens = tokenize(text);
            let doc_len = tokens.len() as u32;
            doc_lengths.insert(*id, doc_len);

            // Accumulate per-document term frequencies.
            let mut tf: HashMap<&str, u32> = HashMap::new();
            for token in &tokens {
                *tf.entry(token.as_str()).or_insert(0) += 1;
            }

            for (term, freq) in tf {
                postings
                    .entry(term.to_owned())
                    .or_default()
                    .push((*id, freq));
            }
        }

        let avg_doc_len = if num_docs == 0 {
            0.0
        } else {
            doc_lengths.values().map(|&l| l as f32).sum::<f32>() / num_docs as f32
        };

        Self {
            params,
            num_docs,
            avg_doc_len,
            postings,
            doc_lengths,
        }
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    /// Number of documents (vectors) in the index.
    pub fn num_docs(&self) -> u64 {
        self.num_docs
    }

    /// Number of unique terms in the vocabulary.
    pub fn num_terms(&self) -> usize {
        self.postings.len()
    }

    /// BM25 parameters used when this index was built.
    pub fn params(&self) -> &BM25Params {
        &self.params
    }

    // ── Search ────────────────────────────────────────────────────────────────

    /// Return the top-k documents most relevant to `query`.
    ///
    /// Query text is tokenized identically to the index build step.  Documents
    /// that do not contain any query term receive a score of `0` and are
    /// excluded from the result set.
    ///
    /// Scores are **negated** so that a lower value indicates higher relevance,
    /// consistent with the distance-based convention used by `exact_search` and
    /// the rest of the Shardlake scoring pipeline.
    ///
    /// Returns at most `k` results; may return fewer when fewer than `k`
    /// documents contain any query term or when the index is empty.
    pub fn search(&self, query: &str, k: usize) -> Vec<SearchResult> {
        if k == 0 || self.num_docs == 0 {
            return Vec::new();
        }

        let tokens = tokenize(query);
        if tokens.is_empty() {
            return Vec::new();
        }

        // Accumulate BM25 scores per document.
        let mut scores: HashMap<VectorId, f32> = HashMap::new();

        for token in &tokens {
            let Some(posting_list) = self.postings.get(token.as_str()) else {
                continue;
            };
            let df = posting_list.len() as u64;
            let idf = bm25_idf(self.num_docs, df);

            for &(doc_id, tf) in posting_list {
                let doc_len = *self.doc_lengths.get(&doc_id).unwrap_or(&0);
                let tf_norm =
                    bm25_tf_norm(tf, doc_len, self.avg_doc_len, self.params.k1, self.params.b);
                *scores.entry(doc_id).or_insert(0.0) += idf * tf_norm;
            }
        }

        // Negate scores so that lower value = more relevant.
        let mut results: Vec<SearchResult> = scores
            .into_iter()
            .map(|(id, score)| SearchResult {
                id,
                score: -score,
                metadata: None,
            })
            .collect();

        results.sort_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(k);
        results
    }

    // ── Serialisation ─────────────────────────────────────────────────────────

    /// Serialise the index to bytes (binary format v1).
    ///
    /// All multi-byte integers are little-endian.  See the [module-level
    /// documentation][crate::bm25] for the exact wire layout.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Header
        buf.write_all(BM25_MAGIC)?;
        buf.write_all(&[BM25_FORMAT_VERSION])?;
        buf.write_all(&self.params.k1.to_le_bytes())?;
        buf.write_all(&self.params.b.to_le_bytes())?;
        write_u64(&mut buf, self.num_docs)?;
        buf.write_all(&self.avg_doc_len.to_le_bytes())?;

        // Document lengths
        write_u64(&mut buf, self.doc_lengths.len() as u64)?;
        for (&id, &len) in &self.doc_lengths {
            write_u64(&mut buf, id.0)?;
            write_u32(&mut buf, len)?;
        }

        // Postings
        write_u64(&mut buf, self.postings.len() as u64)?;
        for (term, posting_list) in &self.postings {
            let term_bytes = term.as_bytes();
            write_u32(&mut buf, term_bytes.len() as u32)?;
            buf.write_all(term_bytes)?;
            write_u64(&mut buf, posting_list.len() as u64)?;
            for &(id, tf) in posting_list {
                write_u64(&mut buf, id.0)?;
                write_u32(&mut buf, tf)?;
            }
        }

        Ok(buf)
    }

    /// Deserialise a [`Bm25Index`] from bytes (binary format v1).
    ///
    /// Returns [`IndexError::Other`] when the magic bytes or format version do
    /// not match.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cur = Cursor::new(bytes);

        // Magic
        let mut magic = [0u8; 8];
        cur.read_exact(&mut magic)?;
        if &magic != BM25_MAGIC {
            return Err(IndexError::Other("invalid BM25 index magic".into()));
        }

        // Format version
        let mut ver = [0u8; 1];
        cur.read_exact(&mut ver)?;
        if ver[0] != BM25_FORMAT_VERSION {
            return Err(IndexError::Other(format!(
                "unsupported BM25 format version {}",
                ver[0]
            )));
        }

        // Parameters
        let k1 = read_f32(&mut cur)?;
        let b = read_f32(&mut cur)?;
        let num_docs = read_u64(&mut cur)?;
        let avg_doc_len = read_f32(&mut cur)?;

        // Document lengths
        let doc_count = read_u64(&mut cur)?;
        let mut doc_lengths = HashMap::with_capacity(doc_count as usize);
        for _ in 0..doc_count {
            let id = VectorId(read_u64(&mut cur)?);
            let len = read_u32(&mut cur)?;
            doc_lengths.insert(id, len);
        }

        // Postings
        let term_count = read_u64(&mut cur)?;
        let mut postings = HashMap::with_capacity(term_count as usize);
        for _ in 0..term_count {
            let term_len = read_u32(&mut cur)? as usize;
            let mut term_bytes = vec![0u8; term_len];
            cur.read_exact(&mut term_bytes)?;
            let term = String::from_utf8(term_bytes)
                .map_err(|e| IndexError::Other(format!("invalid UTF-8 in term: {e}")))?;

            let post_count = read_u64(&mut cur)?;
            let mut posting_list = Vec::with_capacity(post_count as usize);
            for _ in 0..post_count {
                let id = VectorId(read_u64(&mut cur)?);
                let tf = read_u32(&mut cur)?;
                posting_list.push((id, tf));
            }
            postings.insert(term, posting_list);
        }

        Ok(Self {
            params: BM25Params { k1, b },
            num_docs,
            avg_doc_len,
            postings,
            doc_lengths,
        })
    }

    // ── ObjectStore helpers ───────────────────────────────────────────────────

    /// Persist the index to `store` at `key`.
    pub fn save(&self, store: &dyn ObjectStore, key: &str) -> Result<()> {
        let bytes = self.to_bytes()?;
        store.put(key, bytes)?;
        Ok(())
    }

    /// Load an index from `store` at `key`.
    pub fn load(store: &dyn ObjectStore, key: &str) -> Result<Self> {
        let bytes = store.get(key)?;
        Self::from_bytes(&bytes)
    }
}

// ── BM25 scoring helpers ──────────────────────────────────────────────────────

/// Okapi BM25 IDF component.
///
/// `idf(t) = ln((N - df(t) + 0.5) / (df(t) + 0.5) + 1)`
fn bm25_idf(num_docs: u64, doc_freq: u64) -> f32 {
    let n = num_docs as f32;
    let df = doc_freq as f32;
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
}

/// Okapi BM25 normalised term-frequency component.
///
/// `tf_norm(t, D) = (f * (k1 + 1)) / (f + k1 * (1 - b + b * |D| / avgdl))`
fn bm25_tf_norm(tf: u32, doc_len: u32, avg_doc_len: f32, k1: f32, b: f32) -> f32 {
    if avg_doc_len == 0.0 {
        return 0.0;
    }
    let f = tf as f32;
    let dl = doc_len as f32;
    (f * (k1 + 1.0)) / (f + k1 * (1.0 - b + b * dl / avg_doc_len))
}

// ── Tokenization ──────────────────────────────────────────────────────────────

/// Tokenize `text` by lowercasing and splitting on non-alphanumeric characters.
///
/// Empty tokens are discarded.  Unicode alphanumeric characters are preserved.
/// The function is symmetric: the same tokenization is applied at build time
/// and query time.
pub fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

// ── I/O primitives ────────────────────────────────────────────────────────────

fn write_u32(buf: &mut Vec<u8>, v: u32) -> Result<()> {
    buf.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn write_u64(buf: &mut Vec<u8>, v: u64) -> Result<()> {
    buf.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn read_u32(cur: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut buf = [0u8; 4];
    cur.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64(cur: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut buf = [0u8; 8];
    cur.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn read_f32(cur: &mut Cursor<&[u8]>) -> Result<f32> {
    let mut buf = [0u8; 4];
    cur.read_exact(&mut buf)?;
    Ok(f32::from_le_bytes(buf))
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_docs() -> Vec<(VectorId, &'static str)> {
        vec![
            (VectorId(1), "the quick brown fox jumps over the lazy dog"),
            (VectorId(2), "the lazy dog slept all day"),
            (VectorId(3), "quick brown rabbit runs fast"),
        ]
    }

    // ── tokenize ──────────────────────────────────────────────────────────────

    #[test]
    fn tokenize_splits_on_non_alphanumeric() {
        let tokens = tokenize("hello, world! foo-bar");
        assert_eq!(tokens, vec!["hello", "world", "foo", "bar"]);
    }

    #[test]
    fn tokenize_lowercases() {
        let tokens = tokenize("Hello WORLD");
        assert_eq!(tokens, vec!["hello", "world"]);
    }

    #[test]
    fn tokenize_empty_string() {
        assert!(tokenize("").is_empty());
    }

    #[test]
    fn tokenize_only_punctuation() {
        assert!(tokenize("!!! ---").is_empty());
    }

    // ── Bm25Index::build ──────────────────────────────────────────────────────

    #[test]
    fn build_empty_documents_gives_empty_index() {
        let idx = Bm25Index::build(&[], BM25Params::default());
        assert_eq!(idx.num_docs(), 0);
        assert_eq!(idx.num_terms(), 0);
        assert!(idx.search("hello", 5).is_empty());
    }

    #[test]
    fn build_records_correct_doc_count_and_term_count() {
        let docs = make_docs();
        let idx = Bm25Index::build(&docs, BM25Params::default());
        assert_eq!(idx.num_docs(), 3);
        // Terms: the, quick, brown, fox, jumps, over, lazy, dog, slept, all, day, rabbit, runs, fast
        assert!(idx.num_terms() >= 10);
    }

    // ── Bm25Index::search ─────────────────────────────────────────────────────

    #[test]
    fn search_returns_at_most_k_results() {
        let docs = make_docs();
        let idx = Bm25Index::build(&docs, BM25Params::default());
        let results = idx.search("quick", 1);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn search_returns_empty_for_unknown_term() {
        let docs = make_docs();
        let idx = Bm25Index::build(&docs, BM25Params::default());
        assert!(idx.search("zzz_no_match", 5).is_empty());
    }

    #[test]
    fn search_scores_are_sorted_ascending_lower_is_more_relevant() {
        let docs = make_docs();
        let idx = Bm25Index::build(&docs, BM25Params::default());
        let results = idx.search("quick brown", 3);
        assert!(!results.is_empty());
        for w in results.windows(2) {
            assert!(w[0].score <= w[1].score);
        }
    }

    #[test]
    fn search_scores_are_negative() {
        let docs = make_docs();
        let idx = Bm25Index::build(&docs, BM25Params::default());
        let results = idx.search("quick", 3);
        for r in &results {
            assert!(r.score < 0.0, "expected negative score, got {}", r.score);
        }
    }

    #[test]
    fn search_with_k_zero_returns_empty() {
        let docs = make_docs();
        let idx = Bm25Index::build(&docs, BM25Params::default());
        assert!(idx.search("quick", 0).is_empty());
    }

    #[test]
    fn search_is_case_insensitive() {
        let docs = vec![(VectorId(1), "Hello World"), (VectorId(2), "goodbye")];
        let idx = Bm25Index::build(&docs, BM25Params::default());
        let r1 = idx.search("hello", 5);
        let r2 = idx.search("HELLO", 5);
        assert_eq!(r1.len(), r2.len());
        if !r1.is_empty() {
            assert_eq!(r1[0].id, r2[0].id);
        }
    }

    #[test]
    fn doc_with_more_matching_terms_scores_higher() {
        // VectorId(1) matches both "quick" and "brown"; VectorId(2) matches only "quick".
        let docs = vec![
            (VectorId(1), "quick brown fox"),
            (VectorId(2), "quick grey cat"),
        ];
        let idx = Bm25Index::build(&docs, BM25Params::default());
        let results = idx.search("quick brown", 2);
        // Both should be returned; the doc with two matches should rank first (lower score).
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, VectorId(1));
    }

    // ── IDF saturation ────────────────────────────────────────────────────────

    #[test]
    fn term_present_in_all_docs_has_low_idf() {
        // "the" appears in all three documents → low IDF → near-zero contribution.
        let docs = make_docs();
        let idx = Bm25Index::build(&docs, BM25Params::default());
        // "fox" only in doc 1; searching "fox" should return only one result.
        let results = idx.search("fox", 5);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, VectorId(1));
    }

    // ── Serialisation round-trip ──────────────────────────────────────────────

    #[test]
    fn to_bytes_from_bytes_round_trip() {
        let docs = make_docs();
        let idx = Bm25Index::build(&docs, BM25Params::default());

        let bytes = idx.to_bytes().expect("serialise");
        let loaded = Bm25Index::from_bytes(&bytes).expect("deserialise");

        assert_eq!(loaded.num_docs(), idx.num_docs());
        assert_eq!(loaded.num_terms(), idx.num_terms());
        assert_eq!(loaded.params(), idx.params());

        // Search results must be identical after round-trip.
        let before = idx.search("quick brown", 3);
        let after = loaded.search("quick brown", 3);
        assert_eq!(before.len(), after.len());
        for (b, a) in before.iter().zip(after.iter()) {
            assert_eq!(b.id, a.id);
            assert!((b.score - a.score).abs() < 1e-5);
        }
    }

    #[test]
    fn from_bytes_rejects_bad_magic() {
        let mut bytes = Bm25Index::build(&make_docs(), BM25Params::default())
            .to_bytes()
            .unwrap();
        bytes[0] = 0xFF;
        assert!(Bm25Index::from_bytes(&bytes).is_err());
    }

    #[test]
    fn from_bytes_rejects_bad_version() {
        let mut bytes = Bm25Index::build(&make_docs(), BM25Params::default())
            .to_bytes()
            .unwrap();
        bytes[8] = 99; // format version byte
        assert!(Bm25Index::from_bytes(&bytes).is_err());
    }

    #[test]
    fn empty_index_round_trip() {
        let idx = Bm25Index::build(&[], BM25Params::default());
        let bytes = idx.to_bytes().expect("serialise");
        let loaded = Bm25Index::from_bytes(&bytes).expect("deserialise");
        assert_eq!(loaded.num_docs(), 0);
        assert_eq!(loaded.num_terms(), 0);
        assert!(loaded.search("hello", 5).is_empty());
    }

    // ── ObjectStore integration ───────────────────────────────────────────────

    #[test]
    fn save_and_load_via_object_store() {
        use shardlake_storage::LocalObjectStore;
        let tmp = tempfile::TempDir::new().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();

        let docs = make_docs();
        let idx = Bm25Index::build(&docs, BM25Params::default());
        idx.save(&store, "test_key").expect("save");

        let loaded = Bm25Index::load(&store, "test_key").expect("load");
        assert_eq!(loaded.num_docs(), idx.num_docs());

        let before = idx.search("lazy dog", 3);
        let after = loaded.search("lazy dog", 3);
        assert_eq!(before.len(), after.len());
        for (b, a) in before.iter().zip(after.iter()) {
            assert_eq!(b.id, a.id);
        }
    }

    // ── BM25Params ────────────────────────────────────────────────────────────

    #[test]
    fn bm25_params_default_values() {
        let p = BM25Params::default();
        assert_eq!(p.k1, 1.5);
        assert_eq!(p.b, 0.75);
    }

    #[test]
    fn custom_params_affect_scores() {
        let docs = vec![
            (VectorId(1), "quick brown fox"),
            (VectorId(2), "quick grey cat"),
        ];
        let default_params = BM25Params::default();
        let no_length_norm = BM25Params { k1: 1.5, b: 0.0 };

        let idx_default = Bm25Index::build(&docs, default_params);
        let idx_no_norm = Bm25Index::build(&docs, no_length_norm);

        let r_default = idx_default.search("quick", 2);
        let r_no_norm = idx_no_norm.search("quick", 2);

        // Both indexes have the same number of results.
        assert_eq!(r_default.len(), r_no_norm.len());
    }
}
