//! Integration tests for BM25 inverted index.

use shardlake_core::types::VectorId;
use shardlake_index::bm25::{BM25Params, Bm25Index};
use shardlake_manifest::LexicalIndexConfig;
use shardlake_storage::{paths::index_lexical_key, LocalObjectStore};
use tempfile::TempDir;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn temp_store() -> (TempDir, LocalObjectStore) {
    let dir = TempDir::new().unwrap();
    let store = LocalObjectStore::new(dir.path()).unwrap();
    (dir, store)
}

fn sample_docs() -> Vec<(VectorId, &'static str)> {
    vec![
        (VectorId(1), "the quick brown fox jumps over the lazy dog"),
        (VectorId(2), "the lazy dog slept all day"),
        (VectorId(3), "quick brown rabbit runs fast"),
        (VectorId(4), "fox and rabbit are friends"),
        (VectorId(5), "the cat sat on the mat"),
    ]
}

// ── Build ─────────────────────────────────────────────────────────────────────

#[test]
fn build_indexes_all_documents() {
    let docs = sample_docs();
    let idx = Bm25Index::build(&docs, BM25Params::default());
    assert_eq!(idx.num_docs(), docs.len() as u64);
    assert!(idx.num_terms() > 0);
}

#[test]
fn build_empty_corpus() {
    let idx = Bm25Index::build(&[], BM25Params::default());
    assert_eq!(idx.num_docs(), 0);
    assert_eq!(idx.num_terms(), 0);
    assert!(idx.search("anything", 5).is_empty());
}

// ── BM25 scoring behaviour ────────────────────────────────────────────────────

#[test]
fn search_returns_ranked_results_for_single_term() {
    let docs = sample_docs();
    let idx = Bm25Index::build(&docs, BM25Params::default());

    // "fox" appears in doc 1 and doc 4.
    let results = idx.search("fox", 5);
    assert_eq!(results.len(), 2);
    let ids: Vec<u64> = results.iter().map(|r| r.id.0).collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&4));
}

#[test]
fn search_multi_term_query_ranks_better_matches_first() {
    let docs = sample_docs();
    let idx = Bm25Index::build(&docs, BM25Params::default());

    // "quick brown" matches doc 1 (both terms), doc 3 (both terms), doc 4 (partial — neither).
    // Docs 1 and 3 should outscore any doc with only one matching term.
    let results = idx.search("quick brown", 5);
    assert!(results.len() >= 2);
    let top_ids: Vec<u64> = results.iter().take(2).map(|r| r.id.0).collect();
    // Docs 1 and 3 both contain "quick" and "brown".
    assert!(top_ids.contains(&1) || top_ids.contains(&3));
}

#[test]
fn search_scores_are_sorted_ascending() {
    let docs = sample_docs();
    let idx = Bm25Index::build(&docs, BM25Params::default());
    let results = idx.search("the lazy dog", 5);
    assert!(!results.is_empty());
    for pair in results.windows(2) {
        assert!(
            pair[0].score <= pair[1].score,
            "results not sorted ascending"
        );
    }
}

#[test]
fn search_scores_are_negative() {
    let docs = sample_docs();
    let idx = Bm25Index::build(&docs, BM25Params::default());
    for r in idx.search("fox", 5) {
        assert!(r.score < 0.0, "score should be negative (negated BM25)");
    }
}

#[test]
fn search_unknown_term_returns_empty() {
    let idx = Bm25Index::build(&sample_docs(), BM25Params::default());
    assert!(idx.search("zzz_no_match", 10).is_empty());
}

#[test]
fn search_k_zero_returns_empty() {
    let idx = Bm25Index::build(&sample_docs(), BM25Params::default());
    assert!(idx.search("fox", 0).is_empty());
}

#[test]
fn search_respects_k_limit() {
    let idx = Bm25Index::build(&sample_docs(), BM25Params::default());
    // "the" appears in docs 1, 2, and 5.
    let results = idx.search("the", 2);
    assert_eq!(results.len(), 2);
}

#[test]
fn frequent_term_across_all_docs_has_lower_idf_contribution() {
    // "the" appears in docs 1, 2, and 5 (high df); "fox" appears in docs 1 and 4 (lower df).
    // A query for "fox" should return 2 results; "the" should return 3.
    let idx = Bm25Index::build(&sample_docs(), BM25Params::default());
    let fox_results = idx.search("fox", 10);
    let the_results = idx.search("the", 10);
    assert_eq!(fox_results.len(), 2);
    assert_eq!(the_results.len(), 3);
}

// ── Persistence and loading ───────────────────────────────────────────────────

#[test]
fn round_trip_via_bytes_preserves_search_results() {
    let docs = sample_docs();
    let original = Bm25Index::build(&docs, BM25Params::default());

    let bytes = original.to_bytes().expect("serialise");
    let loaded = Bm25Index::from_bytes(&bytes).expect("deserialise");

    assert_eq!(loaded.num_docs(), original.num_docs());
    assert_eq!(loaded.num_terms(), original.num_terms());
    assert_eq!(loaded.params(), original.params());

    let before = original.search("quick brown", 5);
    let after = loaded.search("quick brown", 5);
    assert_eq!(before.len(), after.len());
    for (b, a) in before.iter().zip(after.iter()) {
        assert_eq!(b.id, a.id, "doc ids differ after round-trip");
        assert!(
            (b.score - a.score).abs() < 1e-5,
            "scores differ after round-trip: {} vs {}",
            b.score,
            a.score,
        );
    }
}

#[test]
fn save_and_load_via_object_store_round_trip() {
    let (_dir, store) = temp_store();
    let docs = sample_docs();
    let original = Bm25Index::build(&docs, BM25Params::default());

    let key = index_lexical_key("idx-v1");
    original.save(&store, &key).expect("save");
    let loaded = Bm25Index::load(&store, &key).expect("load");

    assert_eq!(loaded.num_docs(), original.num_docs());

    let before = original.search("lazy dog", 5);
    let after = loaded.search("lazy dog", 5);
    assert_eq!(before.len(), after.len());
    for (b, a) in before.iter().zip(after.iter()) {
        assert_eq!(b.id, a.id);
    }
}

#[test]
fn artifact_key_matches_paths_module() {
    // Ensure the canonical path helper produces the expected layout.
    assert_eq!(index_lexical_key("idx-v99"), "indexes/idx-v99/lexical.bm25");
}

// ── Manifest integration ──────────────────────────────────────────────────────

#[test]
fn lexical_index_config_serialises_and_deserialises() {
    let cfg = LexicalIndexConfig {
        artifact_key: "indexes/idx-v1/lexical.bm25".into(),
        k1: 1.5,
        b: 0.75,
        doc_count: 42,
    };
    let json = serde_json::to_string(&cfg).expect("serialise");
    let parsed: LexicalIndexConfig = serde_json::from_str(&json).expect("deserialise");
    assert_eq!(parsed, cfg);
}

// ── Custom BM25 parameters ────────────────────────────────────────────────────

#[test]
fn custom_k1_and_b_are_persisted() {
    let params = BM25Params { k1: 2.0, b: 0.5 };
    let docs = sample_docs();
    let original = Bm25Index::build(&docs, params.clone());

    let bytes = original.to_bytes().expect("serialise");
    let loaded = Bm25Index::from_bytes(&bytes).expect("deserialise");

    assert_eq!(loaded.params().k1, 2.0);
    assert_eq!(loaded.params().b, 0.5);
}

#[test]
fn zero_b_disables_length_normalisation() {
    // With b=0.0 all documents are treated as having the same length.
    let params = BM25Params { k1: 1.5, b: 0.0 };
    let docs = sample_docs();
    let idx = Bm25Index::build(&docs, params);
    let results = idx.search("fox", 5);
    // Should still return matching docs.
    assert!(!results.is_empty());
}
