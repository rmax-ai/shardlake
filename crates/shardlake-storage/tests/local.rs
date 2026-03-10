use shardlake_storage::{LocalObjectStore, ObjectStore};

#[test]
fn test_put_get_exists_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();

    assert!(!store.exists("foo/bar").unwrap());
    store.put("foo/bar", b"hello".to_vec()).unwrap();
    assert!(store.exists("foo/bar").unwrap());

    let data = store.get("foo/bar").unwrap();
    assert_eq!(data, b"hello");

    let keys = store.list("foo/").unwrap();
    assert!(keys.contains(&"foo/bar".to_string()));

    store.delete("foo/bar").unwrap();
    assert!(!store.exists("foo/bar").unwrap());
}
