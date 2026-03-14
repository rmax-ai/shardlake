//! Simple K-means implementation for centroid-based shard partitioning.

use rand::{seq::SliceRandom, Rng};

/// Run K-means clustering on `vectors` (each of length `dims`).
///
/// Returns `k` centroids of length `dims`.
pub fn kmeans(vectors: &[Vec<f32>], k: usize, iters: u32, rng: &mut impl Rng) -> Vec<Vec<f32>> {
    assert!(!vectors.is_empty(), "kmeans: empty input");
    let dims = vectors[0].len();
    let n = vectors.len();
    let k = k.min(n);

    let mut indices: Vec<usize> = (0..n).collect();
    indices.shuffle(rng);
    let mut centroids: Vec<Vec<f32>> = indices[..k].iter().map(|&i| vectors[i].clone()).collect();

    let mut assignments = vec![0usize; n];

    for _iter in 0..iters {
        // Assignment step.
        let mut changed = false;
        for (i, vec) in vectors.iter().enumerate() {
            let best = nearest_centroid(vec, &centroids);
            if best != assignments[i] {
                assignments[i] = best;
                changed = true;
            }
        }
        if !changed {
            break;
        }

        // Update step.
        let mut sums = vec![vec![0.0f32; dims]; k];
        let mut counts = vec![0usize; k];
        for (i, &c) in assignments.iter().enumerate() {
            for (d, &v) in vectors[i].iter().enumerate() {
                sums[c][d] += v;
            }
            counts[c] += 1;
        }
        for c in 0..k {
            if counts[c] > 0 {
                for d in 0..dims {
                    centroids[c][d] = sums[c][d] / counts[c] as f32;
                }
            }
        }
    }

    centroids
}

/// Return the index of the nearest centroid to `vec` using squared-Euclidean distance.
pub fn nearest_centroid(vec: &[f32], centroids: &[Vec<f32>]) -> usize {
    centroids
        .iter()
        .enumerate()
        .map(|(i, c)| (i, sq_l2(vec, c)))
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(i, _)| i)
        .unwrap_or(0)
}

/// Top-`n` nearest centroids (for nprobe).
pub fn top_n_centroids(vec: &[f32], centroids: &[Vec<f32>], n: usize) -> Vec<usize> {
    let mut scores: Vec<(usize, f32)> = centroids
        .iter()
        .enumerate()
        .map(|(i, c)| (i, sq_l2(vec, c)))
        .collect();
    scores.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    scores.iter().take(n).map(|(i, _)| *i).collect()
}

/// Squared L2 distance.
pub fn sq_l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn test_kmeans_basic() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vecs: Vec<Vec<f32>> = (0..50).map(|_| vec![0.0f32, 0.0]).collect();
        vecs.extend((0..50).map(|_| vec![100.0f32, 100.0]));
        let centroids = kmeans(&vecs, 2, 30, &mut rng);
        assert_eq!(centroids.len(), 2);
        let d0 = sq_l2(&[0.0, 0.0], &centroids[0]).min(sq_l2(&[0.0, 0.0], &centroids[1]));
        let d1 = sq_l2(&[100.0, 100.0], &centroids[0]).min(sq_l2(&[100.0, 100.0], &centroids[1]));
        assert!(d0 < 1.0, "centroid not near (0,0): {d0}");
        assert!(d1 < 1.0, "centroid not near (100,100): {d1}");
    }

    #[test]
    fn test_nearest_centroid() {
        let centroids = vec![vec![0.0f32, 0.0], vec![10.0f32, 10.0]];
        assert_eq!(nearest_centroid(&[1.0, 1.0], &centroids), 0);
        assert_eq!(nearest_centroid(&[9.0, 9.0], &centroids), 1);
    }

    /// Verify that training on a sample still produces the correct cluster
    /// structure when the data has two well-separated groups.
    #[test]
    fn test_kmeans_on_sample_finds_correct_clusters() {
        // 40 vectors near (0,0) and 40 near (100,100).  Use a hand-crafted
        // sample that includes vectors from both groups so the test is
        // deterministic without relying on a lucky random draw.
        let mut vecs: Vec<Vec<f32>> = (0..40).map(|_| vec![0.0f32, 0.0]).collect();
        vecs.extend((0..40).map(|_| vec![100.0f32, 100.0]));

        // Build a sample with 5 vectors from each group.
        let sample: Vec<Vec<f32>> = (0..5)
            .map(|_| vec![0.0f32, 0.0])
            .chain((0..5).map(|_| vec![100.0f32, 100.0]))
            .collect();

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let centroids = kmeans(&sample, 2, 30, &mut rng);
        assert_eq!(centroids.len(), 2);

        // After training on the sample, assign ALL vectors to the nearest centroid.
        let groups: Vec<usize> = vecs
            .iter()
            .map(|v| nearest_centroid(v, &centroids))
            .collect();
        // The 40 vectors at (0,0) should all map to the same centroid.
        let first_group = groups[0];
        assert!(
            groups[..40].iter().all(|&g| g == first_group),
            "vectors near (0,0) should all share one centroid"
        );
        // The 40 vectors at (100,100) should all map to the other centroid.
        let second_group = groups[40];
        assert_ne!(
            first_group, second_group,
            "the two clusters must map to different centroids"
        );
        assert!(
            groups[40..].iter().all(|&g| g == second_group),
            "vectors near (100,100) should all share one centroid"
        );
    }

    /// Two calls to `kmeans` with the same seed must return identical centroids.
    #[test]
    fn test_kmeans_is_deterministic() {
        let vecs: Vec<Vec<f32>> = (0..60).map(|i| vec![i as f32, (i * 2) as f32]).collect();

        let run = || {
            let mut rng = rand::rngs::StdRng::seed_from_u64(0xdead_beef);
            kmeans(&vecs, 3, 20, &mut rng)
        };

        let c1 = run();
        let c2 = run();
        assert_eq!(c1, c2, "kmeans must be deterministic for the same seed");
    }
}
