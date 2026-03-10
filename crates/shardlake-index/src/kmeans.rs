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
///
/// `centroids` can be any slice whose items dereference to `[f32]`
/// (e.g. `&[Vec<f32>]` or `&[&Vec<f32>]`), so callers can avoid cloning.
pub fn top_n_centroids<C: AsRef<[f32]>>(vec: &[f32], centroids: &[C], n: usize) -> Vec<usize> {
    let mut scores: Vec<(usize, f32)> = centroids
        .iter()
        .enumerate()
        .map(|(i, c)| (i, sq_l2(vec, c.as_ref())))
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
}
