//! CRC32C (Castagnoli, reflected polynomial 0x82F63B78).
//!
//! Output is bit-for-bit identical to the previous byte-at-a-time implementation, so
//! existing page checksums, WAL records and backup manifests stay valid. Only the speed
//! changes:
//!   * x86_64 with SSE4.2: hardware `crc32` instruction, 8 bytes per step
//!     (selected at runtime; the CPU feature check is cached by std).
//!   * everything else (or old CPUs): portable slicing-by-8, 8 bytes per step.

/// Builds the 8 lookup tables used by slicing-by-8. `T[0]` is the classic
/// byte-at-a-time table.
const fn build_tables() -> [[u32; 256]; 8] {
    let mut t = [[0u32; 256]; 8];

    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82F63B78;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        t[0][i] = crc;
        i += 1;
    }

    let mut k = 1;
    while k < 8 {
        let mut i = 0;
        while i < 256 {
            let prev = t[k - 1][i];
            t[k][i] = (prev >> 8) ^ t[0][(prev & 0xFF) as usize];
            i += 1;
        }
        k += 1;
    }
    t
}

static TABLES: [[u32; 256]; 8] = build_tables();

/// Portable slicing-by-8 update. `crc` is the running (already inverted) state.
fn update_slice8(mut crc: u32, data: &[u8]) -> u32 {
    let mut chunks = data.chunks_exact(8);
    for chunk in &mut chunks {
        let lo = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) ^ crc;
        let hi = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
        crc = TABLES[7][(lo & 0xFF) as usize]
            ^ TABLES[6][((lo >> 8) & 0xFF) as usize]
            ^ TABLES[5][((lo >> 16) & 0xFF) as usize]
            ^ TABLES[4][(lo >> 24) as usize]
            ^ TABLES[3][(hi & 0xFF) as usize]
            ^ TABLES[2][((hi >> 8) & 0xFF) as usize]
            ^ TABLES[1][((hi >> 16) & 0xFF) as usize]
            ^ TABLES[0][(hi >> 24) as usize];
    }
    for &byte in chunks.remainder() {
        crc = (crc >> 8) ^ TABLES[0][((crc ^ byte as u32) & 0xFF) as usize];
    }
    crc
}

/// Hardware update using the SSE4.2 `crc32` instruction (Castagnoli polynomial).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
fn update_hw(crc: u32, data: &[u8]) -> u32 {
    use std::arch::x86_64::{_mm_crc32_u8, _mm_crc32_u64};

    let mut state = crc as u64;
    let mut chunks = data.chunks_exact(8);
    for chunk in &mut chunks {
        let word = u64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        state = _mm_crc32_u64(state, word);
    }
    let mut crc = state as u32;
    for &byte in chunks.remainder() {
        crc = _mm_crc32_u8(crc, byte);
    }
    crc
}

#[inline]
fn update_crc(crc: u32, data: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("sse4.2") {
            // SAFETY: the sse4.2 CPU feature was just verified at runtime.
            return unsafe { update_hw(crc, data) };
        }
    }
    update_slice8(crc, data)
}

pub fn checksum(data: &[u8]) -> u32 {
    let mut hasher = Crc32cHasher::new();
    hasher.update(data);
    hasher.finalize()
}

#[derive(Debug, Clone, Copy)]
pub struct Crc32cHasher {
    crc: u32,
}

impl Crc32cHasher {
    pub const fn new() -> Self {
        Self { crc: 0xFFFFFFFF }
    }

    pub fn update(&mut self, data: &[u8]) {
        self.crc = update_crc(self.crc, data);
    }

    pub fn finalize(self) -> u32 {
        self.crc ^ 0xFFFFFFFF
    }
}

impl Default for Crc32cHasher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The original byte-at-a-time algorithm, kept as the reference oracle.
    fn reference_checksum(data: &[u8]) -> u32 {
        let mut crc = 0xFFFFFFFFu32;
        for &byte in data {
            crc = (crc >> 8) ^ TABLES[0][((crc ^ byte as u32) & 0xFF) as usize];
        }
        crc ^ 0xFFFFFFFF
    }

    fn pseudo_random_bytes(len: usize, seed: u64) -> Vec<u8> {
        let mut x = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x >> 24) as u8
            })
            .collect()
    }

    #[test]
    fn test_crc32c_standard_vector() {
        let input = b"123456789";
        assert_eq!(checksum(input), 0xE3069283);

        let mut hasher = Crc32cHasher::new();
        hasher.update(b"1234");
        hasher.update(b"56789");
        assert_eq!(hasher.finalize(), 0xE3069283);
    }

    #[test]
    fn test_crc32c_matches_reference_for_all_small_lengths_and_alignments() {
        let buf = pseudo_random_bytes(600, 7);
        for start in 0..9 {
            for len in 0..300 {
                let slice = &buf[start..start + len];
                assert_eq!(
                    checksum(slice),
                    reference_checksum(slice),
                    "mismatch start={start} len={len}"
                );
            }
        }
    }

    #[test]
    fn test_crc32c_matches_reference_for_page_sized_inputs() {
        for (i, len) in [4096usize, 4096 - 8, 8192, 65_536, 131_121]
            .into_iter()
            .enumerate()
        {
            let buf = pseudo_random_bytes(len, 100 + i as u64);
            assert_eq!(checksum(&buf), reference_checksum(&buf), "len={len}");
        }
    }

    #[test]
    fn test_crc32c_slice8_fallback_matches_reference() {
        // The portable path must agree too (it is what non-SSE4.2 machines run).
        for len in [0usize, 1, 7, 8, 9, 63, 64, 65, 4096, 5000] {
            let buf = pseudo_random_bytes(len, 55);
            let got = update_slice8(0xFFFFFFFF, &buf) ^ 0xFFFFFFFF;
            assert_eq!(got, reference_checksum(&buf), "len={len}");
        }
    }

    #[test]
    fn test_crc32c_incremental_matches_one_shot_at_every_split() {
        let buf = pseudo_random_bytes(257, 9);
        let whole = checksum(&buf);
        for split in 0..=buf.len() {
            let mut h = Crc32cHasher::new();
            h.update(&buf[..split]);
            h.update(&buf[split..]);
            assert_eq!(h.finalize(), whole, "split={split}");
        }
    }
}
