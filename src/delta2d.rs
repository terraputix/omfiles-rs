use crate::aligned_buffer::as_typed_slice_mut;

/// Decodes a 2D delta-encoded buffer.
///
/// # Parameters
///
/// * `length0` - The length of the first dimension of the buffer.
/// * `length1` - The length of the second dimension of the buffer.
/// * `chunk_buffer` - A mutable reference to the buffer to be decoded.
#[inline(always)]
pub fn delta2d_decode(length0: usize, length1: usize, chunk_buffer: &mut [i16]) {
    if length0 <= 1 {
        return;
    }
    for d0 in 1..length0 {
        for d1 in 0..length1 {
            let index = d0 * length1 + d1;
            // Represents the index of the previous element in a 2D delta calculation.
            let prev_index = (d0 - 1) * length1 + d1;
            chunk_buffer[index] += chunk_buffer[prev_index];
        }
    }
}

/// Encodes a 2D delta-encoded buffer.
///
/// # Parameters
///
/// * `length0` - The length of the first dimension of the buffer.
/// * `length1` - The length of the second dimension of the buffer.
/// * `chunk_buffer` - A mutable reference to the buffer to be encoded.
#[inline(always)]
pub fn delta2d_encode(length0: usize, length1: usize, chunk_buffer: &mut [i16]) {
    if length0 <= 1 {
        return;
    }
    for d0 in (1..length0).rev() {
        for d1 in 0..length1 {
            let index = d0 * length1 + d1;
            let prev_index = (d0 - 1) * length1 + d1;
            chunk_buffer[index] -= chunk_buffer[prev_index];
        }
    }
}

/// Decodes a 2D delta-encoded XOR buffer.
///
/// # Parameters
///
/// * `length0` - The length of the first dimension of the buffer.
/// * `length1` - The length of the second dimension of the buffer.
/// * `chunk_buffer` - A mutable reference to the buffer to be decoded.
#[inline(always)]
pub fn delta2d_decode_xor(length0: usize, length1: usize, chunk_buffer: &mut [f32]) {
    if length0 <= 1 {
        return;
    }
    let chunk_buffer_int = as_typed_slice_mut::<i32, f32>(chunk_buffer);

    for d0 in 1..length0 {
        for d1 in 0..length1 {
            let index = d0 * length1 + d1;
            let prev_index = (d0 - 1) * length1 + d1;
            chunk_buffer_int[index] ^= chunk_buffer_int[prev_index];
        }
    }
}

/// Encodes a 2D delta-encoded XOR buffer.
///
/// # Parameters
///
/// * `length0` - The length of the first dimension of the buffer.
/// * `length1` - The length of the second dimension of the buffer.
/// * `chunk_buffer` - A mutable reference to the buffer to be encoded.
#[inline(always)]
pub fn delta2d_encode_xor(length0: usize, length1: usize, chunk_buffer: &mut [f32]) {
    if length0 <= 1 {
        return;
    }
    let chunk_buffer_int = as_typed_slice_mut::<i32, f32>(chunk_buffer);

    for d0 in (1..length0).rev() {
        for d1 in 0..length1 {
            let index = d0 * length1 + d1;
            let prev_index = (d0 - 1) * length1 + d1;
            chunk_buffer_int[index] ^= chunk_buffer_int[prev_index];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta2d_decode() {
        let mut buffer: Vec<i16> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        delta2d_decode(2, 5, &mut buffer);
        assert_eq!(buffer, vec![1, 2, 3, 4, 5, 7, 9, 11, 13, 15]);
    }

    #[test]
    fn test_delta2d_encode() {
        let mut buffer: Vec<i16> = vec![1, 2, 3, 4, 5, 7, 9, 11, 13, 15];
        delta2d_encode(2, 5, &mut buffer);
        assert_eq!(buffer, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn test_delta2d_decode_xor() {
        let mut buffer: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        delta2d_decode_xor(2, 5, &mut buffer);
        let expected: Vec<f32> = vec![
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
            2.5521178e38,
            2.0571151e-38,
            3.526483e-38,
            5.2897246e-38,
            4.7019774e-38,
        ];
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_delta2d_encode_xor() {
        let mut buffer: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 7.0, 5.0, 11.0, 12.0, 15.0];
        delta2d_encode_xor(2, 5, &mut buffer);
        let expected: Vec<f32> = vec![
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
            2.9774707e38,
            1.469368e-38,
            4.4081038e-38,
            7.052966e-38,
            7.6407133e-38,
        ];
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_delta2d_xor_roundtrip() {
        let mut buffer: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        delta2d_decode_xor(2, 5, &mut buffer);
        delta2d_encode_xor(2, 5, &mut buffer);
        let expected: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        assert_eq!(buffer, expected);
    }

    #[test]
    fn test_delta2d_roundtrip() {
        let mut buffer: Vec<i16> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        delta2d_decode(2, 5, &mut buffer);
        delta2d_encode(2, 5, &mut buffer);
        let expected: Vec<i16> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        assert_eq!(buffer, expected);
    }
}
