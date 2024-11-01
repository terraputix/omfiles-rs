use std::ops::Range;

pub fn divide_rounded_up(value: usize, divisor: usize) -> usize {
    let rem = value % divisor;
    if rem == 0 {
        value / divisor
    } else {
        value / divisor + 1
    }
}

/// Divide lower and upper bound. For upper bound use `divide_rounded_up`.
pub fn divide_range(range: &Range<usize>, divisor: usize) -> Range<usize> {
    Range {
        start: range.start / divisor,
        end: divide_rounded_up(range.end, divisor),
    }
}

pub fn add_range(range: &Range<usize>, offset: usize) -> Range<usize> {
    Range {
        start: range.start + offset,
        end: range.end + offset,
    }
}

// pub fn subtract_range(range: &Range<usize>, offset: usize) -> Range<usize> {
//     Range {
//         start: range.start - offset,
//         end: range.end - offset,
//     }
// }

// pub fn clamp_range(range: &Range<usize>, to: &Range<usize>) -> Range<usize> {
//     Range {
//         start: range.start.clamp(to.start, to.end),
//         end: range.end.clamp(to.start, to.end),
//     }
// }
