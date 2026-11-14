use std::fmt;
use std::str::FromStr;

/// A half-open range of journal commitments: `[start, end)`.
#[derive(Clone, Copy, Debug)]
pub struct EntryRange {
    pub start: usize,
    /// `None` means run to the end of the journal.
    pub end: Option<usize>,
}

impl EntryRange {
    pub const fn full() -> Self {
        Self { start: 0, end: None }
    }

}

impl Default for EntryRange {
    fn default() -> Self {
        Self::full()
    }
}

impl fmt::Display for EntryRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.end {
            None => write!(f, "{}..", self.start),
            Some(e) => write!(f, "{}..{}", self.start, e),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseEntryRangeError(String);

impl fmt::Display for ParseEntryRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid commitment range '{}': expected Rust range syntax such as 5.., ..10, 5..10, ..=9, or 5..=9", self.0)
    }
}

impl std::error::Error for ParseEntryRangeError {}

impl FromStr for EntryRange {
    type Err = ParseEntryRangeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(Self::full());
        }

        // Check for inclusive range first (..=) so we don't mistake it for ..
        if let Some(pos) = s.find("..") {
            let left = s[..pos].trim();
            let right = s[pos + 2..].trim();

            let inclusive = right.starts_with('=');
            let right = if inclusive { &right[1..] } else { right };

            let start: usize = if left.is_empty() {
                0
            } else {
                left.parse()
                    .map_err(|_| ParseEntryRangeError(s.to_string()))?
            };

            let end: Option<usize> = if right.is_empty() {
                None
            } else {
                let e: usize = right
                    .parse()
                    .map_err(|_| ParseEntryRangeError(s.to_string()))?;
                // Convert inclusive bound to exclusive.
                if inclusive {
                    Some(e.checked_add(1).ok_or_else(|| ParseEntryRangeError(s.to_string()))?)
                } else {
                    Some(e)
                }
            };

            // Validate ordering when both bounds are present.
            if let Some(e) = end {
                if start > e {
                    return Err(ParseEntryRangeError(s.to_string()));
                }
            }

            return Ok(EntryRange { start, end });
        }

        // If there is no .., try parsing a single usize as `n..` (from n to end).
        if let Ok(n) = s.parse::<usize>() {
            return Ok(EntryRange {
                start: n,
                end: None,
            });
        }

        Err(ParseEntryRangeError(s.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full() {
        let r: EntryRange = "..".parse().unwrap();
        assert_eq!(r.start, 0);
        assert_eq!(r.end, None);
    }

    #[test]
    fn parse_from() {
        let r: EntryRange = "5..".parse().unwrap();
        assert_eq!(r.start, 5);
        assert_eq!(r.end, None);
    }

    #[test]
    fn parse_to() {
        let r: EntryRange = "..10".parse().unwrap();
        assert_eq!(r.start, 0);
        assert_eq!(r.end, Some(10));
    }

    #[test]
    fn parse_range() {
        let r: EntryRange = "3..7".parse().unwrap();
        assert_eq!(r.start, 3);
        assert_eq!(r.end, Some(7));
    }

    #[test]
    fn parse_inclusive() {
        let r: EntryRange = "3..=6".parse().unwrap();
        assert_eq!(r.start, 3);
        assert_eq!(r.end, Some(7)); // 6 inclusive -> 7 exclusive
    }

    #[test]
    fn parse_to_inclusive() {
        let r: EntryRange = "..=9".parse().unwrap();
        assert_eq!(r.start, 0);
        assert_eq!(r.end, Some(10));
    }

    #[test]
    fn parse_single_number() {
        let r: EntryRange = "42".parse().unwrap();
        assert_eq!(r.start, 42);
        assert_eq!(r.end, None);
    }

    #[test]
    fn parse_invalid() {
        assert!("foo".parse::<EntryRange>().is_err());
        assert!("5..3".parse::<EntryRange>().is_err());
    }
}
