use crate::errors::S3O2Error;
use globset::Glob;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

/// FileFilter provides include/exclude pattern matching for file paths.
/// Follows AWS S3 sync behavior: filters are evaluated in order, last match wins.
#[derive(Clone)]
pub struct FileFilter {
    rules: Vec<FilterRule>,
    has_includes: bool,
}

#[derive(Clone)]
struct FilterRule {
    filter_type: FilterType,
    glob: Glob,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FilterType {
    Include,
    Exclude,
}

impl FileFilter {
    /// Creates a new FilterBuilder to construct a FileFilter.
    pub fn builder() -> FilterBuilder {
        FilterBuilder::default()
    }

    /// Determines if a file path should be included based on filter rules.
    ///
    /// # Order-based Evaluation (AWS S3 sync model)
    /// 1. Normalize path to use forward slashes
    /// 2. Iterate through filters in order they were added
    /// 3. Last matching filter determines the result
    /// 4. If no filters match:
    ///    - If include filters exist: reject (only explicit includes allowed)
    ///    - If only exclude filters: accept (include everything except excludes)
    ///    - If no filters: accept (include everything)
    ///
    /// # Example
    /// ```
    /// use s3o2::file_filter::FileFilter;
    ///
    /// // Exclude all .log files, then re-include important.log
    /// let filter = FileFilter::builder()
    ///     .add_exclude("*.log")
    ///     .add_include("important.log")
    ///     .build().unwrap();
    ///
    /// assert!(!filter.should_include("error.log"));      // Excluded by first rule
    /// assert!(filter.should_include("important.log"));   // Re-included by second rule
    /// ```
    pub fn should_include(&self, path: &str) -> bool {
        // Normalize path to use forward slashes for consistent matching
        let normalized = path.replace('\\', "/");

        // Determine default decision based on whether includes exist
        let default_decision = !self.has_includes;

        // Apply filters in order, last match wins
        let mut decision = default_decision;
        for rule in &self.rules {
            if rule.glob.compile_matcher().is_match(&normalized) {
                decision = match rule.filter_type {
                    FilterType::Include => true,
                    FilterType::Exclude => false,
                };
            }
        }

        decision
    }
}

#[derive(Default)]
pub struct FilterBuilder {
    rules: Vec<(FilterType, String)>,
}

impl FilterBuilder {
    /// Adds an include pattern.
    /// Filters are evaluated in the order they are added.
    pub fn add_include(mut self, pattern: &str) -> Self {
        self.rules.push((FilterType::Include, pattern.to_string()));
        self
    }

    /// Adds an exclude pattern.
    /// Filters are evaluated in the order they are added.
    pub fn add_exclude(mut self, pattern: &str) -> Self {
        self.rules.push((FilterType::Exclude, pattern.to_string()));
        self
    }

    /// Loads include patterns from a file (one pattern per line).
    /// Lines starting with # are treated as comments.
    /// Patterns are added in the order they appear in the file.
    pub fn add_includes_from_file(mut self, path: &Path) -> Result<Self, S3O2Error> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            let trimmed = line.trim();
            // Skip empty lines and comments
            if !trimmed.is_empty() && !trimmed.starts_with('#') {
                self.rules.push((FilterType::Include, trimmed.to_string()));
            }
        }

        Ok(self)
    }

    /// Loads exclude patterns from a file (one pattern per line).
    /// Lines starting with # are treated as comments.
    /// Patterns are added in the order they appear in the file.
    pub fn add_excludes_from_file(mut self, path: &Path) -> Result<Self, S3O2Error> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            let trimmed = line.trim();
            // Skip empty lines and comments
            if !trimmed.is_empty() && !trimmed.starts_with('#') {
                self.rules.push((FilterType::Exclude, trimmed.to_string()));
            }
        }

        Ok(self)
    }

    /// Builds the FileFilter, compiling all patterns.
    pub fn build(self) -> Result<FileFilter, S3O2Error> {
        let mut rules = Vec::new();
        let mut has_includes = false;

        for (filter_type, pattern) in self.rules {
            if filter_type == FilterType::Include {
                has_includes = true;
            }

            let glob = Glob::new(&pattern).map_err(|e| {
                S3O2Error::FilterError(format!("Invalid pattern '{}': {}", pattern, e))
            })?;

            rules.push(FilterRule { filter_type, glob });
        }

        Ok(FileFilter {
            rules,
            has_includes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_filter_no_patterns_accepts_all() {
        let filter = FileFilter::builder().build().unwrap();

        assert!(filter.should_include("file.txt"));
        assert!(filter.should_include("dir/file.log"));
        assert!(filter.should_include("anything"));
    }

    #[test]
    fn test_filter_exclude_only() {
        let filter = FileFilter::builder().add_exclude("*.log").build().unwrap();

        assert!(filter.should_include("file.txt"));
        assert!(filter.should_include("data.csv"));
        assert!(!filter.should_include("file.log"));
        assert!(!filter.should_include("dir/app.log"));
    }

    #[test]
    fn test_filter_include_only() {
        let filter = FileFilter::builder().add_include("*.txt").build().unwrap();

        assert!(filter.should_include("file.txt"));
        assert!(filter.should_include("dir/notes.txt"));
        assert!(!filter.should_include("file.log"));
        assert!(!filter.should_include("data.csv"));
    }

    #[test]
    fn test_filter_order_matters_exclude_then_include() {
        // AWS S3 behavior: exclude all logs, then re-include important.log
        // Because we have an include, only explicitly included files are accepted
        let filter = FileFilter::builder()
            .add_exclude("*.log")
            .add_include("important.log")
            .build()
            .unwrap();

        // Most .log files are excluded
        assert!(!filter.should_include("error.log"));
        assert!(!filter.should_include("debug.log"));

        // important.log is re-included by the later rule
        assert!(filter.should_include("important.log"));

        // Non-matching files are excluded (because include filter exists)
        assert!(!filter.should_include("data.txt"));
    }

    #[test]
    fn test_filter_exclude_only_default_include() {
        // When there are NO include filters, default is to include everything
        let filter = FileFilter::builder().add_exclude("*.log").build().unwrap();

        // .log files are excluded
        assert!(!filter.should_include("error.log"));
        assert!(!filter.should_include("debug.log"));

        // Everything else is included by default
        assert!(filter.should_include("data.txt"));
        assert!(filter.should_include("readme.md"));
    }

    #[test]
    fn test_filter_order_matters_include_then_exclude() {
        // Reverse order: include important.log, then exclude all logs
        let filter = FileFilter::builder()
            .add_include("important.log")
            .add_exclude("*.log")
            .build()
            .unwrap();

        // important.log matches both, but exclude comes last
        assert!(!filter.should_include("important.log"));

        // Other logs are excluded
        assert!(!filter.should_include("error.log"));

        // Non-matching files are excluded (because includes exist)
        assert!(!filter.should_include("data.txt"));
    }

    #[test]
    fn test_filter_multiple_patterns() {
        let filter = FileFilter::builder()
            .add_include("*.txt")
            .add_include("*.md")
            .add_exclude("*.tmp")
            .add_exclude("build/*")
            .build()
            .unwrap();

        assert!(filter.should_include("readme.txt"));
        assert!(filter.should_include("doc.md"));
        assert!(!filter.should_include("file.tmp"));
        assert!(!filter.should_include("build/output.txt"));
        assert!(!filter.should_include("data.csv")); // Not included
    }

    #[test]
    fn test_filter_complex_aws_pattern() {
        // AWS S3 pattern: exclude everything, then include specific files
        let filter = FileFilter::builder()
            .add_exclude("*")
            .add_include("*.txt")
            .add_include("docs/*")
            .build()
            .unwrap();

        // Included by later rules
        assert!(filter.should_include("readme.txt"));
        assert!(filter.should_include("docs/guide.md"));

        // Excluded by first rule
        assert!(!filter.should_include("script.sh"));
    }

    #[test]
    fn test_filter_directory_patterns() {
        let filter = FileFilter::builder()
            .add_exclude("logs/*")
            .add_exclude("**/temp/*")
            .build()
            .unwrap();

        assert!(filter.should_include("src/main.rs"));
        assert!(!filter.should_include("logs/app.log"));
        assert!(!filter.should_include("logs/debug.log"));
        assert!(!filter.should_include("data/temp/cache.txt"));
        assert!(!filter.should_include("src/temp/build.tmp"));
    }

    #[test]
    fn test_filter_path_normalization() {
        let filter = FileFilter::builder().add_exclude("temp/*").build().unwrap();

        // Forward slashes (Unix-style)
        assert!(!filter.should_include("temp/file.txt"));

        // Backslashes (Windows-style) should be normalized
        assert!(!filter.should_include("temp\\file.txt"));
    }

    #[test]
    fn test_filter_from_file() {
        let mut exclude_file = NamedTempFile::new().unwrap();
        writeln!(exclude_file, "# Comment line").unwrap();
        writeln!(exclude_file, "*.log").unwrap();
        writeln!(exclude_file, "").unwrap(); // Empty line
        writeln!(exclude_file, "temp/*").unwrap();
        exclude_file.flush().unwrap();

        let filter = FileFilter::builder()
            .add_excludes_from_file(exclude_file.path())
            .unwrap()
            .build()
            .unwrap();

        assert!(filter.should_include("file.txt"));
        assert!(!filter.should_include("app.log"));
        assert!(!filter.should_include("temp/cache.dat"));
    }

    #[test]
    fn test_filter_include_from_file() {
        let mut include_file = NamedTempFile::new().unwrap();
        writeln!(include_file, "*.txt").unwrap();
        writeln!(include_file, "# Include markdown").unwrap();
        writeln!(include_file, "*.md").unwrap();
        include_file.flush().unwrap();

        let filter = FileFilter::builder()
            .add_includes_from_file(include_file.path())
            .unwrap()
            .build()
            .unwrap();

        assert!(filter.should_include("readme.txt"));
        assert!(filter.should_include("doc.md"));
        assert!(!filter.should_include("script.sh"));
    }

    #[test]
    fn test_filter_mixed_file_and_inline() {
        let mut exclude_file = NamedTempFile::new().unwrap();
        writeln!(exclude_file, "*.log").unwrap();
        exclude_file.flush().unwrap();

        // Exclude logs from file, then re-include important.log inline
        let filter = FileFilter::builder()
            .add_excludes_from_file(exclude_file.path())
            .unwrap()
            .add_include("important.log")
            .build()
            .unwrap();

        assert!(!filter.should_include("error.log"));
        assert!(filter.should_include("important.log")); // Re-included
    }

    #[test]
    fn test_filter_invalid_pattern() {
        let result = FilterBuilder::default().add_exclude("[invalid").build();

        assert!(result.is_err());
        if let Err(S3O2Error::FilterError(msg)) = result {
            assert!(msg.contains("Invalid pattern"));
        } else {
            panic!("Expected FilterError");
        }
    }

    #[test]
    fn test_filter_last_match_wins() {
        // Demonstrate that the last matching filter determines the result
        let filter = FileFilter::builder()
            .add_include("*.txt")
            .add_exclude("temp/*.txt")
            .add_include("temp/keep.txt")
            .build()
            .unwrap();

        // Basic .txt files are included
        assert!(filter.should_include("readme.txt"));

        // temp/*.txt files are excluded by second rule
        assert!(!filter.should_include("temp/cache.txt"));

        // temp/keep.txt is re-included by third rule
        assert!(filter.should_include("temp/keep.txt"));
    }
}
