//! Defining a table (contract §4.11).
//!
//! The one definition the builder carries: a table's name, the columns it
//! declares, and the storage layout clause. Definitions name things and hold
//! no values, so nothing binds — the partition count is the statement's
//! shape, like `START` and `LIMIT`, and arrives as an integer.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::query::{BuildError, Query, check_name};

/// The layout a table's records are projected into (contract §4.11).
///
/// An enumeration rather than a string for the reason [`crate::query::Answerer`]
/// is: the refusal is unrepresentable for a caller who writes Rust, and
/// [`TryFrom`] is the one door a string comes through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    /// `STORE ROW` — the default said out loud.
    Row,
    /// `STORE COLUMN` — a columnar projection beside the row authority.
    Column,
}

impl StoreKind {
    /// The clause word, as written.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::Row => "ROW",
            Self::Column => "COLUMN",
        }
    }
}

impl TryFrom<&str> for StoreKind {
    type Error = BuildError;

    fn try_from(word: &str) -> Result<Self, Self::Error> {
        match word {
            "row" => Ok(Self::Row),
            "column" => Ok(Self::Column),
            other => Err(BuildError::NotAStoreKind {
                name: other.to_owned(),
            }),
        }
    }
}

/// `DEFINE TABLE <name> (<columns>) [STORE ROW|COLUMN] [PARTITIONS n BY field]`.
///
/// ```
/// use tessaridb_client::query::DefineTable;
///
/// let query = DefineTable::named("events")
///     .column("kind", "string")
///     .column("seq", "int")
///     .store_column()
///     .partitions(4, "kind")
///     .and_then(|define| define.build())
///     .expect("a well-formed definition");
///
/// assert_eq!(
///     query.script,
///     "DEFINE TABLE events (kind string, seq int) STORE COLUMN PARTITIONS 4 BY kind;"
/// );
/// assert!(query.parameters.is_empty());
/// ```
#[derive(Debug, Clone)]
pub struct DefineTable {
    table: String,
    columns: BTreeMap<String, String>,
    store: Option<StoreKind>,
    partitions: Option<(u64, String)>,
}

impl DefineTable {
    /// A table of this name.
    #[must_use]
    pub fn named(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            columns: BTreeMap::new(),
            store: None,
            partitions: None,
        }
    }

    /// One declared column: the field's name and its type word, as the
    /// definition spells them. Columns render in the contract's byte order
    /// (§4.8), so call order is not the rendered order.
    #[must_use]
    pub fn column(mut self, name: impl Into<String>, kind: impl Into<String>) -> Self {
        self.columns.insert(name.into(), kind.into());
        self
    }

    /// `STORE ROW`.
    #[must_use]
    pub fn store_row(self) -> Self {
        self.store_kind(StoreKind::Row)
    }

    /// `STORE COLUMN` — the columnar projection clause (tessariql.md §4b).
    #[must_use]
    pub fn store_column(self) -> Self {
        self.store_kind(StoreKind::Column)
    }

    /// The layout from a caller's word — `row` or `column` — checked here,
    /// because a string that arrived from outside is exactly what the
    /// enumeration exists to check.
    ///
    /// # Errors
    ///
    /// [`BuildError::NotAStoreKind`] when the word is neither.
    pub fn store(self, word: &str) -> Result<Self, BuildError> {
        Ok(self.store_kind(StoreKind::try_from(word)?))
    }

    fn store_kind(mut self, kind: StoreKind) -> Self {
        self.store = Some(kind);
        self
    }

    /// `PARTITIONS <count> BY <field>` — the hash partitioning of a columnar
    /// projection. Refused at [`build`](Self::build) beside any store other
    /// than `column`, and here when the count is not a whole number of at
    /// least one.
    ///
    /// # Errors
    ///
    /// [`BuildError::NotAPartitionCount`] when `count` is zero.
    pub fn partitions(mut self, count: u64, by: impl Into<String>) -> Result<Self, BuildError> {
        if count == 0 {
            return Err(BuildError::NotAPartitionCount {
                name: count.to_string(),
            });
        }
        self.partitions = Some((count, by.into()));
        Ok(self)
    }

    /// Render the statement. A definition binds nothing: every position
    /// names a thing, and the count is the statement's shape.
    ///
    /// # Errors
    ///
    /// [`BuildError::NotAName`] for a table, column or field name the grammar
    /// would not read; [`BuildError::PartitionsWithoutStore`] for
    /// `partitions` beside no `store` or a `row` one.
    pub fn build(&self) -> Result<Query, BuildError> {
        check_name("a table", &self.table)?;
        let mut columns = Vec::with_capacity(self.columns.len());
        for (name, kind) in &self.columns {
            check_name("a field", name)?;
            columns.push(format!("{name} {kind}"));
        }
        let mut script = format!("DEFINE TABLE {} ({})", self.table, columns.join(", "));
        if let Some(store) = self.store {
            script.push_str(" STORE ");
            script.push_str(store.word());
        }
        if let Some((count, by)) = &self.partitions {
            if self.store != Some(StoreKind::Column) {
                return Err(BuildError::PartitionsWithoutStore);
            }
            check_name("a field", by)?;
            let _ = write!(script, " PARTITIONS {count} BY {by}");
        }
        Ok(Query {
            script: format!("{script};"),
            parameters: crate::query::Parameters::new(),
        })
    }
}
