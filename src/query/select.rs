//! Reading.

use crate::query::{Answerer, Binder, BuildError, Filter, Query, check_name, check_span};

/// Which way an ordering runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    /// Smallest first. The node's default, and written out anyway so the
    /// statement says what it does.
    Ascending,
    /// Largest first.
    Descending,
}

impl Order {
    const fn spelled(self) -> &'static str {
        match self {
            Self::Ascending => "ASC",
            Self::Descending => "DESC",
        }
    }
}

/// One entry of a projection (contract §4.2).
///
/// A bare field name, or a window onto a long text field. It is an enum rather
/// than two parallel lists because §4.2 renders the items **in the order they
/// were named**, and two lists cannot express one order.
#[derive(Debug, Clone)]
enum Projection {
    Field(String),
    Lines {
        field: String,
        start: u64,
        count: u64,
    },
}

impl Projection {
    /// The field this item reads, which is name-checked either way.
    const fn field(&self) -> &String {
        match self {
            Self::Field(name) | Self::Lines { field: name, .. } => name,
        }
    }

    fn render(&self) -> String {
        match self {
            Self::Field(name) => name.clone(),
            Self::Lines {
                field,
                start,
                count,
            } => format!("string::lines({field}, {start}, {count}) AS {field}"),
        }
    }
}

/// A read.
///
/// ```
/// use tessaridb_client::query::{Order, Select, field};
///
/// let query = Select::from("memories")
///     .filter(field("session").eq("abc"))
///     .order_by("created", Order::Descending)
///     .limit(50)
///     .build()
///     .expect("a well-formed query");
///
/// assert!(query.script.starts_with("SELECT * FROM memories WHERE"));
/// // The value is bound, not written into the text.
/// assert!(!query.script.contains("abc"));
/// ```
#[derive(Debug, Clone)]
pub struct Select {
    source: String,
    projection: Vec<Projection>,
    filter: Option<Filter>,
    order: Vec<(String, Order)>,
    start: Option<u64>,
    limit: Option<u64>,
    staleness: Option<String>,
    answered_by: Option<Answerer>,
}

impl Select {
    /// Read from a table.
    #[must_use]
    pub fn from(table: impl Into<String>) -> Self {
        Self {
            source: table.into(),
            projection: Vec::new(),
            filter: None,
            order: Vec::new(),
            start: None,
            limit: None,
            staleness: None,
            answered_by: None,
        }
    }

    /// Name a field to return.
    ///
    /// With none named the statement projects everything.
    #[must_use]
    pub fn field(mut self, name: impl Into<String>) -> Self {
        self.projection.push(Projection::Field(name.into()));
        self
    }

    /// Return `count` lines of a text field, starting at line `start`.
    ///
    /// For reading a long body a piece at a time rather than whole. Lines are
    /// counted from zero, and two adjacent windows reconstruct the field, so a
    /// caller pages through a document without ever holding all of it. The
    /// field comes back under **its own name**, so the mapping that reads it
    /// does not change with the window.
    ///
    /// The two counts are part of the statement's text rather than parameters —
    /// §4.2's rule for `START` and `LIMIT`, for its reason: they are `u64` from
    /// the type system, so there is no syntax to smuggle through them.
    ///
    /// ```
    /// use tessaridb_client::query::Select;
    ///
    /// let query = Select::from("memories")
    ///     .field_lines("body", 0, 40)
    ///     .build()
    ///     .expect("a well-formed query");
    ///
    /// assert_eq!(
    ///     query.script,
    ///     "SELECT string::lines(body, 0, 40) AS body FROM memories;"
    /// );
    /// ```
    #[must_use]
    pub fn field_lines(mut self, name: impl Into<String>, start: u64, count: u64) -> Self {
        self.projection.push(Projection::Lines {
            field: name.into(),
            start,
            count,
        });
        self
    }

    /// Keep only records the condition holds for.
    ///
    /// Calling this twice replaces the condition rather than combining — use
    /// [`Filter::and`] to say what you mean, because a builder that silently
    /// `AND`ed two filters would make a duplicated call look like it worked.
    #[must_use]
    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Order by a field. Called more than once, orders by each in turn.
    #[must_use]
    pub fn order_by(mut self, name: impl Into<String>, order: Order) -> Self {
        self.order.push((name.into(), order));
        self
    }

    /// Skip this many records.
    #[must_use]
    pub const fn start(mut self, at: u64) -> Self {
        self.start = Some(at);
        self
    }

    /// Return at most this many.
    #[must_use]
    pub const fn limit(mut self, count: u64) -> Self {
        self.limit = Some(count);
        self
    }

    /// How far behind the node answering this read may be — `"30s"`, `"1m30s"`.
    ///
    /// A candidate filter and never a marker: it says which nodes may answer at
    /// all, rather than labelling an answer as stale. A read no node can satisfy
    /// is refused by the node rather than quietly promoted to the one that can.
    ///
    /// A string rather than a [`Duration`](std::time::Duration) because the text
    /// is the contract: `1m30s` and `90s` are the same length and different
    /// statements, and a client that normalised one into the other would render
    /// something the shared corpus does not carry.
    #[must_use]
    pub fn staleness(mut self, bound: impl Into<String>) -> Self {
        self.staleness = Some(bound.into());
        self
    }

    /// Where the answer must come from.
    ///
    /// Not a tighter [`staleness`](Self::staleness): a follower at zero lag is
    /// *level*, not authoritative, so no freshness bound expresses *this must
    /// come from where writes are decided*.
    #[must_use]
    pub const fn answered_by(mut self, answerer: Answerer) -> Self {
        self.answered_by = Some(answerer);
        self
    }

    /// Render the statement and its parameters.
    pub fn build(&self) -> Result<Query, BuildError> {
        check_name("a table", &self.source)?;
        let mut binder = Binder::new();

        let projection = if self.projection.is_empty() {
            "*".to_owned()
        } else {
            for item in &self.projection {
                check_name("a field", item.field())?;
            }
            self.projection
                .iter()
                .map(Projection::render)
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut script = format!("SELECT {projection} FROM {}", self.source);

        if let Some(filter) = &self.filter {
            let rendered = filter.render(&mut binder)?;
            script.push_str(" WHERE ");
            script.push_str(&rendered);
        }

        if !self.order.is_empty() {
            let mut parts = Vec::with_capacity(self.order.len());
            for (name, order) in &self.order {
                check_name("a field", name)?;
                parts.push(format!("{name} {}", order.spelled()));
            }
            script.push_str(" ORDER BY ");
            script.push_str(&parts.join(", "));
        }

        // A count is a literal, not a value: it is part of the statement's shape
        // and cannot be supplied by a parameter. It is a `u64` from the type
        // system, so there is nothing here a caller could smuggle syntax through.
        if let Some(at) = self.start {
            script.push_str(" START ");
            script.push_str(&at.to_string());
        }
        if let Some(count) = self.limit {
            script.push_str(" LIMIT ");
            script.push_str(&count.to_string());
        }

        // Both come after `LIMIT`, and `STALENESS` before `ANSWERED BY`, because
        // a node's parser accepts no other sequence.
        if let Some(bound) = &self.staleness {
            check_span(bound)?;
            script.push_str(" STALENESS ");
            script.push_str(bound);
        }
        if let Some(answerer) = self.answered_by {
            script.push_str(" ANSWERED BY ");
            script.push_str(answerer.spelled());
        }

        script.push(';');
        Ok(Query {
            script,
            parameters: binder.finish(),
        })
    }
}
