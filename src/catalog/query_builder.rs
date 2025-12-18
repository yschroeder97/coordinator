use sqlx::sqlite::SqliteArguments;
use sqlx::Sqlite;
use sqlx::{Arguments, Encode, Type};
use std::fmt;
use std::fmt::{Display, Formatter};
use strum::EnumIter;

pub trait ToSql {
    fn to_sql(&self) -> (String, SqliteArguments);
}

#[derive(Clone, Copy, EnumIter)]
pub enum SqlOperation {
    Insert(&'static str),
    Select(&'static str),
    Delete(&'static str),
    Update(&'static str),
}

impl Display for SqlOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SqlOperation::Insert(tbl) => write!(f, "INSERT INTO {tbl} VALUES"),
            SqlOperation::Select(tbl) => write!(f, "SELECT FROM {tbl}"),
            SqlOperation::Delete(tbl) => write!(f, "DELETE FROM {tbl}"),
            SqlOperation::Update(tbl) => write!(f, "UPDATE {tbl} SET"),
        }
    }
}

/// A specialized Sqlite query builder that supports cloning.
///
/// This is a replacement for `sqlx::QueryBuilder<Sqlite>` with the following advantages:
/// - Supports `Clone` trait for flexibility in usage
/// - Sqlite-specific (no generic type parameters)
/// - Simplified lifetime management
/// - Direct integration with `SqliteArguments`
/// A specialized SQLite query builder that supports cloning.
#[derive(Clone, Debug, Default)]
struct SqliteQueryBuilder {
    sql: String,
    arguments: SqliteArguments<'static>,
}

impl SqliteQueryBuilder {
    /// Creates a new SQLite query builder with the given initial SQL.
    fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            arguments: SqliteArguments::default(),
        }
    }

    /// Appends an SQL fragment to the query.
    fn push(&mut self, sql: impl Display) -> &mut Self {
        self.sql.push_str(&sql.to_string());
        self
    }

    /// Binds a parameter and appends its placeholder (?) to the query.
    fn push_bind<T>(&mut self, value: T) -> &mut Self
    where
        // T must be 'static to be owned by the arguments vector, allowing Clone
        T: 'static + Type<Sqlite> + Encode<'static, Sqlite> + Send,
    {
        // SQLite uses '?' as the placeholder.
        // We add a leading space to ensure safety from the previous fragment.
        self.sql.push_str(" ?");

        // Add the value to arguments
        self.arguments.add(value).expect("Failed to bind parameter");

        self
    }

    /// Returns the current SQL string.
    fn sql(&self) -> &str {
        &self.sql
    }

    /// Destruct into usable parts.
    fn into_parts(self) -> (String, SqliteArguments<'static>) {
        (self.sql, self.arguments)
    }
}

#[derive(Clone, Debug)]
pub struct WhereBuilder {
    builder: SqliteQueryBuilder,
    has_where: bool,
}

impl WhereBuilder {
    /// Creates a new `WhereBuilder` with the given base SQL query.
    ///
    /// The base query should be a complete SQL statement up to but not including
    /// the WHERE clause. The builder will automatically add `WHERE` and subsequent
    /// `AND` conjunctions as conditions are added.
    ///
    /// # Arguments
    ///
    /// * `base_query` - The base SQL query (e.g., "SELECT * FROM users")
    ///
    /// # Examples
    ///
    /// ```rust
    /// use query_builder::WhereBuilder;
    ///
    /// let builder = WhereBuilder::new(SqlOperation::Select, "users");
    /// ```
    pub fn from(op: SqlOperation) -> Self {
        WhereBuilder {
            builder: SqliteQueryBuilder::new(op.to_string()),
            has_where: false,
        }
    }

    /// Internal constructor: continues a query that is already in progress.
    /// Used by UpdateBuilder to hand off the SQL stream.
    pub fn continue_from(builder: SqliteQueryBuilder) -> Self {
        Self {
            builder,
            has_where: false,
        }
    }

    fn add_condition(&mut self, condition: &str) {
        if !self.has_where {
            self.builder.push(" WHERE ");
            self.has_where = true;
        } else {
            self.builder.push(" AND ");
        }
        self.builder.push(condition);
    }

    /// Core conditional method that adds a WHERE/AND condition only if the value is `Some`.
    ///
    /// This is the building block for all other conditional methods. It automatically
    /// handles adding `WHERE` for the first condition and `AND` for subsequent conditions.
    ///
    /// # Arguments
    ///
    /// * `column` - The column name to compare
    /// * `op` - The SQL operator (e.g., "=", ">", "LIKE")
    /// * `value` - Optional value to bind. Condition is only added if `Some`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use query_builder::WhereBuilder;
    ///
    /// let query = WhereBuilder::new("SELECT * FROM users")
    ///     .and_if("age", ">", Some(18))
    ///     .and_if("status", "=", None::<String>) // This won't add anything
    ///     .into_parts();
    /// ```
    pub fn and_if<T>(mut self, column: &str, op: &str, value: Option<T>) -> Self
    where
        T: Send + sqlx::Type<Sqlite> + sqlx::Encode<'static, Sqlite> + 'static,
    {
        if let Some(val) = value {
            self.add_condition(&format!("{column} {op}"));
            self.builder.push_bind(val);
        }
        self
    }

    /// Adds an equality condition (`column = value`) if value is `Some`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use query_builder::WhereBuilder;
    ///
    /// let query = WhereBuilder::new("SELECT * FROM users")
    ///     .eq("status", Some("active"))
    ///     .into_parts();
    /// // Generates: SELECT * FROM users WHERE status = $1
    /// ```
    pub fn eq<T>(self, column: &str, value: Option<T>) -> Self
    where
        T: Send + sqlx::Type<Sqlite> + sqlx::Encode<'static, Sqlite> + 'static,
    {
        self.and_if(column, "=", value)
    }

    /// Returns the generated SQL string for debugging purposes.
    ///
    /// This is useful for inspecting the generated query without executing it.
    /// Parameter placeholders (`$1`, `$2`, etc.) will be visible in the output.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use query_builder::WhereBuilder;
    ///
    /// let builder = WhereBuilder::new("SELECT * FROM users")
    ///     .eq("status", Some("active"));
    ///
    /// println!("{}", builder.to_sql());
    /// // Prints: SELECT * FROM users WHERE status = $1
    /// ```
    pub fn to_sql(&self) -> String {
        self.builder.sql().to_string()
    }

    /// Consumes the builder and returns the underlying SQL query and arguments.
    ///
    /// This is the final step that allows you to execute the query using SQLx methods
    /// like `fetch_all()`, `fetch_one()`, etc.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use query_builder::WhereBuilder;
    ///
    /// let (sql, arguments) = WhereBuilder::new("SELECT * FROM users")
    ///     .eq("status", Some("active"))
    ///     .into_parts();
    ///
    /// // Now you can execute with SQLx:
    /// // let users = sqlx::query_with(&sql, arguments).fetch_all(&pool).await?;
    /// ```
    pub fn into_parts(self) -> (String, SqliteArguments<'static>) {
        self.builder.into_parts()
    }
}

#[derive(Clone, Debug)]
pub struct UpdateBuilder {
    builder: SqliteQueryBuilder,
    first_set: bool,
    in_where_clause: bool,
}

impl UpdateBuilder {
    /// Starts a new UPDATE query: "UPDATE <table> SET"
    pub fn from(table: &str) -> Self {
        let builder = SqliteQueryBuilder::new(format!("UPDATE {} SET", table));
        Self {
            builder,
            first_set: true,
            in_where_clause: false,
        }
    }

    /// Adds a column to the SET clause: "col = val"
    pub fn set<T>(mut self, col: &str, value: T) -> Self
    where
        T: 'static + Type<Sqlite> + Encode<'static, Sqlite> + Send,
    {
        if self.in_where_clause {
            panic!("Cannot call .set() after starting the WHERE clause");
        }

        // Handle the comma separation
        if !self.first_set {
            self.builder.push(", ");
        }
        self.first_set = false;

        // Append " col = ?"
        self.builder.push(format!(" {} =", col));
        self.builder.push_bind(value);

        self
    }

    /// Helper: Only sets the field if the Option is Some(value).
    /// Useful for PATCH endpoints where None means "do not change".
    pub fn set_if_some<T>(self, col: &str, value: Option<T>) -> Self
    where
        T: 'static + Type<Sqlite> + Encode<'static, Sqlite> + Send,
    {
        if let Some(v) = value {
            self.set(col, v)
        } else {
            self
        }
    }

    /// Starts or continues the WHERE clause.
    pub fn filter<T>(mut self, col: &str, value: T) -> Self
    where
        T: 'static + Type<Sqlite> + Encode<'static, Sqlite> + Send,
    {
        if !self.in_where_clause {
            self.builder.push(" WHERE");
            self.in_where_clause = true;
        } else {
            self.builder.push(" AND");
        }

        self.builder.push(format!(" {} =", col));
        self.builder.push_bind(value);

        self
    }

    /// Consumes the builder and returns the SQL and Arguments
    pub fn into_parts(self) -> (String, SqliteArguments<'static>) {
        if self.first_set {
            panic!("Generated an UPDATE statement with no SET clauses!");
        }
        self.builder.into_parts()
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::query_builder::{SqlOperation, WhereBuilder};
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;
    use sqlx::Arguments;

    #[test]
    fn where_builder_no_where() {
        let sql = WhereBuilder::from(SqlOperation::Select("table")).to_sql();

        assert_eq!(sql, "SELECT FROM table");
    }

    #[test]
    fn where_builder_select_one() {
        let sql = WhereBuilder::from(SqlOperation::Select("table"))
            .eq::<String>("col1", Some("value".into()))
            .to_sql();

        assert_eq!(sql, "SELECT FROM table WHERE col1 = ?");
    }

    #[test]
    fn where_builder_select_two() {
        let sql = WhereBuilder::from(SqlOperation::Select("table"))
            .eq::<String>("col1", Some("value".into()))
            .eq::<String>("col2", Some("another".into()))
            .to_sql();

        assert_eq!(sql, "SELECT FROM table WHERE col1 = ? AND col2 = ?");
    }

    impl Arbitrary for SqlOperation {
        fn arbitrary(g: &mut Gen) -> Self {
            use strum::IntoEnumIterator;
            let variants: Vec<SqlOperation> = SqlOperation::iter().collect();
            *g.choose(&variants).expect("choose value")
        }
    }

    #[derive(Clone, Debug)]
    struct WhereBuilderInput {
        builder: WhereBuilder,
        conditions: Vec<(String, String)>,
    }

    impl Arbitrary for WhereBuilderInput {
        fn arbitrary(g: &mut Gen) -> Self {
            let size = usize::arbitrary(g) % 10;
            let conditions: Vec<(String, String)> = (0..size)
                .map(|_| (String::arbitrary(g), String::arbitrary(g)))
                .collect();

            let builder = conditions.iter().fold(
                WhereBuilder::from(SqlOperation::arbitrary(g)),
                |builder, (col, val)| builder.eq(col, Some(val.clone())),
            );

            WhereBuilderInput {
                builder,
                conditions,
            }
        }
    }

    #[quickcheck]
    fn where_builder_all_values_bound(input: WhereBuilderInput) {
        let (_, args) = input.builder.into_parts();
        assert_eq!(input.conditions.len(), args.len());
    }

    #[quickcheck]
    fn where_builder_correctly_bound(input: WhereBuilderInput) {
        let (sql, args) = input.builder.into_parts();
        assert!(input.conditions.iter().all(|(col, _)| sql.contains(col)));
    }
}
