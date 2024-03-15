use bb8_postgres::{
    bb8::{Pool, PooledConnection},
    PostgresConnectionManager,
};
use futures::future::BoxFuture;
use tokio_postgres::{NoTls, Transaction};

use crate::{contexts::migrate_database, util::config::get_config_element};

use super::Migration;

type Result<T, E = tokio_postgres::Error> = std::result::Result<T, E>;

#[derive(Debug, PartialEq)]
pub struct SchemaInfo {
    pub tables: Vec<String>,
    pub columns: Vec<SchemaColumn>,
    pub views: Vec<SchemaView>,

    // composite types, domains, udts
    pub attributes: Vec<SchemaAttribute>,
    pub domains: Vec<SchemaDomain>,
    pub user_defined_types: Vec<SchemaUserDefinedTypes>,

    // arrays
    pub column_arrays: Vec<SchemaColumnArrays>,
    pub composite_arrays: Vec<SchemaCompositeArrays>,
    pub domain_arrays: Vec<SchemaDomainArrays>,

    // enums
    pub enums: Vec<SchemaEnum>,

    // functions
    pub parameters: Vec<SchemaParameters>,

    // constraints
    pub domain_check_constraints: Vec<SchemaDomainCheckConstraint>,
    pub table_key_constraints: Vec<SchemaTableKeyConstraint>,
    pub table_referential_constraints: Vec<SchemaTableReferentialConstraint>,
    pub table_check_constraints: Vec<SchemaTableCheckConstraint>,
}

pub async fn schema_info_from_information_schema(
    transaction: Transaction<'_>,
    schema_name: &str,
) -> Result<SchemaInfo> {
    Ok(SchemaInfo {
        tables: tables(&transaction, schema_name).await?,
        columns: columns(&transaction, schema_name).await?,
        views: views(&transaction, schema_name).await?,
        attributes: attributes(&transaction, schema_name).await?,
        domains: domains(&transaction, schema_name).await?,
        user_defined_types: user_defined_types(&transaction, schema_name).await?,
        column_arrays: schema_column_arrays(&transaction, schema_name).await?,
        composite_arrays: schema_composite_arrays(&transaction, schema_name).await?,
        domain_arrays: schema_domain_arrays(&transaction, schema_name).await?,
        enums: schema_enums(&transaction, schema_name).await?,
        parameters: parameters(&transaction, schema_name).await?,
        domain_check_constraints: domain_constraints(&transaction, schema_name).await?,
        table_key_constraints: key_column_usage(&transaction, schema_name).await?,
        table_check_constraints: table_constraints(&transaction, schema_name).await?,
        table_referential_constraints: referential_constraints(&transaction, schema_name).await?,
    })
}

macro_rules! schema_info_table {
    (
        $type_name:ident,
        $table_name:ident,
        $(from = $join:literal,)?
        $schema_column:literal,
        $order:literal,
        $(($field_name:ident, $field_type:ty)),+
    ) => {
        #[derive(Debug, PartialEq)]
        #[allow(clippy::struct_field_names)] // use postgres names
        pub struct $type_name {
            $($field_name: $field_type),+
        }

        async fn $table_name (transaction: &Transaction<'_>, schema_name: &str) -> Result<Vec<$type_name>> {
            Ok(transaction
                .query(
                    &format!("
                        SELECT
                            {columns}
                        FROM
                            {from_str}
                        WHERE
                            {schema_column} = $1
                        ORDER BY {order_by}
                    ",
                    columns = stringify!($($field_name),+),
                    from_str = schema_info_table!(@from_str $table_name $($join)?),
                    schema_column = $schema_column,
                    order_by = $order,
                    ),
                    &[&schema_name],
                )
                .await?
                .iter()
                .map(|row| $type_name {
                    $($field_name: row.get(stringify!($field_name))),+
                })
                .collect::<Vec<$type_name>>())
        }
    };

    (@from_str $table_name:ident) => {concat!("information_schema.", stringify!($table_name))};
    (@from_str $table_name:ident $str:literal) => {$str};
}

schema_info_table!(
    SchemaColumn,
    columns,
    "table_schema",
    "table_name ASC, ordinal_position ASC",
    (table_name, String),
    (column_name, String),
    (ordinal_position, i32),
    (column_default, Option<String>),
    (is_nullable, String),
    (data_type, String),
    (character_maximum_length, Option<i32>),
    (character_octet_length, Option<i32>),
    (numeric_precision, Option<i32>),
    (numeric_precision_radix, Option<i32>),
    (numeric_scale, Option<i32>),
    (datetime_precision, Option<i32>),
    (interval_type, Option<String>),
    (interval_precision, Option<i32>)
);

schema_info_table!(
    SchemaColumnArrays,
    schema_column_arrays,
    from = "
        information_schema.element_types e JOIN (
            SELECT
                table_catalog,
                table_schema,
                table_name,
                column_name,
                ordinal_position,
                dtd_identifier
            FROM information_schema.columns
        ) c ON (
            (c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier)
          = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier)
        )
    ",
    "table_schema",
    "table_name ASC, ordinal_position ASC",
    (table_name, String),
    (column_name, String),
    (ordinal_position, i32),
    (object_type, String),
    (data_type, String),
    (character_maximum_length, Option<i32>),
    (character_octet_length, Option<i32>),
    (numeric_precision, Option<i32>),
    (numeric_precision_radix, Option<i32>),
    (numeric_scale, Option<i32>),
    (datetime_precision, Option<i32>),
    (interval_type, Option<String>),
    (interval_precision, Option<i32>),
    (udt_name, String)
);

schema_info_table!(
    SchemaDomainArrays,
    schema_domain_arrays,
    from = "
        information_schema.element_types e JOIN (
            SELECT
                domain_catalog as the_domain_catalog,
                domain_schema as the_domain_schema,
                domain_name as the_domain_name,
                dtd_identifier
            FROM information_schema.domains
        ) d ON (
            (d.the_domain_catalog, d.the_domain_schema, d.the_domain_name, 'DOMAIN', d.dtd_identifier)
          = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier)
        )
    ",
    "the_domain_schema",
    "the_domain_name ASC",
    (the_domain_name, String),
    (object_type, String),
    (data_type, String),
    (character_maximum_length, Option<i32>),
    (character_octet_length, Option<i32>),
    (numeric_precision, Option<i32>),
    (numeric_precision_radix, Option<i32>),
    (numeric_scale, Option<i32>),
    (datetime_precision, Option<i32>),
    (interval_type, Option<String>),
    (interval_precision, Option<i32>),
    (udt_name, String)
);

schema_info_table!(
    SchemaCompositeArrays,
    schema_composite_arrays,
    from = "
        information_schema.element_types e JOIN (
            SELECT
                udt_catalog as the_udt_catalog,
                udt_schema as the_udt_schema,
                udt_name as the_udt_name,
                attribute_name,
                ordinal_position,
                dtd_identifier
            FROM information_schema.attributes
        ) a ON (
            (a.the_udt_catalog, a.the_udt_schema, a.the_udt_name, 'USER-DEFINED TYPE', a.dtd_identifier)
          = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier)
        )
    ",
    "the_udt_schema",
    "the_udt_name ASC, ordinal_position ASC",
    (the_udt_name, String),
    (attribute_name, String),
    (object_type, String),
    (data_type, String),
    (character_maximum_length, Option<i32>),
    (character_octet_length, Option<i32>),
    (numeric_precision, Option<i32>),
    (numeric_precision_radix, Option<i32>),
    (numeric_scale, Option<i32>),
    (datetime_precision, Option<i32>),
    (interval_type, Option<String>),
    (interval_precision, Option<i32>),
    (udt_name, String)
);

schema_info_table!(
    SchemaAttribute,
    attributes,
    "udt_schema",
    "udt_name ASC, ordinal_position ASC, attribute_name ASC",
    (udt_name, String),
    (attribute_name, String),
    // (ordinal_position, i32), // we cannot store them, as gaps remain after drops
    (attribute_default, Option<String>),
    (is_nullable, String),
    (data_type, String),
    (character_maximum_length, Option<i32>),
    (character_octet_length, Option<i32>),
    (numeric_precision, Option<i32>),
    (numeric_precision_radix, Option<i32>),
    (numeric_scale, Option<i32>),
    (datetime_precision, Option<i32>),
    (interval_type, Option<String>),
    (interval_precision, Option<i32>)
);

schema_info_table!(
    SchemaView,
    views,
    "table_schema",
    "table_name ASC",
    (table_name, String),
    (view_definition, String),
    (is_updatable, String),
    (is_insertable_into, String),
    (is_trigger_updatable, String),
    (is_trigger_deletable, String),
    (is_trigger_insertable_into, String)
);

schema_info_table!(
    SchemaDomainCheckConstraint,
    domain_constraints,
    from =
        "information_schema.domain_constraints NATURAL JOIN information_schema.check_constraints",
    "constraint_schema",
    "domain_name ASC, check_clause ASC",
    (domain_name, String),
    (check_clause, String),
    (is_deferrable, String),
    (initially_deferred, String)
);

schema_info_table!(
    SchemaTableCheckConstraint,
    table_constraints,
    from = "information_schema.table_constraints NATURAL JOIN information_schema.check_constraints",
    "table_schema",
    "table_name ASC, check_clause ASC",
    (table_name, String),
    (check_clause, String),
    (constraint_type, String),
    (is_deferrable, String),
    (initially_deferred, String)
);

schema_info_table!(
    SchemaTableReferentialConstraint,
    referential_constraints,
    from = "information_schema.referential_constraints NATURAL JOIN information_schema.table_constraints",
    "constraint_schema",
    "table_name ASC, unique_constraint_name ASC, constraint_name ASC",
    (table_name, String),
    (constraint_name, String),
    (unique_constraint_name, String),
    (match_option, String),
    (update_rule, String),
    (delete_rule, String),
    (constraint_type, String),
    (is_deferrable, String),
    (initially_deferred, String)
);

schema_info_table!(
    SchemaTableKeyConstraint,
    key_column_usage,
    from = "information_schema.key_column_usage NATURAL JOIN information_schema.table_constraints",
    "table_schema",
    "table_name ASC, constraint_name ASC, ordinal_position ASC, column_name ASC",
    (constraint_name, String),
    (table_name, String),
    (column_name, String),
    (constraint_type, String),
    (position_in_unique_constraint, Option<i32>),
    (is_deferrable, String),
    (initially_deferred, String)
);

schema_info_table!(
    SchemaUserDefinedTypes,
    user_defined_types,
    "user_defined_type_schema",
    "user_defined_type_name ASC",
    (user_defined_type_name, String)
);

schema_info_table!(
    SchemaParameters,
    parameters,
    "specific_schema",
    "specific_name, ordinal_position ASC",
    (specific_name, String),
    (ordinal_position, i32),
    (parameter_mode, String),
    (parameter_name, String),
    (data_type, String),
    (character_maximum_length, Option<i32>),
    (character_octet_length, Option<i32>),
    (numeric_precision, Option<i32>),
    (numeric_precision_radix, Option<i32>),
    (numeric_scale, Option<i32>),
    (datetime_precision, Option<i32>),
    (interval_type, Option<String>),
    (interval_precision, Option<i32>),
    (udt_name, String),
    (parameter_default, Option<String>)
);

schema_info_table!(
    SchemaDomain,
    domains,
    "domain_schema",
    "domain_name, udt_name ASC",
    (domain_name, String),
    (data_type, String),
    (character_maximum_length, Option<i32>),
    (character_octet_length, Option<i32>),
    (numeric_precision, Option<i32>),
    (numeric_precision_radix, Option<i32>),
    (numeric_scale, Option<i32>),
    (datetime_precision, Option<i32>),
    (interval_type, Option<String>),
    (interval_precision, Option<i32>),
    (domain_default, Option<String>),
    (udt_name, String)
);

async fn tables(transaction: &Transaction<'_>, schema_name: &str) -> Result<Vec<String>> {
    Ok(transaction
        .query(
            "
            SELECT
                table_name
            FROM
                information_schema.tables
            WHERE
                table_schema = $1
            ORDER BY table_name ASC",
            &[&schema_name],
        )
        .await?
        .iter()
        .map(|row| row.get("table_name"))
        .collect::<Vec<String>>())
}

schema_info_table!(
    SchemaEnum,
    schema_enums,
    from = "(
        SELECT
            n.nspname as enum_schema, -- namespace = schema
            t.typname AS type_name,
            array_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_labels
        FROM pg_catalog.pg_type t
        JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid)
        JOIN pg_catalog.pg_enum e ON (t.oid = e.enumtypid)
        WHERE
            t.typtype = 'e' -- enums
        GROUP BY n.nspname, t.typname
    ) enums",
    "enum_schema",
    "type_name ASC",
    (type_name, String),
    (enum_labels, Vec<String>)
);

/// Asserts that the schema after applying the migrations is equal to the ground truth schema.
///
/// The ground truth schema is defined by the given SQL string, containing the schema definition.
///
/// Cf. [`assert_schema_eq`] for more information.
///
pub async fn assert_migration_schema_eq(
    migrations: &[Box<dyn Migration>],
    ground_truth_schema_definition: &str,
    population_config: AssertSchemaEqPopulationConfig,
) {
    assert_schema_eq(
        |mut connection| {
            Box::pin(async move {
                migrate_database(&mut connection, migrations, None)
                    .await
                    .unwrap();

                connection
            })
        },
        |connection| {
            Box::pin(async move {
                connection
                    .batch_execute(ground_truth_schema_definition)
                    .await
                    .unwrap();

                connection
            })
        },
        population_config,
    )
    .await;
}

/// Asserts that two schemata, gathered by two transactions and their `CURRENT_SCHEMA`, are equal.
///
/// The check is not exhaustive, but it is a good start to ensure that the schemas are in fact equal.
///
/// What is covered:
/// - table names
/// - column names, types and constraints
/// - view names and definitions
/// - domains and types
/// - some properties of user defined types
/// - some properties of functions and parameters
/// - key constraints, referential constraints, and table check constraints
///
/// What is not covered:
/// - schema names
/// - triggers
/// - permissions and privileges
/// - sequences
/// - collations
/// - foreign tables
/// - (other things that are not mentioned above)
///
/// It gathers this information from the `information_schema`.
/// This is described here for PostgreSQL: <https://www.postgresql.org/docs/current/information-schema.html>
///
/// There are some sanity checks involved, such as checking if the tables, columns, views, etc. are not empty.
/// Uses [`AssertSchemaEqPopulationConfig`] to control which fields are checked for non-emptiness.
///
async fn assert_schema_eq<'c>(
    f1: impl FnOnce(
        PooledConnection<'static, PostgresConnectionManager<NoTls>>,
    ) -> BoxFuture<'c, PooledConnection<'static, PostgresConnectionManager<NoTls>>>,
    f2: impl FnOnce(
        PooledConnection<'static, PostgresConnectionManager<NoTls>>,
    ) -> BoxFuture<'c, PooledConnection<'static, PostgresConnectionManager<NoTls>>>,
    population_config: AssertSchemaEqPopulationConfig,
) {
    async fn get_pool() -> Pool<PostgresConnectionManager<NoTls>> {
        let postgres_config = get_config_element::<crate::util::config::Postgres>().unwrap();
        let pg_mgr = PostgresConnectionManager::new(postgres_config.try_into().unwrap(), NoTls);

        Pool::builder().max_size(1).build(pg_mgr).await.unwrap()
    }

    async fn get_schema(transaction: Transaction<'_>) -> SchemaInfo {
        let schema = transaction
            .query_one("SELECT current_schema", &[])
            .await
            .unwrap()
            .get::<_, String>("current_schema");

        schema_info_from_information_schema(transaction, &schema)
            .await
            .unwrap()
    }

    let schema_a = {
        let pool = get_pool().await;
        let mut connection = pool.get_owned().await.unwrap();
        connection = f1(connection).await;
        get_schema(
            connection
                .build_transaction()
                .read_only(true)
                .start()
                .await
                .unwrap(),
        )
        .await
    };

    let schema_b = {
        let pool = get_pool().await;
        let mut connection = pool.get_owned().await.unwrap();
        connection = f2(connection).await;
        get_schema(
            connection
                .build_transaction()
                .read_only(true)
                .start()
                .await
                .unwrap(),
        )
        .await
    };

    assert_schema_info_eq(&schema_a, &schema_b);

    assert_schema_info_population(&schema_a, population_config);
}

/// Compares the schemas field by field.
fn assert_schema_info_eq(schema_a: &SchemaInfo, schema_b: &SchemaInfo) {
    pretty_assertions::assert_eq!(schema_a.tables, schema_b.tables, "Schema tables differ");
    pretty_assertions::assert_eq!(schema_a.columns, schema_b.columns, "Table columns differ");
    pretty_assertions::assert_eq!(schema_a.views, schema_b.views, "Schema views differ");

    pretty_assertions::assert_eq!(
        schema_a.attributes,
        schema_b.attributes,
        "Schemas have different composite types"
    );
    pretty_assertions::assert_eq!(
        schema_a.domains,
        schema_b.domains,
        "Schemas have different domains"
    );

    pretty_assertions::assert_eq!(
        schema_a.user_defined_types,
        schema_b.user_defined_types,
        "Schemas have different user defined types"
    );
    pretty_assertions::assert_eq!(
        schema_a.parameters,
        schema_b.parameters,
        "Schemas have different functions"
    );

    pretty_assertions::assert_eq!(
        schema_a.column_arrays,
        schema_b.column_arrays,
        "Columns with arrays differ"
    );
    pretty_assertions::assert_eq!(
        schema_a.composite_arrays,
        schema_b.composite_arrays,
        "Composite types with arrays differ"
    );
    pretty_assertions::assert_eq!(
        schema_a.domain_arrays,
        schema_b.domain_arrays,
        "Domains with arrays differ"
    );

    pretty_assertions::assert_eq!(
        schema_a.enums,
        schema_b.enums,
        "Schemas have different enums"
    );

    // check constraints…

    pretty_assertions::assert_eq!(
        schema_a.table_key_constraints,
        schema_b.table_key_constraints,
        "Schemas have different table key constraints"
    );
    pretty_assertions::assert_eq!(
        schema_a.table_referential_constraints,
        schema_b.table_referential_constraints,
        "Schemas have different table referential constraints"
    );
    pretty_assertions::assert_eq!(
        schema_a.table_check_constraints,
        schema_b.table_check_constraints,
        "Schemas have different table check constraints"
    );

    pretty_assertions::assert_eq!(
        schema_a.domain_check_constraints,
        schema_b.domain_check_constraints,
        "Schemas have different domain check constraints"
    );
}

/// If an attribute is set to `true`, the corresponding field is checked for non-emptiness.
#[allow(
    clippy::struct_field_names, // prefix is important
    clippy::struct_excessive_bools, // no state-machine
)]
pub struct AssertSchemaEqPopulationConfig {
    pub has_tables: bool,
    pub has_columns: bool,
    pub has_views: bool,
    pub has_attributes: bool,
    pub has_domains: bool,
    pub has_user_defined_types: bool,
    pub has_parameters: bool,
    pub has_columns_with_arrays: bool,
    pub has_composite_tyes_with_arrays: bool,
    pub has_domain_with_arrays: bool,
    pub has_enums: bool,
    pub has_table_key_constraints: bool,
    pub has_table_referential_constraints: bool,
    pub has_table_check_constraints: bool,
    pub has_domain_check_constraints: bool,
}

impl Default for AssertSchemaEqPopulationConfig {
    fn default() -> Self {
        Self {
            has_tables: true,
            has_columns: true,
            has_views: true,
            has_attributes: true,
            has_domains: true,
            has_user_defined_types: true,
            has_parameters: true,
            has_columns_with_arrays: true,
            has_composite_tyes_with_arrays: true,
            has_domain_with_arrays: true,
            has_enums: true,
            has_table_key_constraints: true,
            has_table_referential_constraints: true,
            has_table_check_constraints: true,
            has_domain_check_constraints: true,
        }
    }
}

impl AssertSchemaEqPopulationConfig {
    pub fn no_check() -> Self {
        Self {
            has_tables: false,
            has_columns: false,
            has_views: false,
            has_attributes: false,
            has_domains: false,
            has_user_defined_types: false,
            has_parameters: false,
            has_columns_with_arrays: false,
            has_composite_tyes_with_arrays: false,
            has_domain_with_arrays: false,
            has_enums: false,
            has_table_key_constraints: false,
            has_table_referential_constraints: false,
            has_table_check_constraints: false,
            has_domain_check_constraints: false,
        }
    }
}

/// Checks if the schema info is populated correctly.
/// This is more of a sanity check that the schema info is not empty.
fn assert_schema_info_population(
    schema: &SchemaInfo,
    AssertSchemaEqPopulationConfig {
        has_tables,
        has_columns,
        has_views,
        has_attributes,
        has_domains,
        has_user_defined_types,
        has_parameters,
        has_columns_with_arrays,
        has_composite_tyes_with_arrays,
        has_domain_with_arrays,
        has_enums,
        has_table_key_constraints,
        has_table_referential_constraints,
        has_table_check_constraints,
        has_domain_check_constraints,
    }: AssertSchemaEqPopulationConfig,
) {
    assert!(!has_tables || !schema.tables.is_empty());
    assert!(!has_columns || !schema.columns.is_empty());
    assert!(!has_views || !schema.views.is_empty());
    assert!(!has_attributes || !schema.attributes.is_empty());
    assert!(!has_domains || !schema.domains.is_empty());
    assert!(!has_user_defined_types || !schema.user_defined_types.is_empty());
    assert!(!has_parameters || !schema.parameters.is_empty());
    assert!(!has_columns_with_arrays || !schema.column_arrays.is_empty());
    assert!(!has_composite_tyes_with_arrays || !schema.composite_arrays.is_empty());
    assert!(!has_domain_with_arrays || !schema.domain_arrays.is_empty());
    assert!(!has_enums || !schema.enums.is_empty());
    assert!(!has_table_key_constraints || !schema.table_key_constraints.is_empty());
    assert!(!has_table_referential_constraints || !schema.table_referential_constraints.is_empty());
    assert!(!has_table_check_constraints || !schema.table_check_constraints.is_empty());
    assert!(!has_domain_check_constraints || !schema.domain_check_constraints.is_empty());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_compares_schemas() {
        assert_schema_eq(
            |connection| {
                Box::pin(async move {
                    connection
                        .batch_execute(
                            "
                            CREATE TABLE test (
                                id INT PRIMARY KEY CHECK (id >= 0)
                            );
            
                            CREATE TYPE test_type1 AS (a FLOAT);
                            CREATE TYPE test_type2 AS ENUM ('a', 'b', 'c');
                            CREATE DOMAIN test_domain AS INT CHECK(VALUE > 0);
            
                            ALTER TABLE test ADD COLUMN test_column test_domain;
                            ALTER TABLE test ADD COLUMN test_column2 test_type1;
                            ALTER TABLE test ADD COLUMN test_column3 test_type2;
            
                            CREATE TABLE test_ref (
                                foo TEXT
                            );
                            DROP TABLE test_ref;
                            CREATE TABLE test_ref (
                                id INT NOT NULL REFERENCES test(id)
                            );
            
                            CREATE VIEW test_view AS SELECT * FROM test;
                            ",
                        )
                        .await
                        .unwrap();
                    connection
                })
            },
            |connection| {
                Box::pin(async move {
                    connection
                        .batch_execute(
                            "
                            CREATE TYPE test_type1 AS (a FLOAT);
                            CREATE TYPE test_type2 AS ENUM ('a', 'b', 'c');
                            CREATE DOMAIN test_domain AS INT CHECK(VALUE > 0);
                
                            CREATE TABLE test (
                                id INT PRIMARY KEY CHECK (id >= 0),
                                test_column test_domain,
                                test_column2 test_type1,
                                test_column3 test_type2
                            );
                
                            CREATE TABLE test_ref (
                                id INT NOT NULL REFERENCES test(id)
                            );
                
                            CREATE VIEW test_view AS SELECT * FROM test;
                            ",
                        )
                        .await
                        .unwrap();
                    connection
                })
            },
            AssertSchemaEqPopulationConfig {
                has_parameters: false,
                has_columns_with_arrays: false,
                has_composite_tyes_with_arrays: false,
                has_domain_with_arrays: false,
                ..Default::default()
            },
        )
        .await;
    }

    #[tokio::test]
    #[should_panic = "Schemas have different composite types"]
    async fn it_catches_schema_diffs() {
        assert_schema_eq(
            |connection| {
                Box::pin(async move {
                    connection
                        .batch_execute(
                            "
                            CREATE TYPE foo AS (id INT);
            
                            CREATE TABLE bar (
                                id INT,
                                foo foo
                            );
            
                            ALTER TYPE foo ADD ATTRIBUTE baz TEXT;
                            ",
                        )
                        .await
                        .unwrap();
                    connection
                })
            },
            |connection| {
                Box::pin(async move {
                    connection
                        .batch_execute(
                            "
                            CREATE TYPE foo AS (id INT);
                
                            CREATE TABLE bar (
                                id INT,
                                foo foo
                            );
                            ",
                        )
                        .await
                        .unwrap();
                    connection
                })
            },
            AssertSchemaEqPopulationConfig {
                has_views: false,
                has_parameters: false,
                ..Default::default()
            },
        )
        .await;
    }

    #[tokio::test]
    #[should_panic = "Schemas have different enums"]
    async fn it_catches_enum_diffs() {
        assert_schema_eq(
            |connection| {
                Box::pin(async move {
                    connection
                        .batch_execute(
                            "
                            CREATE TYPE test_enum AS ENUM ('a', 'b', 'c');
            
                            CREATE TABLE test (
                                test_enum test_enum
                            );
                            ",
                        )
                        .await
                        .unwrap();
                    connection
                })
            },
            |connection| {
                Box::pin(async move {
                    connection
                        .batch_execute(
                            "
                            CREATE TYPE test_enum AS ENUM ('a', 'b');
            
                            CREATE TABLE test (
                                test_enum test_enum
                            );
                            ",
                        )
                        .await
                        .unwrap();
                    connection
                })
            },
            AssertSchemaEqPopulationConfig::no_check(),
        )
        .await;
    }

    #[tokio::test]
    #[should_panic = "Columns with arrays differ"]
    async fn it_catches_array_diffs() {
        assert_schema_eq(
            |connection| {
                Box::pin(async move {
                    connection
                        .batch_execute(
                            "
                            CREATE TABLE test (
                                test_array INT []
                            );
                            ",
                        )
                        .await
                        .unwrap();
                    connection
                })
            },
            |connection| {
                Box::pin(async move {
                    connection
                        .batch_execute(
                            "
                            CREATE TABLE test (
                                test_array TEXT []
                            );
                            ",
                        )
                        .await
                        .unwrap();
                    connection
                })
            },
            AssertSchemaEqPopulationConfig::no_check(),
        )
        .await;
    }
}
