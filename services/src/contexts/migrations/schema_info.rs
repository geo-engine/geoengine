use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
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
        $(join = $join:literal,)?
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
                            information_schema.{table_name}
                            {join}
                        WHERE
                            {schema_column} = $1
                        ORDER BY {order_by}
                    ",
                    columns = stringify!($($field_name),+),
                    table_name = stringify!($table_name),
                    join = schema_info_table!(@join_str $($join)?),
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

    (@join_str) => {""};
    (@join_str $str:literal) => {$str};
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
    join = "NATURAL JOIN information_schema.check_constraints",
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
    join = "NATURAL JOIN information_schema.check_constraints",
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
    join = "NATURAL JOIN information_schema.table_constraints",
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
    join = "NATURAL JOIN information_schema.table_constraints",
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

pub struct AssertSchemaEqConfig {
    pub has_views: bool,
}

pub async fn assert_schema_eq(
    migrations: &[Box<dyn Migration>],
    ground_truth_schema_definition: &str,
    AssertSchemaEqConfig { has_views }: AssertSchemaEqConfig,
) {
    async fn get_pool() -> Pool<PostgresConnectionManager<NoTls>> {
        let postgres_config = get_config_element::<crate::util::config::Postgres>().unwrap();
        let pg_mgr = PostgresConnectionManager::new(postgres_config.try_into().unwrap(), NoTls);

        Pool::builder().max_size(1).build(pg_mgr).await.unwrap()
    }

    async fn get_schema(
        connection: &mut bb8_postgres::bb8::PooledConnection<
            '_,
            bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>,
        >,
    ) -> SchemaInfo {
        let transaction = connection
            .build_transaction()
            .read_only(true)
            .start()
            .await
            .unwrap();
        let schema = transaction
            .query_one("SELECT current_schema", &[])
            .await
            .unwrap()
            .get::<_, String>("current_schema");

        schema_info_from_information_schema(transaction, &schema)
            .await
            .unwrap()
    }

    let schema_after_migrations = {
        let pool = get_pool().await;
        let mut connection = pool.get().await.unwrap();

        // initial schema
        migrate_database(&mut connection, migrations, None)
            .await
            .unwrap();

        get_schema(&mut connection).await
    };

    let ground_truth_schema = {
        let pool = get_pool().await;
        let mut connection = pool.get().await.unwrap();

        connection
            .batch_execute(ground_truth_schema_definition)
            .await
            .unwrap();

        get_schema(&mut connection).await
    };

    // it is easier to assess errors if we compare the schemas field by field

    pretty_assertions::assert_eq!(schema_after_migrations.tables, ground_truth_schema.tables);
    assert!(!ground_truth_schema.tables.is_empty());

    pretty_assertions::assert_eq!(schema_after_migrations.columns, ground_truth_schema.columns);
    assert!(!ground_truth_schema.columns.is_empty());

    pretty_assertions::assert_eq!(
        schema_after_migrations.attributes,
        ground_truth_schema.attributes
    );
    assert!(!ground_truth_schema.attributes.is_empty());

    pretty_assertions::assert_eq!(schema_after_migrations.views, ground_truth_schema.views);
    assert!(has_views || ground_truth_schema.views.is_empty());
    assert!(!has_views || !ground_truth_schema.views.is_empty());

    pretty_assertions::assert_eq!(
        schema_after_migrations.user_defined_types,
        ground_truth_schema.user_defined_types
    );
    assert!(!ground_truth_schema.user_defined_types.is_empty());

    pretty_assertions::assert_eq!(
        schema_after_migrations.parameters,
        ground_truth_schema.parameters
    );
    assert!(ground_truth_schema.parameters.is_empty()); // no FUNCTIONs currently

    pretty_assertions::assert_eq!(schema_after_migrations.domains, ground_truth_schema.domains);
    assert!(!ground_truth_schema.domains.is_empty());

    // check constraints…

    pretty_assertions::assert_eq!(
        schema_after_migrations.domain_check_constraints,
        ground_truth_schema.domain_check_constraints
    );
    assert!(!ground_truth_schema.domain_check_constraints.is_empty());

    pretty_assertions::assert_eq!(
        schema_after_migrations.table_key_constraints,
        ground_truth_schema.table_key_constraints
    );
    assert!(!ground_truth_schema.table_key_constraints.is_empty());

    pretty_assertions::assert_eq!(
        schema_after_migrations.table_referential_constraints,
        ground_truth_schema.table_referential_constraints
    );
    assert!(!ground_truth_schema.table_referential_constraints.is_empty());

    pretty_assertions::assert_eq!(
        schema_after_migrations.table_check_constraints,
        ground_truth_schema.table_check_constraints
    );
    assert!(!ground_truth_schema.table_check_constraints.is_empty());
}
