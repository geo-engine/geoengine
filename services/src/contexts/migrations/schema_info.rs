use tokio_postgres::Transaction;

type Result<T, E = tokio_postgres::Error> = std::result::Result<T, E>;

#[derive(Debug, PartialEq)]
pub struct SchemaInfo {
    pub tables: Vec<String>,
    pub columns: Vec<SchemaColumn>,
    pub attributes: Vec<SchemaAttribute>,
    pub views: Vec<SchemaView>,
    pub table_constraints: Vec<SchemaTableConstraint>,
    pub referential_constraints: Vec<SchemaReferentialConstraint>,
    pub column_domain_constraints: Vec<SchemaDomainConstraint>,
    pub column_domain_usages: Vec<SchemaColumnDomainUsage>,
    pub constraint_column_usages: Vec<SchemaConstraintColumnUsage>,
    pub constraint_table_usages: Vec<SchemaConstraintTableUsage>,
    pub user_defined_types: Vec<SchemaUserDefinedTypes>,
    pub parameters: Vec<SchemaParameters>,
    pub domains: Vec<SchemaDomain>,
}

pub async fn schema_info_from_information_schema(
    transaction: Transaction<'_>,
    schema_name: &str,
) -> Result<SchemaInfo> {
    Ok(SchemaInfo {
        tables: tables(&transaction, schema_name).await?,
        columns: columns(&transaction, schema_name).await?,
        attributes: attributes(&transaction, schema_name).await?,
        views: views(&transaction, schema_name).await?,
        table_constraints: table_constraints(&transaction, schema_name).await?,
        referential_constraints: referential_constraints(&transaction, schema_name).await?,
        column_domain_constraints: domain_constraints(&transaction, schema_name).await?,
        column_domain_usages: column_domain_usage(&transaction, schema_name).await?,
        constraint_column_usages: constraint_column_usage(&transaction, schema_name).await?,
        constraint_table_usages: constraint_table_usage(&transaction, schema_name).await?,
        user_defined_types: user_defined_types(&transaction, schema_name).await?,
        parameters: parameters(&transaction, schema_name).await?,
        domains: domains(&transaction, schema_name).await?,
    })
}

macro_rules! schema_info_table {
    (
        $type_name:ident,
        $table_name:ident,
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
                        WHERE
                            {schema_column} = $1
                        ORDER BY {order_by}
                    ",
                    columns = stringify!($($field_name),+),
                    table_name = stringify!($table_name),
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
}

schema_info_table!(
    SchemaColumn,
    columns,
    "table_schema",
    "table_name, ordinal_position ASC",
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
    "udt_name, ordinal_position ASC",
    (udt_name, String),
    (attribute_name, String),
    (ordinal_position, i32),
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
    SchemaTableConstraint,
    table_constraints,
    "table_schema",
    "table_name, constraint_name ASC",
    (constraint_name, String),
    (table_name, String),
    (constraint_type, String),
    (is_deferrable, String),
    (initially_deferred, String)
);

schema_info_table!(
    SchemaReferentialConstraint,
    referential_constraints,
    "constraint_schema",
    "constraint_name ASC",
    (constraint_name, String),
    (match_option, String),
    (update_rule, String),
    (delete_rule, String)
);

schema_info_table!(
    SchemaDomainConstraint,
    domain_constraints,
    "constraint_schema",
    "domain_name, constraint_name ASC",
    (constraint_name, String),
    (domain_name, String),
    (is_deferrable, String),
    (initially_deferred, String)
);

schema_info_table!(
    SchemaColumnDomainUsage,
    column_domain_usage,
    "table_schema",
    "table_name, column_name, domain_name ASC",
    (domain_name, String),
    (table_name, String),
    (column_name, String)
);

schema_info_table!(
    SchemaConstraintColumnUsage,
    constraint_column_usage,
    "table_schema",
    "table_name, column_name, constraint_name ASC",
    (table_name, String),
    (column_name, String),
    (constraint_name, String)
);

schema_info_table!(
    SchemaConstraintTableUsage,
    constraint_table_usage,
    "table_schema",
    "table_name, constraint_name ASC",
    (table_name, String),
    (constraint_name, String)
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

// #[derive(Debug, PartialEq)]
// struct SchemaAttribute {
//     udt_name: String,
//     attribute_name: String,
//     ordinal_position: i32,
//     attribute_default: Option<String>,
//     is_nullable: String,
//     data_type: String,
//     character_maximum_length: i32,
//     character_octet_length: i32,
//     numeric_precision: i32,
//     numeric_precision_radix: i32,
//     numeric_scale: i32,
//     datetime_precision: i32,
//     interval_type: String,
//     interval_precision: i32,
// }
//
// async fn columns(transaction: &Transaction<'_>, schema_name: &str) -> Result<Vec<SchemaColumn>> {
//     Ok(transaction
//         .query(
//             "SELECT
//                 table_name,
//                 column_name,
//                 ordinal_position,
//                 column_default,
//                 is_nullable,
//                 data_type
//             FROM
//                 information_schema.columns
//             WHERE
//                 table_schema = $1
//             ORDER BY table_name, ordinal_position ASC",
//             &[&schema_name],
//         )
//         .await?
//         .iter()
//         .map(|row| SchemaColumn {
//             table_name: row.get("table_name"),
//             column_name: row.get("column_name"),
//             ordinal_position: row.get("ordinal_position"),
//             column_default: row.get("column_default"),
//             is_nullable: row.get("is_nullable"),
//             data_type: row.get("data_type"),
//         })
//         .collect::<Vec<SchemaColumn>>())
// }

// async fn attributes(
//     transaction: &Transaction<'_>,
//     schema_name: &str,
// ) -> Result<Vec<SchemaAttribute>> {
//     Ok(transaction
//         .query(
//             "SELECT
//                 udt_name,
//                 attribute_name,
//                 ordinal_position,
//                 attribute_default,
//                 is_nullable,
//                 data_type,
//                 character_maximum_length,
//                 character_octet_length,
//                 numeric_precision,
//                 numeric_precision_radix,
//                 numeric_scale,
//                 datetime_precision,
//                 interval_type,
//                 interval_precision
//             FROM
//                 information_schema.attributes
//             WHERE
//                 udt_schema = $1
//             ORDER BY udt_name, ordinal_position ASC",
//             &[&schema_name],
//         )
//         .await?
//         .iter()
//         .map(|row| SchemaAttribute {
//             udt_name: row.get("udt_name"),
//             attribute_name: row.get("attribute_name"),
//             ordinal_position: row.get("ordinal_position"),
//             attribute_default: row.get("attribute_default"),
//             is_nullable: row.get("is_nullable"),
//             data_type: row.get("data_type"),
//             character_maximum_length: row.get("character_maximum_length"),
//             character_octet_length: row.get("character_octet_length"),
//             numeric_precision: row.get("numeric_precision"),
//             numeric_precision_radix: row.get("numeric_precision_radix"),
//             numeric_scale: row.get("numeric_scale"),
//             datetime_precision: row.get("datetime_precision"),
//             interval_type: row.get("interval_type"),
//             interval_precision: row.get("interval_precision"),
//         })
//         .collect::<Vec<SchemaAttribute>>())
// }
