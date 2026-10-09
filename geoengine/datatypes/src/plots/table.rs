use crate::error;
use crate::plots::{Plot, PlotData, PlotMetaData};
use crate::util::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use snafu::ensure;
use std::collections::HashSet;

/// A table with one row per record and configurable columns.
///
/// The records stay unchanged in the `data.values` of the Vega-Lite spec,
/// so clients can read the raw values from there.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Table {
    /// The field of each row that labels the row
    row_label_field: String,
    rows: Vec<Map<String, Value>>,
    columns: Vec<TableColumn>,
}

/// A column of a [`Table`]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableColumn {
    /// The field of the row to display, or the field to store a computed value in
    pub field: String,
    /// The column header, defaults to `field`
    pub title: Option<String>,
    /// A d3 format string, e.g. `,d` or `.2f`; values are displayed as they are if it is `None`
    pub format: Option<String>,
    /// A Vega expression that computes the value of the column, e.g. `datum.percentiles[0].value`
    pub expression: Option<String>,
}

impl TableColumn {
    /// A column that displays the `field` of each row
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            title: None,
            format: None,
            expression: None,
        }
    }

    /// A column that displays the result of the Vega `expression`, stored in `field`
    pub fn computed(field: impl Into<String>, expression: impl Into<String>) -> Self {
        Self {
            expression: Some(expression.into()),
            ..Self::new(field)
        }
    }

    #[must_use]
    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    #[must_use]
    pub fn with_format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }

    fn title(&self) -> &str {
        self.title.as_deref().unwrap_or(&self.field)
    }
}

impl Table {
    /// Creates a table that labels its `rows` with their `row_label_field`.
    ///
    /// # Errors
    ///
    /// Fails if there are no `columns` or if column fields or titles are not unique.
    ///
    pub fn new(
        row_label_field: impl Into<String>,
        rows: Vec<Map<String, Value>>,
        columns: Vec<TableColumn>,
    ) -> Result<Self> {
        ensure!(
            !columns.is_empty(),
            error::Plot {
                details: "A table needs at least one column."
            }
        );

        let mut fields = HashSet::new();
        let mut titles = HashSet::new();
        for column in &columns {
            ensure!(
                fields.insert(column.field.as_str()),
                error::Plot {
                    details: format!("The table column field `{}` is not unique.", column.field)
                }
            );
            ensure!(
                titles.insert(column.title()),
                error::Plot {
                    details: format!("The table column title `{}` is not unique.", column.title())
                }
            );
        }

        Ok(Self {
            row_label_field: row_label_field.into(),
            rows,
            columns,
        })
    }

    /// The field that holds the display text of the column at `index`
    fn text_field(index: usize) -> String {
        format!("__text_{index}")
    }

    /// The transforms that compute the columns, format their values and fold them into one cell per column
    fn transform(&self) -> Vec<Value> {
        let mut transform = Vec::new();

        for column in &self.columns {
            if let Some(expression) = &column.expression {
                transform.push(serde_json::json!({
                    "calculate": expression,
                    "as": column.field,
                }));
            }
        }

        for (index, column) in self.columns.iter().enumerate() {
            let value = format!("datum[{}]", expression_string(&column.field));
            let text = match &column.format {
                Some(format) => {
                    format!("format({value}, {})", expression_string(format))
                }
                None => format!("'' + {value}"),
            };
            transform.push(serde_json::json!({
                // missing values and NaN, which is serialized as `null`, are displayed as empty cells
                "calculate": format!("isValid({value}) ? {text} : ''"),
                "as": Self::text_field(index),
            }));
        }

        let text_fields: Vec<String> = (0..self.columns.len()).map(Self::text_field).collect();
        transform.push(serde_json::json!({
            "fold": text_fields,
            "as": ["__column", "__text"],
        }));

        let column_titles: Map<String, Value> = self
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| (Self::text_field(index), column.title().into()))
            .collect();
        transform.push(serde_json::json!({
            "calculate": format!("{}[datum.__column]", Value::Object(column_titles)),
            "as": "__title",
        }));

        transform
    }
}

/// A string literal for a Vega expression
fn expression_string(value: &str) -> String {
    Value::String(value.to_string()).to_string()
}

impl Plot for Table {
    fn to_vega_embeddable(&self, _allow_interactions: bool) -> Result<PlotData> {
        let transform = self.transform();

        let vega_spec = serde_json::json!({
            "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
            "width": "container",
            "data": {
                "values": self.rows,
            },
            "transform": transform,
            "encoding": {
                "x": {
                    "field": "__title",
                    "type": "nominal",
                    "title": null,
                    "sort": self.columns.iter().map(TableColumn::title).collect::<Vec<_>>(),
                    "axis": {
                        "orient": "top",
                        "labelAngle": -45,
                        "labelAlign": "left",
                        "labelBaseline": "middle",
                        "labelFontWeight": "bold",
                        "ticks": false,
                        "domain": false,
                        "labelPadding": 8,
                    },
                },
                "y": {
                    "field": self.row_label_field,
                    "type": "nominal",
                    "title": null,
                    "sort": null,
                    "axis": {
                        "labelFontWeight": "bold",
                        "ticks": false,
                        "domain": false,
                        "labelPadding": 8,
                    },
                },
            },
            "layer": [
                {
                    "mark": {
                        "type": "rect",
                        "fill": null,
                        "strokeWidth": 1,
                    },
                },
                {
                    "mark": {
                        "type": "text",
                        // truncate texts with an ellipsis instead of overflowing into the next cell
                        "limit": {"expr": "bandwidth('x') - 4"},
                    },
                    "encoding": {
                        "text": {
                            "field": "__text",
                            "type": "nominal",
                        },
                        // shows the complete text of truncated cells
                        "tooltip": {
                            "field": "__text",
                            "type": "nominal",
                        },
                    },
                },
            ],
            "config": {
                "view": {
                    "stroke": null,
                },
            },
        });

        Ok(PlotData {
            vega_string: vega_spec.to_string(),
            metadata: PlotMetaData::None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(value: Value) -> Map<String, Value> {
        match value {
            Value::Object(map) => map,
            _ => panic!("rows must be JSON objects"),
        }
    }

    #[test]
    fn it_creates_a_vega_table() {
        let rows = vec![
            row(
                serde_json::json!({"name": "b", "count": 1000, "mean": 1.5, "percentiles": [{"percentile": 0.5, "value": 2.0}]}),
            ),
            row(serde_json::json!({"name": "a", "count": 2, "mean": null, "percentiles": []})),
        ];

        let table = Table::new(
            "name",
            rows.clone(),
            vec![
                TableColumn::new("count").with_format(",d"),
                TableColumn::new("mean").with_title("Mean"),
                TableColumn::computed("p0", "datum.percentiles[0].value")
                    .with_title("p50")
                    .with_format(".2f"),
            ],
        )
        .unwrap();

        let spec: Value =
            serde_json::from_str(&table.to_vega_embeddable(false).unwrap().vega_string).unwrap();

        // the rows stay unchanged and in order
        assert_eq!(
            spec["data"]["values"],
            Value::Array(rows.into_iter().map(Value::Object).collect())
        );

        assert_eq!(
            spec["transform"],
            serde_json::json!([
                {"calculate": "datum.percentiles[0].value", "as": "p0"},
                {"calculate": "isValid(datum[\"count\"]) ? format(datum[\"count\"], \",d\") : ''", "as": "__text_0"},
                {"calculate": "isValid(datum[\"mean\"]) ? '' + datum[\"mean\"] : ''", "as": "__text_1"},
                {"calculate": "isValid(datum[\"p0\"]) ? format(datum[\"p0\"], \".2f\") : ''", "as": "__text_2"},
                {"fold": ["__text_0", "__text_1", "__text_2"], "as": ["__column", "__text"]},
                {"calculate": "{\"__text_0\":\"count\",\"__text_1\":\"Mean\",\"__text_2\":\"p50\"}[datum.__column]", "as": "__title"},
            ])
        );

        assert_eq!(
            spec["encoding"]["x"]["sort"],
            serde_json::json!(["count", "Mean", "p50"])
        );
        assert_eq!(spec["encoding"]["y"]["field"], "name");
        assert_eq!(spec["encoding"]["y"]["sort"], Value::Null);

        // texts are truncated to their cell and shown completely in a tooltip
        assert_eq!(
            spec["layer"][1]["mark"]["limit"],
            serde_json::json!({"expr": "bandwidth('x') - 4"})
        );
        assert_eq!(spec["layer"][1]["encoding"]["tooltip"]["field"], "__text");
    }

    #[test]
    fn it_requires_columns() {
        assert!(Table::new("name", vec![], vec![]).is_err());
    }

    #[test]
    fn it_requires_unique_fields_and_titles() {
        assert!(
            Table::new(
                "name",
                vec![],
                vec![TableColumn::new("a"), TableColumn::new("a").with_title("b")]
            )
            .is_err()
        );
        assert!(
            Table::new(
                "name",
                vec![],
                vec![TableColumn::new("a"), TableColumn::new("b").with_title("a")]
            )
            .is_err()
        );
    }
}
