use std::sync::Arc;

use arrow::array::{Array, Int64Builder, StructArray, StructBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use async_trait::async_trait;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::ExecutionError;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::prelude::*;
use failure::_core::any::Any;

struct MockLoader {
    data: Arc<Vec<StructArray>>,
}

impl MockLoader {
    pub fn new(data: Vec<StructArray>) -> Self {
        assert!(!data.is_empty());

        Self {
            data: Arc::new(data),
        }
    }
}

impl TableProvider for MockLoader {
    fn schema(&self) -> SchemaRef {
        if let DataType::Struct(fields) = self.data[0].data_type() {
            Arc::new(Schema::new(fields.clone()))
        } else {
            unreachable!();
        }
    }

    fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _batch_size: usize,
    ) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
        Ok(Arc::new(MockExecutionPlan {
            schema: self.schema(),
            data: self.data.clone(),
        }))
    }
}

struct MockRecordBatchReader {
    schema: Arc<Schema>,
    data: Arc<Vec<StructArray>>,
    i: usize,
}

impl Iterator for MockRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.i;
        self.i += 1;
        self.data.get(i).map(RecordBatch::from).map(Result::Ok)
    }
}

impl RecordBatchReader for MockRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Debug)]
struct MockExecutionPlan {
    schema: Arc<Schema>,
    data: Arc<Vec<StructArray>>,
}

#[async_trait]
impl ExecutionPlan for MockExecutionPlan {
    fn as_any(&self) -> &dyn Any {
        unimplemented!()
    }

    fn schema(&self) -> SchemaRef {
        unimplemented!()
    }

    fn output_partitioning(&self) -> Partitioning {
        // one partition
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
        unimplemented!()
    }

    async fn execute(
        &self,
        _partition: usize,
    ) -> Result<Box<dyn RecordBatchReader + Send>, ExecutionError> {
        Ok(Box::new(MockRecordBatchReader {
            schema: self.schema.clone(),
            data: self.data.clone(),
            i: 0,
        }))
    }
}

#[tokio::test]
#[allow(clippy::blacklisted_name)]
async fn filter() {
    let mut foo = StructBuilder::from_schema(
        Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]),
        5,
    );
    foo.field_builder::<Int64Builder>(0)
        .unwrap()
        .append_slice(&[0, 1, 2, 3, 4])
        .unwrap();
    foo.field_builder::<Int64Builder>(1)
        .unwrap()
        .append_slice(&[1, 1, 2, 2, 3])
        .unwrap();
    for _ in 0..5 {
        foo.append(true).unwrap();
    }

    let foo = foo.finish();

    let mut ctx = ExecutionContext::new();

    let schema = if let DataType::Struct(fields) = foo.data_type() {
        Arc::new(Schema::new(fields.clone()))
    } else {
        panic!();
    };

    let provider = MemTable::new(schema, vec![vec![RecordBatch::from(&foo)]]).unwrap();

    ctx.register_table("foo", Box::new(provider));
    let df = ctx
        .table("foo")
        .and_then(|df| df.filter(col("a").lt_eq(col("b"))))
        .unwrap();

    let result = df.collect().await.unwrap();

    assert_eq!(result.len(), 1);

    dbg!(&result[0]);
}

#[tokio::test]
#[allow(clippy::blacklisted_name)]
async fn loader() {
    let mut foo = StructBuilder::from_schema(
        Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]),
        5,
    );
    foo.field_builder::<Int64Builder>(0)
        .unwrap()
        .append_slice(&[0, 1, 2, 3, 4])
        .unwrap();
    foo.field_builder::<Int64Builder>(1)
        .unwrap()
        .append_slice(&[1, 1, 2, 2, 3])
        .unwrap();
    for _ in 0..5 {
        foo.append(true).unwrap();
    }

    let foo = foo.finish();

    let mut ctx = ExecutionContext::new();

    let loader = MockLoader::new(vec![foo]);
    let provider = MemTable::load(&loader, usize::MAX).await.unwrap();

    ctx.register_table("foo", Box::new(provider));
    let df = ctx
        .table("foo")
        .and_then(|df| df.filter(col("a").lt_eq(col("b"))))
        .unwrap();

    let result = df.collect().await.unwrap();

    assert_eq!(result.len(), 1);

    dbg!(&result[0]);
}
