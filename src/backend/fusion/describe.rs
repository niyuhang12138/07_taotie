use std::{fmt, sync::Arc};

use arrow::datatypes::{DataType, Field};
use datafusion::{
    dataframe::DataFrame,
    functions::expr_fn::length,
    functions_aggregate::{
        average::avg as f_avg,
        count::count as f_count,
        median::median as f_median,
        min_max::{max as f_max, min as f_min},
        stddev::stddev as f_stddev,
        sum::sum as f_sum,
    },
    functions_array::length::array_length,
    // prelude::{cast, col},
    logical_expr::{case, cast, col, is_null, lit},
};

#[allow(unused)]
#[derive(Debug)]
pub enum DescribeMethod {
    Total,
    NullTotal,
    Min,
    Max,
    Mean,
    Stddev,
    Median,
    // Percentile(u8),
}

#[allow(unused)]
pub struct DataFrameDescriber {
    original: DataFrame,
    transformed: DataFrame,
    methods: Vec<DescribeMethod>,
}

#[allow(unused)]
impl DataFrameDescriber {
    pub fn try_new(df: DataFrame) -> anyhow::Result<Self> {
        let fields = df.schema().fields().iter();
        // change all temporal columns to Float64
        let expressions = fields
            .map(|field| {
                let dt = field.data_type();
                let expr = match dt {
                    dt if dt.is_temporal() => cast(col(field.name()), DataType::Float64),
                    dt if dt.is_numeric() => col(field.name()),
                    DataType::List(_) | DataType::LargeList(_) => array_length(col(field.name())),
                    _ => length(cast(col(field.name()), DataType::Utf8)),
                };
                expr.alias(field.name())
            })
            .collect();

        let transformed = df.clone().select(expressions)?;

        Ok(Self {
            original: df,
            transformed,
            methods: vec![
                DescribeMethod::Total,
                DescribeMethod::NullTotal,
                DescribeMethod::Min,
                DescribeMethod::Max,
                DescribeMethod::Mean,
                DescribeMethod::Stddev,
                DescribeMethod::Median,
            ],
        })
    }

    pub async fn describe(&self) -> anyhow::Result<DataFrame> {
        let df = self.do_describe().await?;
        self.cast_back(df)
    }

    async fn do_describe(&self) -> anyhow::Result<DataFrame> {
        let df: Option<DataFrame> = self.methods.iter().fold(None, |acc, method| {
            let df = self.transformed.clone();
            let stat_df = match method {
                DescribeMethod::Total => total(df).unwrap(),
                DescribeMethod::NullTotal => null_total(df).unwrap(),
                DescribeMethod::Min => min(df).unwrap(),
                DescribeMethod::Max => max(df).unwrap(),
                DescribeMethod::Mean => mean(df).unwrap(),
                DescribeMethod::Stddev => stddev(df).unwrap(),
                DescribeMethod::Median => median(df).unwrap(),
                // _ => todo!(),
            };
            let mut select_expr = vec![lit(method.to_string()).alias("describe")];
            select_expr.extend(stat_df.schema().fields().iter().map(|f| col(f.name())));

            let stat_df = stat_df.select(select_expr).unwrap();

            Some(match acc {
                Some(acc) => acc.union(stat_df).unwrap(),
                None => stat_df,
            })
        });
        df.ok_or_else(|| anyhow::anyhow!("No statistics found"))
    }

    fn cast_back(&self, df: DataFrame) -> anyhow::Result<DataFrame> {
        // we need the describe column
        let describe = Arc::new(Field::new("describe", DataType::Utf8, false));
        let mut fields = vec![&describe];
        fields.extend(self.original.schema().fields().iter());
        let expressions = fields
            .into_iter()
            .map(|field| {
                let dt = field.data_type();
                let expr = match dt {
                    dt if dt.is_temporal() => cast(col(field.name()), dt.clone()),
                    DataType::List(_) | DataType::LargeList(_) => {
                        cast(col(field.name()), DataType::Int32)
                    }
                    _ => col(field.name()),
                };
                expr.alias(field.name())
            })
            .collect();

        Ok(df
            .select(expressions)?
            .sort(vec![col("describe").sort(true, false)])?)
    }
}

impl fmt::Display for DescribeMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DescribeMethod::Total => write!(f, "Total"),
            DescribeMethod::NullTotal => write!(f, "NullTotal"),
            DescribeMethod::Min => write!(f, "Min"),
            DescribeMethod::Max => write!(f, "Max"),
            DescribeMethod::Mean => write!(f, "Mean"),
            DescribeMethod::Stddev => write!(f, "Stddev"),
            DescribeMethod::Median => write!(f, "Median"),
        }
    }
}

macro_rules! describe_method {
    ($name: ident, $method:ident) => {
        fn $name(df: DataFrame) -> anyhow::Result<DataFrame> {
            let original_schema_fields = df.schema().fields().iter();
            let ret = df.clone().aggregate(
                vec![],
                original_schema_fields
                    .map(|f| $method(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            )?;
            Ok(ret)
        }
    };
}

describe_method!(total, f_count);
describe_method!(mean, f_avg);
describe_method!(stddev, f_stddev);
describe_method!(max, f_max);
describe_method!(min, f_min);
describe_method!(median, f_median);

fn null_total(df: DataFrame) -> anyhow::Result<DataFrame> {
    let original_schema_fields = df.schema().fields().iter();
    let ret = df.clone().aggregate(
        vec![],
        original_schema_fields
            .map(|f| {
                f_sum(
                    case(is_null(col(f.name())))
                        .when(lit(true), lit(1))
                        .otherwise(lit(0))
                        .unwrap(),
                )
                .alias(f.name())
            })
            .collect::<Vec<_>>(),
    )?;
    Ok(ret)
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;
    use datafusion::prelude::{CsvReadOptions, SessionContext};

    #[tokio::test]
    async fn data_frame_describer_should_work() -> Result<()> {
        let ctx = SessionContext::new();
        let df = ctx
            .read_csv("assets/person.csv", CsvReadOptions::default())
            .await?;
        let dfd = DataFrameDescriber::try_new(df)?;

        dfd.describe().await?;

        Ok(())
    }
}
