mod describe;

use crate::{cli::connect::DatasetConn, Backend, ReplDisplay};
use datafusion::{
    arrow::util::pretty::pretty_format_batches,
    prelude::{CsvReadOptions, DataFrame, NdJsonReadOptions, SessionConfig, SessionContext},
};
use describe::DataFrameDescriber;
use std::ops::Deref;

pub struct DataFusionBackend(SessionContext);

impl DataFusionBackend {
    pub fn new() -> Self {
        let mut config = SessionConfig::new();
        config.options_mut().catalog.information_schema = true;
        Self(SessionContext::new_with_config(config))
    }
}

impl Backend for DataFusionBackend {
    type DataFrame = datafusion::dataframe::DataFrame;

    async fn connect(&mut self, opts: &crate::cli::connect::ConnectOps) -> anyhow::Result<()> {
        match &opts.conn {
            DatasetConn::Postgres(_conn_str) => {
                println!("Connected to Postgres");
            }
            DatasetConn::CSv(file_opts) => {
                let csv_options = CsvReadOptions {
                    file_extension: &file_opts.ext,
                    file_compression_type: file_opts.compression,
                    ..Default::default()
                };
                self.register_csv(&opts.name, &file_opts.filename, csv_options)
                    .await?;
            }
            DatasetConn::Parquet(filename) => {
                self.register_parquet(&opts.name, &filename, Default::default())
                    .await?;
            }
            DatasetConn::NdJson(file_opts) => {
                let json_options = NdJsonReadOptions {
                    file_extension: &file_opts.ext,
                    file_compression_type: file_opts.compression,
                    ..Default::default()
                };
                self.register_json(&opts.name, &file_opts.filename, json_options)
                    .await?;
            }
        };

        Ok(())
    }

    async fn list(&self) -> anyhow::Result<Self::DataFrame> {
        let sql = "select table_name, table_type from information_schema.tables where table_schema = 'public'";
        let df = self.0.sql(sql).await?;
        Ok(df)
    }

    async fn schema(&self, name: &str) -> anyhow::Result<Self::DataFrame> {
        let df = self.0.sql(&format!("DESCRIBE {name}")).await?;
        Ok(df)
    }

    async fn describe(&self, name: &str) -> anyhow::Result<Self::DataFrame> {
        let df = self.0.sql(&format!("select * from {name}")).await?;
        let ddf = DataFrameDescriber::try_new(df)?;
        ddf.describe().await
    }

    async fn head(&self, name: &str, size: usize) -> anyhow::Result<Self::DataFrame> {
        let df = self
            .0
            .sql(&format!("SELECT * FROM {name} LIMIT {size}"))
            .await?;
        Ok(df)
    }

    async fn sql(&self, sql: &str) -> anyhow::Result<Self::DataFrame> {
        let df = self.0.sql(sql).await?;
        Ok(df)
    }
}

impl Default for DataFusionBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for DataFusionBackend {
    type Target = SessionContext;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ReplDisplay for DataFrame {
    async fn display(self) -> anyhow::Result<String> {
        let batches = self.collect().await?;
        let data = pretty_format_batches(&batches)?;
        Ok(data.to_string())
    }
}
