use std::ops::Deref;

use anyhow::Ok;
use datafusion::{
    arrow::util::pretty::pretty_format_batches,
    prelude::{SessionConfig, SessionContext},
};

use crate::{cli::connect::DatasetConn, Backend, ReplDisplay};

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
            DatasetConn::CSv(filename) => {
                self.register_csv(&opts.name, &filename, Default::default())
                    .await?;
            }
            DatasetConn::Parquet(filename) => {
                self.register_parquet(&opts.name, &filename, Default::default())
                    .await?;
            }
            DatasetConn::NdJson(filename) => {
                self.register_json(&opts.name, &filename, Default::default())
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
        let df = df.describe().await?;
        Ok(df)
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

impl ReplDisplay for datafusion::dataframe::DataFrame {
    async fn display(self) -> anyhow::Result<String> {
        let batches = self.collect().await?;
        let data = pretty_format_batches(&batches)?;
        Ok(data.to_string())
    }
}
