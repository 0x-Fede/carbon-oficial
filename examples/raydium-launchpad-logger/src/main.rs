use {
    async_trait::async_trait,
    carbon_core::{
        error::CarbonResult,
        instruction::{DecodedInstruction, InstructionMetadata, NestedInstructions},
        metrics::MetricsCollection,
        processor::Processor,
    },
    carbon_log_metrics::LogMetrics,
    carbon_raydium_launchpad_decoder::{
        instructions::RaydiumLaunchpadInstruction, RaydiumLaunchpadDecoder,
        PROGRAM_ID as RAYDIUM_LAUNCHPAD_PROGRAM_ID,
    },
    carbon_yellowstone_grpc_datasource::YellowstoneGrpcGeyserClient,
    yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequestFilterTransactions},
    std::{env, fs::OpenOptions, io::Write, sync::Arc},
    tokio::sync::{Mutex, RwLock},
    std::collections::{HashMap, HashSet},
};

#[tokio::main]
pub async fn main() -> CarbonResult<()> {
    dotenv::dotenv().ok();
    env_logger::init();

    // NOTE: Workaround to solve issue https://github.com/rustls/rustls/issues/1877
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Can't set crypto provider to aws_lc_rs");

    let transaction_filter = SubscribeRequestFilterTransactions {
        vote: Some(false),
        failed: Some(false),
        account_include: vec![],
        account_exclude: vec![],
        account_required: vec![RAYDIUM_LAUNCHPAD_PROGRAM_ID.to_string().clone()],
        signature: None,
    };

    let mut transaction_filters: HashMap<String, SubscribeRequestFilterTransactions> = HashMap::new();
    transaction_filters.insert("raydium_launchpad_transaction_filter".to_string(), transaction_filter);

    let yellowstone_grpc = YellowstoneGrpcGeyserClient::new(
        env::var("GEYSER_URL").unwrap_or_default(),
        env::var("X_TOKEN").ok(),
        Some(CommitmentLevel::Confirmed),
        HashMap::default(),
        transaction_filters,
        Default::default(),
        Arc::new(RwLock::new(HashSet::new())),
    );

    let log_path =
        env::var("LOG_PATH").unwrap_or_else(|_| "raydium_launchpad_events.log".to_string());
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .expect("Unable to open log file");
    let file = Arc::new(Mutex::new(file));

    carbon_core::pipeline::Pipeline::builder()
        .datasource(yellowstone_grpc)
        .metrics(Arc::new(LogMetrics::new()))
        .metrics_flush_interval(3)
        .instruction(
            RaydiumLaunchpadDecoder,
            RaydiumLaunchpadInstructionProcessor { file: file.clone() },
        )
        .shutdown_strategy(carbon_core::pipeline::ShutdownStrategy::Immediate)
        .build()?
        .run()
        .await?;

    Ok(())
}

pub struct RaydiumLaunchpadInstructionProcessor {
    file: Arc<Mutex<std::fs::File>>,
}

#[async_trait]
impl Processor for RaydiumLaunchpadInstructionProcessor {
    type InputType = (
        InstructionMetadata,
        DecodedInstruction<RaydiumLaunchpadInstruction>,
        NestedInstructions,
        solana_instruction::Instruction,
    );

    async fn process(
        &mut self,
        (metadata, instruction, _nested_instructions, _): Self::InputType,
        _metrics: Arc<MetricsCollection>,
    ) -> CarbonResult<()> {
        let signature = metadata.transaction_metadata.signature;
        let json = serde_json::json!({
            "signature": signature,
            "instruction": instruction.data,
        });
        let mut file = self.file.lock().await;
        writeln!(file, "{}", serde_json::to_string(&json)?)?;
        Ok(())
    }
}
