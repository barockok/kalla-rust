//! kallad — unified Kalla daemon binary.
//!
//! Replaces the former `kalla-scheduler`, `kalla-executor`, and `kalla-worker`
//! binaries with a single binary that exposes `scheduler` and `executor`
//! subcommands.

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "kallad", about = "Kalla reconciliation daemon")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the Ballista scheduler with embedded HTTP runner
    Scheduler {
        /// HTTP port for the job submission API
        #[arg(long, default_value = "8080", env = "HTTP_PORT")]
        http_port: u16,

        /// gRPC port for the Ballista scheduler
        #[arg(long, default_value = "50050", env = "GRPC_PORT")]
        grpc_port: u16,

        /// Host address to bind to
        #[arg(long, default_value = "0.0.0.0", env = "BIND_HOST")]
        bind_host: String,

        /// Number of partitions for distributed source reads
        #[arg(long, default_value = "4", env = "BALLISTA_PARTITIONS")]
        partitions: usize,

        /// Local directory for staging evidence files
        #[arg(long, default_value = "./staging", env = "STAGING_PATH")]
        staging_path: String,
    },

    /// Run a Ballista executor that connects to a scheduler
    Executor {
        /// Hostname of the Ballista scheduler
        #[arg(long, default_value = "localhost", env = "SCHEDULER_HOST")]
        scheduler_host: String,

        /// Port of the Ballista scheduler
        #[arg(long, default_value = "50050", env = "SCHEDULER_PORT")]
        scheduler_port: u16,

        /// Arrow Flight port the executor listens on
        #[arg(long, default_value = "50051", env = "BIND_PORT")]
        flight_port: u16,

        /// gRPC port the executor listens on
        #[arg(long, default_value = "50052", env = "BIND_GRPC_PORT")]
        grpc_port: u16,

        /// Host address to bind to
        #[arg(long, default_value = "0.0.0.0", env = "BIND_HOST")]
        bind_host: String,

        /// Hostname executors advertise to the scheduler (auto-detected if omitted)
        #[arg(long, env = "EXTERNAL_HOST")]
        external_host: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Scheduler {
            http_port,
            grpc_port,
            bind_host,
            partitions,
            staging_path,
        } => {
            kalla_ballista::start_scheduler(kalla_ballista::SchedulerOpts {
                bind_host,
                grpc_port,
                http_port,
                partitions,
                staging_path,
            })
            .await?;
        }
        Commands::Executor {
            scheduler_host,
            scheduler_port,
            flight_port,
            grpc_port,
            bind_host,
            external_host,
        } => {
            kalla_ballista::start_executor(kalla_ballista::ExecutorOpts {
                bind_host,
                flight_port,
                grpc_port,
                scheduler_host,
                scheduler_port,
                external_host,
            })
            .await?;
        }
    }

    Ok(())
}
