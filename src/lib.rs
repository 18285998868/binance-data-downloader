pub mod error;
pub mod run_batch;
pub mod run_once;

pub use error::AppError;

pub async fn run() -> Result<(), AppError> {
    run_batch::run().await
}
