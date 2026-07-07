use backoff::ExponentialBackoff;

pub fn default_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        max_elapsed_time: Some(std::time::Duration::from_secs(30)),
        ..Default::default()
    }
}
