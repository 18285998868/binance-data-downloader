//! Base URLs the pipeline talks to.

use std::borrow::Cow;

use crate::dataset::Market;

/// Public data archive base URL.
pub(crate) const ARCHIVE_BASE: &str = "https://data.binance.vision";

/// The bucket's public S3 endpoint, used for its listing.
pub(crate) const S3_BASE: &str = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision";

/// Base URLs the pipeline talks to.
///
/// Production always uses the real exchanges ([`Endpoints::live`]). Tests
/// redirect every base at a single `wiremock` server via
/// [`Endpoints::for_tests`], which is what makes start-date resolution,
/// degradation, the pair loop, cancellation and the exit code reachable from
/// unit tests without adding any user-facing configuration.
#[derive(Debug, Clone)]
pub(crate) struct Endpoints {
    /// Base of the public data archive.
    pub(crate) archive_files: Cow<'static, str>,
    /// Base of the spot API.
    pub(crate) exchange_api_spot: Cow<'static, str>,
    /// Base of the USDⓈ-M futures API.
    pub(crate) exchange_api_um: Cow<'static, str>,
    /// Base of the COIN-M futures API.
    pub(crate) exchange_api_cm: Cow<'static, str>,
    /// Base of the archive bucket's S3 endpoint, used for its listing.
    pub(crate) archive_listing: Cow<'static, str>,
}

impl Endpoints {
    /// The real exchanges and `data.binance.vision`.
    pub(crate) fn live() -> Self {
        Self {
            archive_files: Cow::Borrowed(ARCHIVE_BASE),
            exchange_api_spot: Cow::Borrowed(Market::Spot.api_base()),
            exchange_api_um: Cow::Borrowed(Market::Um.api_base()),
            exchange_api_cm: Cow::Borrowed(Market::Cm.api_base()),
            archive_listing: Cow::Borrowed(S3_BASE),
        }
    }

    /// The API base for one market (overridable by tests).
    pub(crate) fn exchange_api(&self, market: Market) -> &str {
        match market {
            Market::Spot => &self.exchange_api_spot,
            Market::Um => &self.exchange_api_um,
            Market::Cm => &self.exchange_api_cm,
        }
    }

    /// All bases pointed at one mock server.
    #[cfg(test)]
    pub(crate) fn for_tests(base: String) -> Self {
        Self {
            archive_files: Cow::Owned(base.clone()),
            exchange_api_spot: Cow::Owned(base.clone()),
            exchange_api_um: Cow::Owned(base.clone()),
            exchange_api_cm: Cow::Owned(base.clone()),
            archive_listing: Cow::Owned(base),
        }
    }
}
