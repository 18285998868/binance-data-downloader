/// var: "HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"
const PROXY_ENV_VARS: &[&str] = &[
    "HTTPS_PROXY",
    "https_proxy",
    "HTTP_PROXY",
    "http_proxy",
    "ALL_PROXY",
    "all_proxy",
];

pub fn proxy_url() -> Option<String> {
    PROXY_ENV_VARS
        .iter()
        .find_map(|var| std::env::var(var).ok())
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn https_proxy_is_preferred() {
        temp_env::with_vars(
            &[
                ("HTTPS_PROXY", Some("https://proxy:8443")),
                ("HTTP_PROXY", Some("http://fallback:8080")),
            ],
            || assert_eq!(proxy_url(), Some("https://proxy:8443".into())),
        );
    }

    #[test]
    fn empty_var_is_skipped() {
        temp_env::with_var("HTTPS_PROXY", Some(""), || {
            assert!(proxy_url().is_none());
        });
    }

    #[test]
    fn http_proxy_fallback() {
        temp_env::with_vars(
            &[
                ("HTTPS_PROXY", None),
                ("HTTP_PROXY", Some("http://proxy:3128")),
            ],
            || assert_eq!(proxy_url(), Some("http://proxy:3128".into())),
        );
    }

    #[test]
    fn all_proxy_last_resort() {
        temp_env::with_vars(
            &[
                ("HTTPS_PROXY", None),
                ("HTTP_PROXY", None),
                ("ALL_PROXY", Some("socks5://127.0.0.1:1080")),
            ],
            || assert_eq!(proxy_url(), Some("socks5://127.0.0.1:1080".into())),
        );
    }
}
