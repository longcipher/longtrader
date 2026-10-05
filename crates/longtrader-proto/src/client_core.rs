//! Shared constants and URL helpers for Connect-RPC clients.
//! Single owner is `crate::service_name`; these re-exports keep call sites short.

pub(crate) use crate::service_name::{
    MARKET_DATA as SERVICE_MARKET, RUNTIME as SERVICE_RUNTIME, STRATEGY as SERVICE_STRATEGY,
    TRADING as SERVICE_TRADING,
};

/// The `://` that separates a URL scheme from its authority.
const SCHEME_SEPARATOR: &str = "://";

/// Trim trailing slashes from a base URL, leaving the authority separator alone.
///
/// `trim_end_matches('/')` alone strips *every* trailing slash, so a scheme-only
/// prefix loses its `://` and degrades to the unparsable `http:`. Everything
/// after the separator is an authority and path, so the scan starts past it and
/// only slashes belonging to that half are removed. A base that reduces to
/// nothing but a scheme is left as the scheme it was given — not a usable URL,
/// but not a corrupted one either.
#[must_use]
pub(crate) fn trim_base_url(base_url: &str) -> String {
    let Some(at) = base_url.find(SCHEME_SEPARATOR) else {
        // No scheme, so the whole string is the authority/path half and every
        // trailing slash goes.
        return base_url.trim_end_matches('/').to_string();
    };
    // The prefix is the scheme *including* its `://`, so it is never trimmed.
    let split = at + SCHEME_SEPARATOR.len();
    let authority = base_url[split..].trim_end_matches('/');
    format!("{}{authority}", &base_url[..split])
}

/// Build a Connect path `/{service}/{method}`.
///
/// Trims its own base, so the URL is correct for any caller — including one that
/// forwards a base read straight off disk — rather than only for a caller that
/// remembered to trim first.
#[must_use]
pub(crate) fn service_url(base_url: &str, service: &str, method: &str) -> String {
    format!("{}/{}/{}", trim_base_url(base_url), service, method)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use proptest::prelude::*;

    use super::*;

    /// Repository-relative root of the canonical contract tree, resolved exactly
    /// the way `longtrader-contract`'s `build.rs` resolves it for codegen.
    const PROTO_ROOT: &str = "../../proto";

    /// `(constant, proto file relative to PROTO_ROOT, declared service)` for every
    /// canonical service name re-exported by this module.
    const SERVICE_SOURCES: [(&str, &str, &str); 4] = [
        (SERVICE_MARKET, "longtrader/market/v1/market.proto", "MarketDataService"),
        (SERVICE_TRADING, "longtrader/trading/v1/trading.proto", "TradingService"),
        (SERVICE_RUNTIME, "longtrader/terminal/v1/runtime.proto", "RuntimeService"),
        (SERVICE_STRATEGY, "longtrader/terminal/v1/strategy.proto", "StrategyService"),
    ];

    /// Reads a checked-in `.proto` file out of the canonical contract tree.
    fn read_proto(relative: &str) -> String {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(PROTO_ROOT).join(relative);
        std::fs::read_to_string(&path).expect("canonical contract proto file must be readable")
    }

    /// Extracts the `package <name>;` declaration from a `.proto` source.
    fn proto_package(source: &str) -> String {
        source
            .lines()
            .find_map(|line| line.trim().strip_prefix("package "))
            .map(|pkg| pkg.trim().trim_end_matches(';').to_string())
            .expect("proto source must declare a package")
    }

    /// Every `service <Name> {` declaration found in a `.proto` source.
    fn proto_services(source: &str) -> Vec<String> {
        source
            .lines()
            .filter_map(|line| line.trim().strip_prefix("service "))
            .map(|rest| rest.trim().trim_end_matches('{').trim().to_string())
            .collect()
    }

    /// `true` when `value` only uses characters allowed in a fully qualified
    /// proto service name (`package.ServiceName`).
    fn is_proto_path(value: &str) -> bool {
        value.chars().all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_')
    }

    /// The package portion of a `package.ServiceName` constant: everything before
    /// the final dot. Proto packages are lower-case by convention and by
    /// `buf lint`, so the package half is checked separately from the CamelCase
    /// service half.
    fn package_of(qualified: &str) -> &str {
        qualified.rsplit_once('.').map_or(qualified, |(package, _)| package)
    }

    // ---- service name constants ---------------------------------------------------

    #[test]
    fn service_name_constants_are_non_empty_proto_paths() {
        for (constant, path, _) in SERVICE_SOURCES {
            assert!(!constant.is_empty(), "{path}: service name must not be empty");
            assert!(is_proto_path(constant), "{path}: {constant} has an unexpected character");
            // The package half follows the proto lower-case convention; the
            // service half is CamelCase.
            let package = package_of(constant);
            assert!(
                package
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_'),
                "{path}: package {package} must be lower case"
            );
            assert!(
                constant.rsplit_once('.').is_some_and(|(_, service)| {
                    service.starts_with(|c: char| c.is_ascii_uppercase())
                }),
                "{path}: {constant} must end in a CamelCase service name"
            );
            // A Connect path always starts at the root, so a leading dot or a doubled
            // separator would resolve to a different (or no) service.
            assert!(!constant.starts_with('.'), "{path}: {constant} must be fully qualified");
            assert!(!constant.contains(".."), "{path}: {constant} must not contain '..'");
            assert!(!constant.ends_with('.'), "{path}: {constant} must not end with '.'");
        }
    }

    #[test]
    fn service_name_constants_are_pairwise_distinct() {
        let mut seen: Vec<&str> = SERVICE_SOURCES.iter().map(|(c, _, _)| *c).collect();
        let total = seen.len();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), total, "two canonical service names collide");
    }

    #[test]
    fn service_name_constants_equal_their_proto_package_and_service() {
        for (constant, path, service) in SERVICE_SOURCES {
            let source = read_proto(path);
            let services = proto_services(&source);
            assert!(services.iter().any(|s| s == service), "{path} must declare service {service}");
            assert_eq!(constant, format!("{}.{service}", proto_package(&source)), "{path}");
        }
    }

    // ---- trim_base_url ------------------------------------------------------------

    #[test]
    fn trim_base_url_leaves_a_base_without_a_trailing_slash_untouched() {
        let bases = ["http://host:8080", "https://host", "http://127.0.0.1:8810", "host", " ", ""];
        for raw in bases {
            assert_eq!(trim_base_url(raw), raw, "unexpected rewrite of {raw:?}");
        }
    }

    #[test]
    fn trim_base_url_strips_every_trailing_slash() {
        assert_eq!(trim_base_url("http://host:8080/"), "http://host:8080");
        assert_eq!(trim_base_url("http://host:8080//"), "http://host:8080");
        assert_eq!(trim_base_url("http://host:8080/////"), "http://host:8080");
        assert_eq!(trim_base_url("http://h:1/api/"), "http://h:1/api");
        assert_eq!(trim_base_url("http://h:1/api////"), "http://h:1/api");
        assert_eq!(trim_base_url("/"), "");
        assert_eq!(trim_base_url("//"), "");
        assert_eq!(trim_base_url("///"), "");
        assert_eq!(trim_base_url("///////"), "");
    }

    #[test]
    fn trim_base_url_reduces_a_slash_only_string_to_empty() {
        assert_eq!(trim_base_url(""), "");
        assert_eq!(trim_base_url("/"), "");
        assert_eq!(trim_base_url("///"), "");
        assert_eq!(trim_base_url(&"/".repeat(9)), "");
    }

    #[test]
    fn trim_base_url_only_trims_slashes_never_whitespace() {
        assert_eq!(trim_base_url(" http://host/ "), " http://host/ ");
        assert_eq!(trim_base_url("http://host/\t"), "http://host/\t");
        assert_eq!(trim_base_url("http://host/\n"), "http://host/\n");
        assert_eq!(trim_base_url(" http://host "), " http://host ");
        // A trailing space shields the slash, so only the last slash is removed.
        assert_eq!(trim_base_url("http://host/ /"), "http://host/ ");
    }

    #[test]
    fn trim_base_url_keeps_the_authority_separator_of_a_host_less_scheme() {
        // A scheme-only prefix is the one shape whose *kept* trailing `//` is the
        // contract: `trim_end_matches('/')` would reduce `http://` to the
        // unparsable `http:`, and composing a Connect path on top would yield
        // `http:/pkg.Service/Method`. Real base URLs always carry a host, so the
        // terminal never reaches this; the behaviour is pinned here so that a
        // future fix stays a deliberate, visible change.
        let trimmed = trim_base_url("http://");
        assert_eq!(trimmed, "http://");
        // The composed URL still has no authority — the input had none — but the
        // separator survives, so the defect is a missing host rather than a
        // corrupted scheme.
        assert_eq!(service_url(&trimmed, "svc", "Method"), "http:///svc/Method");

        // A real host after the separator is still trimmed normally.
        assert_eq!(trim_base_url("https://"), "https://");
        assert_eq!(trim_base_url("http://host:1//"), "http://host:1");
    }

    // ---- service_url --------------------------------------------------------------

    #[test]
    fn service_url_composes_the_connect_path() {
        let create = service_url("http://host:8810", SERVICE_TRADING, "CreateOrder");
        assert_eq!(create, "http://host:8810/longtrader.trading.v1.TradingService/CreateOrder");
        let health = service_url("http://host:8810", SERVICE_RUNTIME, "Health");
        assert_eq!(health, "http://host:8810/longtrader.terminal.v1.RuntimeService/Health");
    }

    #[test]
    fn service_url_trims_an_untrimmed_base_so_no_empty_segment_appears() {
        // `service_url` owns the trailing-slash invariant, so a caller that
        // forwards a raw `http://host:1/` still gets a well-formed URL with no
        // doubled separator. This is what lets the constructors hand it whatever
        // they were given.
        let raw = service_url("http://host:1/", SERVICE_RUNTIME, "Health");
        assert_eq!(raw, "http://host:1/longtrader.terminal.v1.RuntimeService/Health");
        let canonical = service_url(&trim_base_url("http://host:1/"), SERVICE_RUNTIME, "Health");
        assert_eq!(raw, canonical);
    }

    #[test]
    fn service_url_keeps_empty_service_and_method_segments() {
        assert_eq!(service_url("http://h:1", "", "Method"), "http://h:1//Method");
        assert_eq!(service_url("http://h:1", "Service", ""), "http://h:1/Service/");
        assert_eq!(service_url("http://h:1", "", ""), "http://h:1//");
    }

    #[test]
    fn service_url_with_an_empty_base_still_yields_an_absolute_path() {
        assert_eq!(service_url("", "svc", "Method"), "/svc/Method");
        assert_eq!(service_url("", "", ""), "//");
        // A slash-only base trims to empty, so it lands on the same absolute path
        // rather than emitting a run of leading separators.
        assert_eq!(service_url("///", "svc", "Method"), "/svc/Method");
    }

    // ---- property-based invariants ------------------------------------------------

    proptest! {
        // A base that already ends in a non-slash character is passed through
        // verbatim, and the result never ends in a slash — with one deliberate
        // exception: a scheme followed by nothing but slashes keeps its `//`
        // authority separator, so it has no trailing slash left to speak of and
        // is excluded here (the example test above pins it).
        #[test]
        fn trim_base_url_never_leaves_a_trailing_slash(raw in ".{0,32}") {
            let scheme_only = raw
                .split_once(SCHEME_SEPARATOR)
                .is_some_and(|(_, rest)| rest.chars().all(|c| c == '/'));
            prop_assume!(!scheme_only);
            let trimmed = trim_base_url(&raw);
            prop_assert!(!trimmed.ends_with('/'));
            if !raw.ends_with('/') {
                prop_assert_eq!(trimmed, raw);
            }
        }
    }

    proptest! {
        #[test]
        fn trim_base_url_is_idempotent(raw in ".{0,32}") {
            let once = trim_base_url(&raw);
            let twice = trim_base_url(&once);
            prop_assert_eq!(twice, once);
        }
    }

    proptest! {
        // Appending n slashes to an already-canonical base cannot change what
        // trimming removes beyond the slashes themselves. The head has to be
        // canonical first, and it must not be able to *grow* a `://` out of the
        // padding: a head ending in `:` becomes a scheme-only prefix once two
        // slashes land on it, which is a different base rather than a trimmed
        // copy of the same one.
        #[test]
        fn trim_base_url_is_a_suffix_removal(head in ".{0,24}", n in 0usize..=8) {
            let canonical = trim_base_url(&head);
            prop_assume!(!canonical.ends_with(['/', ':']));
            let padded = format!("{canonical}{}", "/".repeat(n));
            prop_assert_eq!(trim_base_url(&padded), canonical);
        }
    }

    proptest! {
        #[test]
        fn service_url_always_ends_with_service_and_method(
            base in "[a-z:/ .]{0,24}",
            service in "[A-Za-z]{1,12}",
            method in "[A-Za-z]{1,12}",
        ) {
            let url = service_url(&base, &service, &method);
            prop_assert!(url.ends_with(&format!("/{service}/{method}")), "{}", url);
        }
    }

    proptest! {
        // `service_url` trims its own base, so the raw and the pre-trimmed form
        // always coincide: the URL is independent of the trailing separators the
        // caller happened to pass.
        #[test]
        fn service_url_is_trim_invariant(
            base in "[a-z:/ .]{0,24}",
            service in "[A-Za-z]{1,12}",
            method in "[A-Za-z]{1,12}",
        ) {
            let canonical = service_url(&trim_base_url(&base), &service, &method);
            prop_assert!(!canonical.ends_with('/'));
            prop_assert_eq!(service_url(&base, &service, &method), canonical, "base={:?}", base);
        }
    }
}
