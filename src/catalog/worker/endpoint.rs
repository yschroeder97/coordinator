use http::Uri;
#[cfg(test)]
use proptest_derive::Arbitrary;
use std::fmt;
use std::str::FromStr;

pub type HostName = String;
pub type HostAddr = NetworkAddr;
pub type GrpcAddr = NetworkAddr;

pub const DEFAULT_GRPC_PORT: u16 = 8080;
pub const DEFAULT_DATA_PORT: u16 = 9090;

#[cfg(test)]
fn arb_host_name() -> impl proptest::strategy::Strategy<Value = String> {
    use proptest::prelude::*;
    prop_oneof![
        // Common localhost
        Just("localhost".to_string()),
        Just("127.0.0.1".to_string()),
        // IPv4
        (0..255u8, 0..255u8, 0..255u8, 0..255u8)
            .prop_map(|(a, b, c, d)| format!("{}.{}.{}.{}", a, b, c, d)),
        // Hostname: start with alphanumeric, allow dashes, end with alphanumeric
        r"[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?"
    ]
}

#[cfg_attr(test, derive(Arbitrary))]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct NetworkAddr {
    #[cfg_attr(test, proptest(strategy = "arb_host_name()"))]
    pub host: HostName,
    #[cfg_attr(test, proptest(strategy = "1..u16::MAX"))]
    pub port: u16,
}

impl NetworkAddr {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        let host = host.into();
        assert!(!host.is_empty(), "Hostname cannot be empty");
        assert!(port > 0, "Port cannot be 0");
        Self { host, port }
    }

    /// Converts to a Uri (useful for gRPC libraries like Tonic)
    pub fn to_uri(&self, scheme: &str) -> Uri {
        Uri::builder()
            .scheme(scheme)
            .authority(format!("{}:{}", self.host, self.port))
            .path_and_query("/")
            .build()
            .expect("Invalid NetworkAddr components")
    }
}

impl fmt::Display for NetworkAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl FromStr for NetworkAddr {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // We use Uri to parse because it understands hostnames + ports
        // We add a dummy scheme so the parser recognizes the authority
        let format_s = if s.contains("://") {
            s.to_string()
        } else {
            format!("http://{}", s)
        };
        let uri = format_s.parse::<Uri>().map_err(|e| e.to_string())?;

        let authority = uri.authority().ok_or("Missing host/port")?;
        let host = authority.host().to_string();
        let port = authority.port_u16().ok_or("Missing port")?;

        Ok(NetworkAddr { host, port })
    }
}
