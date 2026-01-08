use std::fmt;
use std::fmt::Formatter;
use std::net::SocketAddr;
use std::str::FromStr;

pub type HostName = String;
pub type HostAddr = NetworkAddr;
pub type GrpcAddr = NetworkAddr;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct NetworkAddr {
    pub host: HostName,
    pub port: u16,
}

impl NetworkAddr {
    pub fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }
}

impl From<&NetworkAddr> for SocketAddr {
    fn from(value: &NetworkAddr) -> Self {
        value.to_string().parse::<SocketAddr>().unwrap()
    }
}

impl From<NetworkAddr> for SocketAddr {
    fn from(value: NetworkAddr) -> Self {
        value.to_string().parse::<SocketAddr>().unwrap()
    }
}

impl From<&SocketAddr> for NetworkAddr {
    fn from(value: &SocketAddr) -> Self {
        NetworkAddr::new(value.ip().to_string(), value.port())
    }
}

impl From<SocketAddr> for NetworkAddr {
    fn from(value: SocketAddr) -> Self {
        NetworkAddr::new(value.ip().to_string(), value.port())
    }
}

impl From<&str> for NetworkAddr {
    fn from(value: &str) -> Self {
        let sock_addr = SocketAddr::from_str(value).unwrap();
        sock_addr.into()
    }
}

impl From<String> for NetworkAddr {
    fn from(value: String) -> Self {
        let sock_addr = SocketAddr::from_str(value.as_str()).unwrap();
        sock_addr.into()
    }
}

impl std::str::FromStr for NetworkAddr {
    type Err = std::net::AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let addr: SocketAddr = s.parse()?;
        Ok(NetworkAddr::from(addr))
    }
}

impl fmt::Display for NetworkAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}
