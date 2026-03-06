pub mod cluster;
pub mod query;

pub trait Supervisable: Send + 'static {
    fn start(self) -> impl std::future::Future<Output = ()> + Send;
}
