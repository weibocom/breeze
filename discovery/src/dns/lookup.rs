pub(super) struct Lookup {
    resolver: TokioAsyncResolver,
}

use trust_dns_resolver::TokioAsyncResolver;

use std::net::IpAddr;
impl Lookup {
    pub(super) fn new() -> Self {
        Self {
            resolver: TokioAsyncResolver::tokio_from_system_conf()
                .expect("Failed to create the resolver"),
        }
    }
    pub(super) async fn lookup(&self, host: &str) -> std::io::Result<Vec<IpAddr>> {
        let ips = self.resolver.lookup_ip(host).await?;
        Ok(ips.into_iter().collect())
    }
}
