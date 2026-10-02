//! TLS to a node (protocol §1.1).
//!
//! A node given a certificate speaks TLS 1.3 on both ports and nothing else, and
//! a cluster node refuses to serve its clients in the clear unless its operator
//! chose to. So a client is **configured** for one or the other: a [`Tls`] says
//! whom it trusts, and every connection made with it — the first, every one a
//! redirect opens, and every HTTP request — checks the node's certificate chain
//! against that trust and its name against the host it dialled.
//!
//! There is no way to skip either check. A client that accepts any certificate
//! is talking to whoever answered.

use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::error::{Error, Result};

/// Whom a client trusts a node by.
///
/// Cheap to clone; one value serves every connection a client opens.
#[derive(Clone)]
pub struct Tls {
    wire: Arc<ClientConfig>,
    http: Arc<ClientConfig>,
}

impl std::fmt::Debug for Tls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tls").finish_non_exhaustive()
    }
}

impl Tls {
    /// Trust the certificates in a PEM file's bytes — one or several.
    ///
    /// # Errors
    ///
    /// [`Error::Tls`] when the bytes hold no certificate, or one that cannot be
    /// a trust anchor.
    pub fn trusting_pem(pem: &[u8]) -> Result<Self> {
        let mut roots = RootCertStore::empty();
        let mut found = 0_usize;
        for certificate in CertificateDer::pem_slice_iter(pem) {
            let certificate = certificate
                .map_err(|why| Error::Tls(format!("an unreadable certificate: {why}")))?;
            roots.add(certificate).map_err(|why| {
                Error::Tls(format!("a certificate that cannot be trusted: {why}"))
            })?;
            found = found.saturating_add(1);
        }
        if found == 0 {
            return Err(Error::Tls("no certificate to trust".to_owned()));
        }
        Ok(Self::trusting(&roots))
    }

    /// Trust the operating system's own certificate store.
    ///
    /// # Errors
    ///
    /// [`Error::Tls`] when the store cannot be read or holds nothing usable.
    #[cfg(feature = "platform-roots")]
    pub fn platform() -> Result<Self> {
        let loaded = rustls_native_certs::load_native_certs();
        let mut roots = RootCertStore::empty();
        let (added, _) = roots.add_parsable_certificates(loaded.certs);
        if added == 0 {
            let why = loaded
                .errors
                .first()
                .map_or_else(|| "it is empty".to_owned(), ToString::to_string);
            return Err(Error::Tls(format!(
                "the platform's certificate store gave nothing to trust: {why}"
            )));
        }
        Ok(Self::trusting(&roots))
    }

    /// Trust exactly these roots.
    ///
    /// Private, so no rustls type is part of this crate's interface and a
    /// rustls upgrade is never a breaking change here.
    fn trusting(roots: &RootCertStore) -> Self {
        let settings = |alpn: &[&[u8]]| {
            let mut settings =
                ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                    .with_root_certificates(roots.clone())
                    .with_no_client_auth();
            settings.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
            Arc::new(settings)
        };
        Self {
            wire: settings(&[]),
            http: settings(&[b"http/1.1"]),
        }
    }

    /// Wrap a connected socket for the wire port of the node at `address`.
    pub(crate) async fn wire(
        &self,
        address: &str,
        socket: TcpStream,
    ) -> Result<TlsStream<TcpStream>> {
        shake(&self.wire, address, socket).await
    }

    /// Wrap a connected socket for the HTTP port of the node at `address`.
    pub(crate) async fn http(
        &self,
        address: &str,
        socket: TcpStream,
    ) -> Result<TlsStream<TcpStream>> {
        shake(&self.http, address, socket).await
    }
}

/// Complete the handshake, checking the certificate against the host dialled.
///
/// Every failure here is [`Error::Tls`] and not [`Error::Io`], because nothing
/// about a second attempt at the same node would differ: the protocol says it
/// is not retried, and a retry loop that matches `Io` must not see it.
async fn shake(
    settings: &Arc<ClientConfig>,
    address: &str,
    socket: TcpStream,
) -> Result<TlsStream<TcpStream>> {
    TlsConnector::from(Arc::clone(settings))
        .connect(server_name(address)?, socket)
        .await
        .map_err(|why| Error::Tls(why.to_string()))
}

/// The name a node's certificate must carry: the host part of `address`, a DNS
/// name or an IP address, without its port or an IPv6 address's brackets.
pub(crate) fn server_name(address: &str) -> Result<ServerName<'static>> {
    let host = address
        .rsplit_once(':')
        .map_or(address, |(host, _)| host)
        .trim_start_matches('[')
        .trim_end_matches(']');
    ServerName::try_from(host.to_owned()).map_err(|why| {
        Error::Tls(format!(
            "{host:?} is not a name a certificate carries: {why}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use rustls::pki_types::ServerName;

    use super::{Tls, server_name};

    #[test]
    fn the_name_checked_is_the_host_without_its_port() {
        assert_eq!(
            server_name("db.example:9080").ok(),
            ServerName::try_from("db.example").ok()
        );
        assert_eq!(
            server_name("[::1]:9080").ok(),
            ServerName::try_from("::1").ok()
        );
        assert!(matches!(
            server_name("127.0.0.1:9080"),
            Ok(ServerName::IpAddress(_))
        ));
        assert!(server_name("not a host:9080").is_err());
    }

    #[test]
    fn nothing_to_trust_is_refused_rather_than_trusting_nothing() {
        assert!(matches!(Tls::trusting_pem(b""), Err(crate::Error::Tls(_))));
    }
}
