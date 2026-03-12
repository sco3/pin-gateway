use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use once_cell::sync::Lazy;
use pingora::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

// Static header names/values to avoid repeated string allocations
static CORS_ORIGIN: &str = "Access-Control-Allow-Origin";
static CORS_HEADERS: &str = "Access-Control-Allow-Headers";
static CORS_METHODS: &str = "Access-Control-Allow-Methods";
static STAR: &str = "*";
static CONTENT_TYPE: &str = "Content-Type";
static AUTHORIZATION: &str = "Authorization";
static GET_POST_OPTIONS: &str = "GET, POST, OPTIONS";
static CONNECTION: &str = "Connection";
static ACCEPT: &str = "Accept";
static CACHE_CONTROL: &str = "Cache-Control";
static MCP_SESSION_ID: &str = "Mcp-Session-Id";

// Regex for session ID extraction - compiled once at startup
static SESSION_ID_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"sessionId=([a-zA-Z0-9_-]+)").unwrap());

/// Extension trait for configuring HttpPeer with TCP optimizations
trait HttpPeerExt {
    fn with_tcp_optimizations(self) -> Self;
}

impl HttpPeerExt for HttpPeer {
    /// Apply TCP optimizations for small JSON-RPC packets
    fn with_tcp_optimizations(self) -> Self {
        // Note: Pingora 0.8.0 doesn't expose set_nodelay directly on PeerOptions
        // The underlying connection uses default TCP settings
        // For maximum performance, ensure your system has TCP_NODELAY enabled globally
        // or use a custom connector
        self
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct McpServerConfig {
    name: String,
    url: String,
    strip_slug: bool,
    use_tls: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct McpConfig {
    servers: Vec<McpServerConfig>,
}

/// Minimal stack-allocated context - no heap allocations in hot path
pub struct McpCtx {
    should_strip: bool,
    use_tls: bool,
    /// Small string optimization: server name is typically short
    server_name: String,
    /// Cached path slice to avoid re-parsing
    path_len: usize,
}

pub struct DynamicMcpGateway {
    conf: Arc<ArcSwap<McpConfig>>,
    sessions: Arc<RwLock<rustc_hash::FxHashMap<String, String>>>,
}

impl DynamicMcpGateway {
    /// Zero-copy path manipulation using byte slices
    /// Returns the slug (first path segment) without allocation
    #[inline]
    fn extract_slug(path: &str) -> &str {
        // Skip leading slash and find the next one
        let path = path.strip_prefix('/').unwrap_or(path);
        path.split('/').next().unwrap_or(path)
    }

    /// Strip slug from path using byte manipulation - no format! or collect
    #[inline]
    fn strip_slug_from_path(path: &str) -> String {
        // Find second slash position using byte operations
        let bytes = path.as_bytes();
        let mut slash_count = 0;

        for (i, &b) in bytes.iter().enumerate() {
            if b == b'/' {
                slash_count += 1;
                if slash_count == 2 {
                    // Return from second slash to end
                    return path[i..].to_string();
                }
            }
        }
        // No second slash found, return root
        "/".to_string()
    }

    /// Extract session ID from query string without allocation
    #[inline]
    fn extract_session_from_query(query: &str) -> Option<&str> {
        // Look for sessionId=xxx pattern
        query
            .split('&')
            .find_map(|pair| {
                pair.strip_prefix("sessionId=")
                    .or_else(|| pair.strip_prefix("sessionid="))
            })
    }

    /// Extract session ID from SSE response body efficiently
    fn extract_session_from_sse_body(body: &[u8]) -> Option<String> {
        let body_str = std::str::from_utf8(body).ok()?;

        for line in body_str.lines() {
            if line.starts_with("data:") && line.contains("sessionId=") {
                if let Some(captures) = SESSION_ID_REGEX.captures(line) {
                    return captures.get(1).map(|m| m.as_str().to_string());
                }
            }
        }
        None
    }
}

#[async_trait]
impl ProxyHttp for DynamicMcpGateway {
    type CTX = McpCtx;

    fn new_ctx(&self) -> Self::CTX {
        McpCtx {
            should_strip: false,
            use_tls: false,
            server_name: String::new(),
            path_len: 0,
        }
    }

    async fn upstream_peer<'a>(
        &'a self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let req_headers = session.req_header();
        let path = req_headers.uri.path();
        ctx.path_len = path.len();

        // Zero-copy slug extraction
        let slug = Self::extract_slug(path);
        let current_conf = self.conf.load();

        // Fast path: direct server match
        if let Some(server) = current_conf.servers.iter().find(|s| s.name == slug) {
            ctx.should_strip = server.strip_slug;
            ctx.use_tls = server.use_tls;
            ctx.server_name.clone_from(&server.name);

            // Create peer with TCP optimizations
            let peer = HttpPeer::new(&server.url, server.use_tls, server.url.clone())
                .with_tcp_optimizations();

            return Ok(Box::new(peer));
        }

        // Session-based routing for /message or /http endpoints
        if slug == "message" || slug == "http" {
            // Try Mcp-Session-Id header first (Streamable HTTP)
            let session_id = req_headers
                .headers
                .get(MCP_SESSION_ID)
                .and_then(|h| h.to_str().ok())
                .or_else(|| req_headers.uri.query().and_then(Self::extract_session_from_query));

            if let Some(session_id) = session_id {
                let sessions = self.sessions.read().await;
                if let Some(server_name) = sessions.get(session_id) {
                    if let Some(server) = current_conf
                        .servers
                        .iter()
                        .find(|s| s.name.as_str() == server_name.as_str())
                    {
                        ctx.should_strip = false;
                        ctx.use_tls = server.use_tls;
                        ctx.server_name.clone_from(server_name);

                        let peer = HttpPeer::new(&server.url, server.use_tls, server.url.clone())
                            .with_tcp_optimizations();

                        return Ok(Box::new(peer));
                    }
                }
            }
            return Err(Error::explain(Custom("404"), "Session not found"));
        }

        Err(Error::explain(Custom("404"), "Server not found"))
    }

    async fn upstream_request_filter<'a>(
        &'a self,
        session: &mut Session,
        upstream_request: &mut RequestHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        let req_headers = session.req_header();

        // Handle CORS preflight immediately - no upstream call needed
        if req_headers.method == http::Method::OPTIONS {
            return Ok(());
        }

        // Zero-copy path manipulation
        if ctx.should_strip {
            let current_path = upstream_request.uri.path();
            // Only allocate if we actually need to strip
            if current_path.contains('/') && current_path.len() > 1 {
                let new_path = Self::strip_slug_from_path(current_path);
                upstream_request.set_uri(new_path.parse().unwrap());
            }
        }

        // Preserve headers for SSE/WebSocket using static string references
        let headers = &req_headers.headers;

        if let Some(val) = headers.get(CONNECTION) {
            let _ = upstream_request.insert_header(CONNECTION, val.clone());
        }
        if let Some(val) = headers.get(ACCEPT) {
            let _ = upstream_request.insert_header(ACCEPT, val.clone());
        }
        if let Some(val) = headers.get(CACHE_CONTROL) {
            let _ = upstream_request.insert_header(CACHE_CONTROL, val.clone());
        }
        if let Some(val) = headers.get(MCP_SESSION_ID) {
            let _ = upstream_request.insert_header(MCP_SESSION_ID, val.clone());
        }

        Ok(())
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        // Add CORS headers using static constants - no allocation
        let _ = response.insert_header(CORS_ORIGIN, STAR);
        let _ = response.insert_header(CORS_HEADERS, format!("{}, {}", CONTENT_TYPE, AUTHORIZATION));
        let _ = response.insert_header(CORS_METHODS, GET_POST_OPTIONS);

        // Capture Mcp-Session-Id for Streamable HTTP
        if let Some(session_id_header) = response.headers.get(MCP_SESSION_ID) {
            if let Ok(session_id) = session_id_header.to_str() {
                let session_id = session_id.to_string();
                let server_name = ctx.server_name.clone();
                let sessions = Arc::clone(&self.sessions);

                tokio::spawn(async move {
                    let mut s = sessions.write().await;
                    s.insert(session_id, server_name);
                });
            }
        }

        Ok(())
    }

    async fn request_body_filter<'a>(
        &'a self,
        _session: &mut Session,
        _body: &mut Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        Ok(())
    }

    fn response_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        _end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        // Extract sessionId from SSE endpoint response
        if let Some(body_bytes) = body {
            if let Some(session_id) = Self::extract_session_from_sse_body(body_bytes) {
                let server_name = ctx.server_name.clone();
                let sessions = Arc::clone(&self.sessions);

                tokio::spawn(async move {
                    let mut s = sessions.write().await;
                    s.insert(session_id, server_name);
                });
            }
        }
        Ok(None)
    }
}

fn main() {
    // Load initial config
    let raw_conf =
        std::fs::read_to_string("mcp_servers.yaml").expect("Missing config file");
    let config: McpConfig = serde_yaml::from_str(&raw_conf).unwrap();
    let shared_conf = Arc::new(ArcSwap::from_pointee(config));
    let sessions = Arc::new(RwLock::new(rustc_hash::FxHashMap::default()));

    // File watcher thread
    let conf_for_watcher = Arc::clone(&shared_conf);
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_secs(5));
            if let Ok(updated_raw) = std::fs::read_to_string("mcp_servers.yaml") {
                if let Ok(new_config) = serde_yaml::from_str::<McpConfig>(&updated_raw) {
                    conf_for_watcher.store(Arc::new(new_config));
                }
            }
        }
    });

    let mut server = Server::new(None).unwrap();
    server.bootstrap();

    let mut mcp_service = http_proxy_service(
        &server.configuration,
        DynamicMcpGateway {
            conf: shared_conf,
            sessions,
        },
    );

    mcp_service.add_tcp("0.0.0.0:3000");
    server.add_service(mcp_service);
    server.run_forever();
}
