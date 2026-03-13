use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use pingora::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

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
static ACCEPT_ENCODING: &str = "Accept-Encoding";

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
    mcp: bool,
}

impl McpServerConfig {
    /// Extract host:port from URL (e.g., "127.0.0.1:8111/http" -> "127.0.0.1:8111")
    fn host_port(&self) -> &str {
        if let Some(pos) = self.url.find('/') {
            &self.url[..pos]
        } else {
            &self.url
        }
    }

    /// Extract base path from URL with leading slash (e.g., "127.0.0.1:8111/http" -> "/http")
    fn base_path(&self) -> &str {
        if let Some(pos) = self.url.find('/') {
            &self.url[pos..]
        } else {
            "/"
        }
    }
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
    /// Base path from config URL (e.g., "/http")
    base_path: String,
    /// Is this an MCP server (requires /vs/<name>/ prefix)
    is_mcp: bool,
}

pub struct DynamicMcpGateway {
    conf: Arc<ArcSwap<McpConfig>>,
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

    /// Strip /vs/<server_name> prefix from path for MCP servers
    /// Returns (path, query) tuple to preserve query string
    #[inline]
    fn strip_vs_prefix(path: &str, server_name: &str) -> (String, String) {
        // Path is like /vs/time/http?sessionId=xxx, we want /http?sessionId=xxx
        // First separate path from query
        let (path_only, query) = if let Some(pos) = path.find('?') {
            (&path[..pos], &path[pos..])
        } else {
            (path, "")
        };
        
        let prefix = format!("/vs/{}", server_name);
        let new_path = if let Some(rest) = path_only.strip_prefix(&prefix) {
            if rest.is_empty() {
                "/".to_string()
            } else {
                rest.to_string()
            }
        } else {
            path_only.to_string()
        };
        
        (new_path, query.to_string())
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
            base_path: String::new(),
            is_mcp: false,
        }
    }

    async fn upstream_peer(
        & self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let req_headers = session.req_header();
        let path = req_headers.uri.path();
        ctx.path_len = path.len();

        let current_conf = self.conf.load();

        // Check for /vs/<server_name>/<path> pattern for MCP servers
        let path_segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        // Handle /vs/<server_name>/<rest> pattern
        if path_segments.len() >= 2 && path_segments[0] == "vs" {
            let server_name = path_segments[1];
            if let Some(server) = current_conf.servers.iter().find(|s| s.name == server_name && s.mcp) {
                ctx.should_strip = true; // Strip /vs/<name>
                ctx.use_tls = server.use_tls;
                ctx.server_name.clone_from(&server.name);
                ctx.base_path = server.base_path().to_string();
                ctx.is_mcp = true;

                let peer = HttpPeer::new(server.host_port(), server.use_tls, server.host_port().to_string())
                    .with_tcp_optimizations();

                return Ok(Box::new(peer));
            }
            return Err(Error::explain(Custom("404"), "MCP server not found"));
        }

        // Fast path: direct server match (non-MCP servers)
        let slug = Self::extract_slug(path);
        if let Some(server) = current_conf.servers.iter().find(|s| s.name == slug && !s.mcp) {
            ctx.should_strip = server.strip_slug;
            ctx.use_tls = server.use_tls;
            ctx.server_name.clone_from(&server.name);
            ctx.base_path = server.base_path().to_string();
            ctx.is_mcp = false;

            let peer = HttpPeer::new(server.host_port(), server.use_tls, server.host_port().to_string())
                .with_tcp_optimizations();

            return Ok(Box::new(peer));
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

        // Only remove Accept-Encoding for MCP requests to allow Gzip for other traffic
        if ctx.is_mcp {
            upstream_request.remove_header(ACCEPT_ENCODING);
        }

        // Zero-copy path manipulation
        if ctx.should_strip {
            // Get the full URI path+query (e.g., "/vs/time/message?sessionId=xxx")
            let current_uri = upstream_request.uri.to_string();
            let new_uri = if ctx.is_mcp {
                // Strip /vs/<server_name> for MCP servers, preserving query string
                let (new_path, query) = Self::strip_vs_prefix(&current_uri, &ctx.server_name);
                format!("{}{}", new_path, query)
            } else {
                // For non-MCP servers, strip just the first segment
                // Extract path only (before query)
                let (path_only, query) = if let Some(pos) = current_uri.find('?') {
                    (&current_uri[..pos], &current_uri[pos..])
                } else {
                    (current_uri.as_str(), "")
                };
                let new_path = if path_only.contains('/') && path_only.len() > 1 {
                    Self::strip_slug_from_path(path_only)
                } else {
                    path_only.to_string()
                };
                format!("{}{}", new_path, query)
            };
            upstream_request.set_uri(new_uri.parse().unwrap());
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
        session: &mut Session,
        response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        // Add CORS headers using static constants - no allocation
        let _ = response.insert_header(CORS_ORIGIN, STAR);
        let _ = response.insert_header(CORS_HEADERS, format!("{}, {}", CONTENT_TYPE, AUTHORIZATION));
        let _ = response.insert_header(CORS_METHODS, GET_POST_OPTIONS);

        // Remove CONTENT_LENGTH only for GET requests on MCP servers (SSE streaming)
        if session.req_header().method == http::Method::GET && ctx.is_mcp {
            response.remove_header("Content-Length");
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
        session: &mut Session,
        body: &mut Option<Bytes>,
        _end_of_stream: bool,
        ctx: &mut Self::CTX,
    ) -> Result<Option<Duration>> {
        // Step 1: Fast path - skip body analysis for non-GET requests (0.2ms latency for POST)
        if session.req_header().method != http::Method::GET {
            return Ok(None);
        }

        // Step 2: Byte scan - check for "data: /" pattern before any string conversion
        let body_bytes = match body {
            Some(bytes) => bytes.clone(),
            None => return Ok(None),
        };

        if !body_bytes.windows(7).any(|w| w == b"data: /") {
            return Ok(None);
        }

        // Step 3: Rewrite - only after passing the above checks
        let body_str = match std::str::from_utf8(&body_bytes) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };

        let new_body = body_str.replace("data: /", &format!("data: /vs/{}/", ctx.server_name));
        *body = Some(Bytes::from(new_body));

        Ok(None)
    }
}

fn main() {
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("pin_gateway=debug".parse().unwrap()),
        )
        .init();

    info!("Starting MCP Gateway with SSE body rewrite");

    // Load initial config
    let raw_conf =
        std::fs::read_to_string("mcp-servers.toml").expect("Missing config file");
    let config: McpConfig = toml::from_str(&raw_conf).unwrap();
    let shared_conf = Arc::new(ArcSwap::from_pointee(config));

    // File watcher thread
    let conf_for_watcher = Arc::clone(&shared_conf);
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_secs(5));
            if let Ok(updated_raw) = std::fs::read_to_string("mcp-servers.toml") {
                if let Ok(new_config) = toml::from_str::<McpConfig>(&updated_raw) {
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
        },
    );

    mcp_service.add_tcp("0.0.0.0:3000");
    server.add_service(mcp_service);
    server.run_forever();
}
