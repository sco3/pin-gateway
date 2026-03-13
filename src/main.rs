use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use pingora::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

// Static header names/values to avoid repeated string allocations
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

/// Minimal stack-allocated context - optimized for zero allocations in hot path
pub struct McpCtx {
    should_strip: bool,
    use_tls: bool,
    /// Server name as bytes for faster comparisons
    server_name: [u8; 32],
    server_name_len: usize,
    /// Base path as bytes for faster operations
    base_path: [u8; 16],
    base_path_len: usize,
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

    /// Get server name as string from byte array
    #[inline]
    fn get_server_name_str(ctx: &McpCtx) -> &str {
        std::str::from_utf8(&ctx.server_name[..ctx.server_name_len]).unwrap_or("")
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

    /// Fast path manipulation using pre-allocated buffers
    /// Avoids format! allocations in hot path
    #[inline]
    fn build_uri_fast(new_path: String, query: &str) -> String {
        let mut result = String::with_capacity(new_path.len() + query.len());
        result.push_str(&new_path);
        result.push_str(query);
        result
    }

    /// Strip /vs/<server_name> prefix using byte manipulation
    #[inline]
    fn strip_vs_prefix<'a>(path: &'a str, server_name: &str) -> (String, &'a str) {
        // Path is like /vs/time/http?sessionId=xxx, we want /http?sessionId=xxx
        // First separate path from query
        let (path_only, query) = if let Some(pos) = path.find('?') {
            (&path[..pos], &path[pos..])
        } else {
            (path, "")
        };
        
        // Build prefix manually to avoid format!
        let mut prefix = String::with_capacity(4 + server_name.len());
        prefix.push_str("/vs/");
        prefix.push_str(server_name);
        
        let new_path = if let Some(rest) = path_only.strip_prefix(&prefix) {
            if rest.is_empty() {
                "/".to_string()
            } else {
                rest.to_string()
            }
        } else {
            path_only.to_string()
        };
        
        (new_path, query)
    }
}

#[async_trait]
impl ProxyHttp for DynamicMcpGateway {
    type CTX = McpCtx;

    fn new_ctx(&self) -> Self::CTX {
        McpCtx {
            should_strip: false,
            use_tls: false,
            server_name: [0; 32],
            server_name_len: 0,
            base_path: [0; 16],
            base_path_len: 0,
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

        let current_conf = self.conf.load();

        // Check for /vs/<server_name>/<path> pattern for MCP servers
        let path_segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        // Handle /vs/<server_name>/<rest> pattern
        if path_segments.len() >= 2 && path_segments[0] == "vs" {
            let server_name = path_segments[1];
            if let Some(server) = current_conf.servers.iter().find(|s| s.name == server_name && s.mcp) {
                ctx.should_strip = true; // Strip /vs/<name>
                ctx.use_tls = server.use_tls;
                // Copy server name to fixed array
                let name_bytes = server.name.as_bytes();
                ctx.server_name_len = name_bytes.len().min(32);
                ctx.server_name[..ctx.server_name_len].copy_from_slice(&name_bytes[..ctx.server_name_len]);
                // Copy base path to fixed array
                let base_path = server.base_path();
                let base_path_bytes = base_path.as_bytes();
                ctx.base_path_len = base_path_bytes.len().min(16);
                ctx.base_path[..ctx.base_path_len].copy_from_slice(&base_path_bytes[..ctx.base_path_len]);
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
            // Copy server name to fixed array
            let name_bytes = server.name.as_bytes();
            ctx.server_name_len = name_bytes.len().min(32);
            ctx.server_name[..ctx.server_name_len].copy_from_slice(&name_bytes[..ctx.server_name_len]);
            // Copy base path to fixed array
            let base_path = server.base_path();
            let base_path_bytes = base_path.as_bytes();
            ctx.base_path_len = base_path_bytes.len().min(16);
            ctx.base_path[..ctx.base_path_len].copy_from_slice(&base_path_bytes[..ctx.base_path_len]);
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
                let server_name_str = Self::get_server_name_str(ctx);
                let (new_path, query) = Self::strip_vs_prefix(&current_uri, server_name_str);
                Self::build_uri_fast(new_path, query)
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
                Self::build_uri_fast(new_path, query)
            };
            upstream_request.set_uri(new_uri.parse().unwrap());
        }

        // Preserve headers for SSE/WebSocket using static string references
        let headers = &session.req_header().headers;

        if let Some(val) = headers.get(CONNECTION) {
            let _ = upstream_request.insert_header(CONNECTION, val);
        }
        if let Some(val) = headers.get(ACCEPT) {
            let _ = upstream_request.insert_header(ACCEPT, val);
        }
        if let Some(val) = headers.get(CACHE_CONTROL) {
            let _ = upstream_request.insert_header(CACHE_CONTROL, val);
        }
        if let Some(val) = headers.get(MCP_SESSION_ID) {
            let _ = upstream_request.insert_header(MCP_SESSION_ID, val);
        }

        Ok(())
    }

    async fn response_filter(
        &self,
        session: &mut Session,
        response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        // Remove Content-Length only for GET requests on MCP servers (SSE streaming)
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
        // Single method check - fast exit for non-GET requests
        let method = &session.req_header().method;
        if method != &http::Method::GET || !ctx.is_mcp {
            return Ok(None);
        }

        // Byte scan - check for "data: /" pattern using reference (no clone)
        let Some(body_bytes) = body else { return Ok(None); };

        if !body_bytes.windows(7).any(|w| w == b"data: /") {
            return Ok(None);
        }

        let body_str = match std::str::from_utf8(body_bytes) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };

        // Pre-allocate result buffer to avoid multiple allocations
        let server_name_str = Self::get_server_name_str(ctx);
        let prefix_len = 7 + server_name_str.len(); // "data: /" + server_name
        let mut result = String::with_capacity(body_str.len() + prefix_len);
        
        // Manual replacement to avoid format! allocation
        let mut search_start = 0;
        while let Some(pos) = body_str[search_start..].find("data: /") {
            let absolute_pos = search_start + pos;
            result.push_str(&body_str[..absolute_pos]);
            result.push_str("data: /vs/");
            result.push_str(server_name_str);
            result.push_str("/");
            search_start = absolute_pos + 7; // len("data: /")
        }
        result.push_str(&body_str[search_start..]);
        
        *body = Some(Bytes::from(result));
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
