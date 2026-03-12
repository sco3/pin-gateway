use arc_swap::ArcSwap;
use async_trait::async_trait;
use pingora::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;


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

pub struct McpCtx {
    should_strip: bool,
    use_tls: bool,
    server_name: String,
}

pub struct DynamicMcpGateway {
    conf: Arc<ArcSwap<McpConfig>>,
    // Map sessionId -> server_name for routing message endpoints
    sessions: Arc<RwLock<HashMap<String, String>>>,
}

#[async_trait]
impl ProxyHttp for DynamicMcpGateway {
    type CTX = McpCtx;
    fn new_ctx(&self) -> Self::CTX {
        McpCtx { should_strip: false, use_tls: false, server_name: String::new() }
    }

    async fn upstream_peer<'a>(
        &'a self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let path = session.req_header().uri.path();
        let slug = path.split('/').filter(|s| !s.is_empty()).next().unwrap_or("");
        let method = session.req_header().method.to_string();

        let current_conf = self.conf.load();

        // Search the list for the matching name
        if let Some(server) = current_conf.servers.iter().find(|s| s.name == slug) {
            // Write the "sticky note" for the next step
            ctx.should_strip = server.strip_slug;
            ctx.use_tls = server.use_tls;
            ctx.server_name = server.name.clone();

            // Set Host header to the upstream server address
            let host = server.url.clone();

            println!("🔌 Routing {} {} -> {}", method, path, server.url);

            Ok(Box::new(HttpPeer::new(&server.url, server.use_tls, host)))
        } else if slug == "message" || slug == "http" {
            // For message endpoint (SSE) or http endpoint (Streamable HTTP), 
            // look up the session to find the server
            let req_headers = session.req_header();
            
            // Try to get session ID from Mcp-Session-Id header (Streamable HTTP)
            let session_id = req_headers.headers.get("Mcp-Session-Id")
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string())
                .or_else(|| {
                    // Fall back to query parameter (SSE)
                    req_headers.uri.query()
                        .and_then(|q| q.split('=').nth(1))
                        .map(|s| s.to_string())
                });
            
            if let Some(session_id) = session_id {
                let sessions = self.sessions.read().await;
                if let Some(server_name) = sessions.get(&session_id) {
                    let server_name = server_name.clone();
                    drop(sessions);
                    if let Some(server) = current_conf.servers.iter().find(|s| s.name == server_name) {
                        ctx.should_strip = false;
                        ctx.use_tls = server.use_tls;
                        ctx.server_name = server.name.clone();

                        let host = server.url.clone();
                        println!("🔌 Routing {} {} -> {} (session: {})", method, path, server.url, session_id);

                        return Ok(Box::new(HttpPeer::new(&server.url, server.use_tls, host)));
                    }
                }
            }
            println!("❌ Session not found for: {}", slug);
            Err(Error::explain(Custom("404"), "Session not found"))
        } else {
            println!("❌ Server not found: {} (from path: {})", slug, path);
            Err(Error::explain(Custom("404"), "Server not found"))
        }
    }

    async fn upstream_request_filter<'a>(
        &'a self,
        session: &mut Session,
        upstream_request: &mut RequestHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        let method = session.req_header().method.to_string();
        let path = session.req_header().uri.path().to_string();
        let headers = session.req_header();
        
        println!("📥 REQUEST: {} {}", method, path);
        println!("   Headers:");
        for (name, value) in headers.headers.iter() {
            if let Ok(v) = value.to_str() {
                println!("     {} : {}", name, v);
            }
        }

        // Handle CORS preflight requests directly
        if session.req_header().method == "OPTIONS" {
            println!("   ⚡ CORS preflight - skipping upstream");
            return Ok(());
        }

        // For SSE GET requests, store a session mapping for later message requests
        if method == "GET" && !ctx.server_name.is_empty() {
            let sessions = self.sessions.clone();
            let server_name = ctx.server_name.clone();
            // We'll store the session when we see the response with sessionId
            println!("   📡 SSE request for server: {}", server_name);
        }

        // Only strip if the config told us to!
        if ctx.should_strip {
            let path = upstream_request.uri.path().to_string();
            let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

            if parts.len() > 1 {
                let new_path = format!("/{}", parts[1..].join("/"));
                upstream_request.set_uri(new_path.parse().unwrap());
                println!("✂️ Stripped slug. New path: {}", new_path);
            }
        }

        // Preserve Connection and Upgrade headers for SSE/WebSocket
        let headers = session.req_header();
        if let Some(conn) = headers.headers.get("Connection") {
            let _ = upstream_request.insert_header("Connection", conn.clone());
        }
        if let Some(accept) = headers.headers.get("Accept") {
            let _ = upstream_request.insert_header("Accept", accept.clone());
        }
        if let Some(cache) = headers.headers.get("Cache-Control") {
            let _ = upstream_request.insert_header("Cache-Control", cache.clone());
        }
        // Preserve Mcp-Session-Id header for Streamable HTTP
        if let Some(session_id) = headers.headers.get("Mcp-Session-Id") {
            let _ = upstream_request.insert_header("Mcp-Session-Id", session_id.clone());
        }

        println!("   → Forwarding to upstream with path: {}", upstream_request.uri.path());

        Ok(())
    }

    async fn response_filter(
        &self,
        session: &mut Session,
        response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        // Add CORS headers for web-based tools like MCP Inspector
        let _ = response.insert_header("Access-Control-Allow-Origin", "*");
        let _ = response.insert_header("Access-Control-Allow-Headers", "Content-Type, Authorization");
        let _ = response.insert_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");

        let status = response.status;
        let path = session.req_header().uri.path();
        println!("✅ Response {} for {}", status, path);
        
        // Capture Mcp-Session-Id header for Streamable HTTP
        if let Some(session_id_header) = response.headers.get("Mcp-Session-Id") {
            if let Ok(session_id) = session_id_header.to_str() {
                let session_id = session_id.to_string();
                println!("💾 Storing Streamable HTTP session: {} -> {}", session_id, ctx.server_name);
                let sessions = self.sessions.clone();
                let server_name = ctx.server_name.clone();
                tokio::spawn(async move {
                    let mut s = sessions.write().await;
                    s.insert(session_id, server_name);
                    println!("💾 Streamable HTTP session stored in map");
                });
            }
        }

        Ok(())
    }

    async fn request_body_filter<'a>(
        &'a self,
        _session: &mut Session,
        _body: &mut Option<bytes::Bytes>,
        _end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        Ok(())
    }

    fn response_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<bytes::Bytes>,
        _end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<Option<std::time::Duration>> {
        // Capture sessionId from SSE endpoint response
        println!("🔍 response_body_filter: body={:?}, server_name={}", body.is_some(), _ctx.server_name);
        if let Some(body_bytes) = body {
            let body_str = String::from_utf8_lossy(body_bytes);
            println!("🔍 Body content: {}", body_str);
            if body_str.contains("sessionId=") {
                // Extract sessionId from "data: /message?sessionId=xxx"
                for line in body_str.lines() {
                    if line.starts_with("data:") && line.contains("sessionId=") {
                        if let Some(session_id) = line.split("sessionId=").nth(1) {
                            let session_id = session_id.trim().to_string();
                            println!("💾 Storing session: {} -> {}", session_id, _ctx.server_name);
                            // Store in a blocking way (we're in a sync function)
                            let sessions = self.sessions.clone();
                            let server_name = _ctx.server_name.clone();
                            tokio::spawn(async move {
                                let mut s = sessions.write().await;
                                s.insert(session_id, server_name);
                                println!("💾 Session stored in map");
                            });
                        }
                    }
                }
            }
        }
        Ok(None)
    }
}
fn main() {
    // 1. Load initial config
    let raw_conf = std::fs::read_to_string("mcp_servers.yaml").expect("Missing config file");
    let config: McpConfig = serde_yaml::from_str(&raw_conf).unwrap();
    let shared_conf = Arc::new(ArcSwap::from_pointee(config));
    let sessions = Arc::new(RwLock::new(HashMap::new()));

    // 2. Setup File Watcher (Simplified)
    let conf_for_watcher = shared_conf.clone();
    std::thread::spawn(move || {
        // In a real app, use the 'notify' crate to watch for edits.
        // For now, we'll just check every 5 seconds for a demo.
        loop {
            std::thread::sleep(std::time::Duration::from_secs(5));
            if let Ok(updated_raw) = std::fs::read_to_string("mcp_servers.yaml") {
                if let Ok(new_config) = serde_yaml::from_str::<McpConfig>(&updated_raw) {
                    conf_for_watcher.store(Arc::new(new_config));
                    // All new requests now use the new server list!
                }
            }
        }
    });

    let mut server = Server::new(None).unwrap();
    server.bootstrap();

    let mut mcp_service = http_proxy_service(
        &server.configuration,
        DynamicMcpGateway { conf: shared_conf, sessions: sessions.clone() },
    );

    mcp_service.add_tcp("0.0.0.0:3000");
    server.add_service(mcp_service);
    server.run_forever();
}