use arc_swap::ArcSwap;
use async_trait::async_trait;
use pingora::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;


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
}

pub struct DynamicMcpGateway {
    conf: Arc<ArcSwap<McpConfig>>,
}

#[async_trait]
impl ProxyHttp for DynamicMcpGateway {
    type CTX = McpCtx;
    fn new_ctx(&self) -> Self::CTX {
        McpCtx { should_strip: false, use_tls: false }
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

            // Set Host header to the upstream server address
            let host = server.url.clone();
            
            println!("🔌 Routing {} {} -> {}", method, path, server.url);

            Ok(Box::new(HttpPeer::new(&server.url, server.use_tls, host)))
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
        
        println!("   → Forwarding to upstream with path: {}", upstream_request.uri.path());

        Ok(())
    }

    async fn response_filter(
        &self,
        session: &mut Session,
        response: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        // Add CORS headers for web-based tools like MCP Inspector
        let _ = response.insert_header("Access-Control-Allow-Origin", "*");
        let _ = response.insert_header("Access-Control-Allow-Headers", "Content-Type, Authorization");
        let _ = response.insert_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        
        let status = response.status;
        let path = session.req_header().uri.path();
        println!("✅ Response {} for {}", status, path);

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
        _body: &mut Option<bytes::Bytes>,
        _end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) -> Result<Option<std::time::Duration>> {
        Ok(None)
    }

    async fn logging_filter(
        &self,
        session: &mut Session,
        e: &Error,
        _ctx: &mut Self::CTX,
    ) {
        let path = session.req_header().uri.path();
        let method = &session.req_header().method;
        println!("❌ ERROR: {} {} - {:?}", method, path, e);
    }
}
fn main() {
    // 1. Load initial config
    let raw_conf = std::fs::read_to_string("mcp_servers.yaml").expect("Missing config file");
    let config: McpConfig = serde_yaml::from_str(&raw_conf).unwrap();
    let shared_conf = Arc::new(ArcSwap::from_pointee(config));

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
        DynamicMcpGateway { conf: shared_conf },
    );

    mcp_service.add_tcp("0.0.0.0:3000");
    server.add_service(mcp_service);
    server.run_forever();
}