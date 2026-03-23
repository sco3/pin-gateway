//! MCP Streamable HTTP Benchmark
//! Compares requests per second: Direct vs Gateway
//! Uses rmcp 1.2.0 with Streamable HTTP transport
//!
//! Proper session management:
//! - Initialize session once per client
//! - Reuse session for all tool calls
//! - Measures actual tool call throughput

use clap::Parser;
use rmcp::{
    model::{ClientCapabilities, ClientInfo, Implementation, CallToolRequestParams},
    transport::StreamableHttpClientTransport,
    ServiceExt,
};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

#[derive(Deserialize, Debug, Clone)]
struct McpServerConfig {
    name: String,
    url: String,
    #[allow(dead_code)]
    strip_slug: bool,
    use_tls: bool,
    #[allow(dead_code)]
    mcp: bool,
}

impl McpServerConfig {
    /// Extract base path from URL with leading slash (e.g., "127.0.0.1:8111/http" -> "/http")
    fn base_path(&self) -> &str {
        if let Some(pos) = self.url.find('/') {
            &self.url[pos..]
        } else {
            "/"
        }
    }
}

#[derive(Deserialize, Debug)]
struct McpConfig {
    servers: Vec<McpServerConfig>,
}

#[derive(Parser, Debug)]
#[command(name = "benchmark")]
#[command(about = "MCP Streamable HTTP Benchmark")]
struct Args {
    /// Number of concurrent clients
    #[arg(short = 'u', long = "users", default_value = "125")]
    users: usize,

    /// Number of requests per client
    #[arg(short = 'r', long = "requests-per-user", default_value = "10000")]
    requests_per_user: usize,

    /// Server name from mcp-servers.toml (direct uses configured URL, gateway uses localhost:3000/vs/<name>)
    #[arg(short = 's', long = "server", default_value = "fast")]
    server: String,

    /// Tool name to call for benchmark
    #[arg(short = 't', long = "tool", default_value = "get_system_time")]
    tool: String,

    /// JSON arguments to pass to the tool (as a string, e.g., '{"key": "value"}')
    #[arg(short = 'a', long = "arguments", default_value = "")]
    arguments: String,

    /// Direct bench only (skip gateway)
    #[arg(short = 'd', long = "direct-only", default_value = "false")]
    direct_only: bool,
}

struct BenchmarkStats {
    total_requests: usize,
    successful_requests: usize,
    failed_requests: usize,
    total_latency: Duration,
    elapsed: Duration,
}

impl BenchmarkStats {
    fn new() -> Self {
        Self {
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            total_latency: Duration::ZERO,
            elapsed: Duration::ZERO,
        }
    }
}

struct BenchmarkConfig {
    name: String,
    base_url: String,
    concurrent_clients: usize,
    requests_per_client: usize,
    tool_name: String,
    arguments: Option<serde_json::Map<String, Value>>,
}

async fn run_benchmark(config: BenchmarkConfig) -> BenchmarkStats {
    let stats = Arc::new(Mutex::new(BenchmarkStats::new()));
    let mut tasks = JoinSet::new();

    println!(
        "\n🚀 Starting benchmark: {} ({} clients × {} requests = {} total)",
        config.name,
        config.concurrent_clients,
        config.requests_per_client,
        config.concurrent_clients * config.requests_per_client
    );
    println!("   Tool: {}", config.tool_name);
    if let Some(ref args) = config.arguments {
        println!("   Arguments: {}", serde_json::Value::Object(args.clone()));
    }

    let start_time = Instant::now();

    for client_id in 0..config.concurrent_clients {
        let stats_clone = Arc::clone(&stats);
        let base_url = config.base_url.clone();
        let tool_name = config.tool_name.clone();
        let arguments = config.arguments.clone();

        tasks.spawn(async move {
            let client_stats = benchmark_client(client_id, base_url, config.requests_per_client, tool_name, arguments).await;

            let mut stats = stats_clone.lock().await;
            stats.total_requests += client_stats.total_requests;
            stats.successful_requests += client_stats.successful_requests;
            stats.failed_requests += client_stats.failed_requests;
            stats.total_latency += client_stats.total_latency;
        });
    }

    // Wait for all tasks to complete
    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("Task error: {}", e);
        }
    }

    let elapsed = start_time.elapsed();
    let final_stats = stats.lock().await;

    let avg_latency_ms = if final_stats.successful_requests > 0 {
        final_stats.total_latency.as_secs_f64() / final_stats.successful_requests as f64 * 1000.0
    } else {
        0.0
    };

    let throughput = final_stats.successful_requests as f64 / elapsed.as_secs_f64();

    println!(
        "\n📊 {} Results:",
        config.name
    );
    println!(
        "   Total: {} requests ({} success, {} failed)",
        final_stats.total_requests, final_stats.successful_requests, final_stats.failed_requests
    );
    println!(
        "   Elapsed: {:.2?}",
        elapsed
    );
    println!(
        "   Avg latency: {:.2}ms",
        avg_latency_ms
    );
    println!(
        "   Throughput: {:.2} req/s",
        throughput
    );

    BenchmarkStats {
        total_requests: final_stats.total_requests,
        successful_requests: final_stats.successful_requests,
        failed_requests: final_stats.failed_requests,
        total_latency: final_stats.total_latency,
        elapsed,
    }
}

async fn benchmark_client(
    client_id: usize,
    base_url: String,
    num_requests: usize,
    tool_name: String,
    arguments: Option<serde_json::Map<String, Value>>,
) -> BenchmarkStats {
    let mut stats = BenchmarkStats::new();
    // Append /http only if not already present
    let http_url = if base_url.ends_with("/http") {
        base_url
    } else {
        format!("{}/http", base_url)
    };

    // Step 1: Initialize session ONCE per client
    let transport = StreamableHttpClientTransport::from_uri(http_url.as_str());
    let client_info = ClientInfo::new(
        ClientCapabilities::default(),
        Implementation::new("benchmark-client", "1.0.0"),
    );

    let client = match client_info.serve(transport).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("   Client {} failed to initialize: {}", client_id, e);
            stats.failed_requests = num_requests;
            stats.total_requests = num_requests;
            return stats;
        }
    };

    //println!("   Client {} initialized session", client_id);

    // Step 2: Reuse session for all tool calls
    for i in 0..num_requests {
        let start = Instant::now();

        // Build tool call with optional arguments
        let mut params = CallToolRequestParams::new(tool_name.clone());
        if let Some(ref args) = arguments {
            params = params.with_arguments(args.clone());
        }

        // Call specified tool
        match client.call_tool(params).await {
            Ok(result) => {
                if !result.content.is_empty() {
                    stats.successful_requests += 1;
                } else {
                    stats.failed_requests += 1;
                    if i == 0 {
                        eprintln!("   Client {} error: Empty response", client_id);
                    }
                }
            }
            Err(e) => {
                stats.failed_requests += 1;
                if i == 0 {
                    eprintln!("   Client {} error: {}", client_id, e);
                }
            }
        }

        stats.total_requests += 1;
        stats.total_latency += start.elapsed();
    }

    stats
}

/// Load server configuration from mcp-servers.toml
fn load_server_config(server_name: &str) -> Result<McpServerConfig, Box<dyn std::error::Error>> {
    let config_content = std::fs::read_to_string("mcp-servers.toml")
        .map_err(|e| format!("Failed to read mcp-servers.toml: {}", e))?;
    let config: McpConfig = toml::from_str(&config_content)
        .map_err(|e| format!("Failed to parse mcp-servers.toml: {}", e))?;
    
    config.servers.into_iter()
        .find(|s| s.name == server_name)
        .ok_or_else(|| format!("Server '{}' not found in mcp-servers.toml", server_name).into())
}

/// Build base URL for direct server connection
fn build_direct_url(server: &McpServerConfig) -> String {
    let protocol = if server.use_tls { "https" } else { "http" };
    format!("{}://{}", protocol, server.url)
}

/// Build base URL for gateway connection with /vs/<name> prefix
fn build_gateway_url(server: &McpServerConfig) -> String {
    let protocol = if server.use_tls { "https" } else { "http" };
    // Gateway URL: http://localhost:3000/vs/<name><base_path>
    // The base_path includes the leading slash and any additional path segments
    format!("{}://localhost:3000/vs/{}{}", protocol, server.name, server.base_path())
}

/// Verify that the tool exists on the server, otherwise print available tools
async fn verify_tool_exists(base_url: &str, tool_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Append /http only if not already present
    let http_url = if base_url.ends_with("/http") {
        base_url.to_string()
    } else {
        format!("{}/http", base_url)
    };
    
    let transport = StreamableHttpClientTransport::from_uri(http_url.as_str());
    let client_info = ClientInfo::new(
        ClientCapabilities::default(),
        Implementation::new("benchmark-client", "1.0.0"),
    );

    let client = client_info.serve(transport).await
        .map_err(|e| format!("Failed to connect to server: {}", e))?;

    // Try to list tools using the client's list_tools method
    let tools_result = client.list_tools(None).await
        .map_err(|e| format!("Failed to list tools: {}", e))?;

    let tool_names: Vec<&str> = tools_result.tools.iter().map(|t| t.name.as_ref()).collect();
    
    if !tool_names.contains(&tool_name) {
        eprintln!("\n❌ Tool '{}' not found on server", tool_name);
        eprintln!("\nAvailable tools:");
        for name in tool_names {
            eprintln!("  - {}", name);
        }
        return Err(format!("Tool '{}' not found", tool_name).into());
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("🔌 MCP Streamable HTTP Benchmark");
    println!("   Comparing: Direct connection vs Gateway proxy");
    println!("   Transport: Streamable HTTP (SSE is deprecated)");
    println!("   Method: Initialize once, call {} repeatedly", args.tool);
    println!("   Users: {}, Requests per user: {}", args.users, args.requests_per_user);

    // Load server configuration from mcp-servers.toml
    let server = load_server_config(&args.server)?;

    println!("   Server: {} ({})", server.name, server.url);

    // Parse JSON arguments if provided
    let tool_arguments = if args.arguments.is_empty() {
        None
    } else {
        // Parse as Value first, then extract as Object
        let value: Value = serde_json::from_str(&args.arguments)
            .map_err(|e| format!("Failed to parse JSON arguments: {}", e))?;
        
        // Ensure it's an object
        match value {
            Value::Object(obj) => Some(obj),
            _ => return Err("JSON arguments must be an object (e.g., {\"key\": \"value\"})".into()),
        }
    };

    let concurrent_clients = args.users;
    let requests_per_client = args.requests_per_user;

    // Test 1: Direct connection (using configured URL)
    let direct_url = build_direct_url(&server);
    println!("\n   Direct URL: {}", direct_url);
    
    // Verify tool exists on direct server
    println!("   Verifying tool '{}' exists...", args.tool);
    verify_tool_exists(&direct_url, &args.tool).await?;
    println!("   ✅ Tool '{}' found", args.tool);
    
    let direct_stats = run_benchmark(BenchmarkConfig {
        name: format!("Direct ({})", server.name),
        base_url: direct_url,
        concurrent_clients,
        requests_per_client,
        tool_name: args.tool.clone(),
        arguments: tool_arguments.clone(),
    })
    .await;

    // Exit early if direct-only mode
    if args.direct_only {
        return Ok(());
    }

    // Small delay between benchmarks
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Test 2: Through gateway with /vs/<name> prefix (MCP mode)
    let gateway_url = build_gateway_url(&server);
    println!("\n   Gateway URL: {}", gateway_url);

    // Verify tool exists on gateway
    verify_tool_exists(&gateway_url, &args.tool).await?;
    println!("   ✅ Tool '{}' found", args.tool);

    let gateway_stats = run_benchmark(BenchmarkConfig {
        name: format!("Gateway /vs/{}", server.name),
        base_url: gateway_url,
        concurrent_clients,
        requests_per_client,
        tool_name: args.tool.clone(),
        arguments: tool_arguments.clone(),
    })
    .await;

    // Summary
    println!("\n{}", "=".repeat(60));
    println!("📈 SUMMARY");
    println!("{}", "=".repeat(60));

    let direct_latency = if direct_stats.successful_requests > 0 {
        direct_stats.total_latency.as_secs_f64() / direct_stats.successful_requests as f64
    } else {
        1.0
    };

    let direct_throughput = direct_stats.successful_requests as f64 / direct_stats.elapsed.as_secs_f64();

    println!(
        "Direct:  {:.2} req/s (avg {:.2}ms, {}% success)",
        direct_throughput,
        direct_latency * 1000.0,
        direct_stats.successful_requests * 100 / direct_stats.total_requests.max(1)
    );

    // Exit early if direct-only mode
    if args.direct_only {
        return Ok(());
    }

    let gateway_latency = if gateway_stats.successful_requests > 0 {
        gateway_stats.total_latency.as_secs_f64() / gateway_stats.successful_requests as f64
    } else {
        1.0
    };

    let gateway_throughput = gateway_stats.successful_requests as f64 / gateway_stats.elapsed.as_secs_f64();

    println!(
        "Gateway: {:.2} req/s (avg {:.2}ms, {}% success)",
        gateway_throughput,
        gateway_latency * 1000.0,
        gateway_stats.successful_requests * 100 / gateway_stats.total_requests.max(1)
    );

    let latency_overhead_ms = (gateway_latency - direct_latency) * 1000.0;

    println!(
        "\nGateway latency overhead: +{:.2}ms",
        latency_overhead_ms
    );

    if gateway_stats.successful_requests == direct_stats.successful_requests {
        println!(
            "\n✅ Gateway achieves same throughput with +{:.2}ms latency overhead",
            latency_overhead_ms
        );
    } else if gateway_stats.successful_requests >= direct_stats.successful_requests * 90 / 100 {
        println!("\n✅ Gateway performance is acceptable");
    } else {
        println!("\n⚠️ Gateway has significant overhead");
    }

    Ok(())
}
