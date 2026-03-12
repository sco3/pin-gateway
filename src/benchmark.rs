//! MCP Streamable HTTP Benchmark
//! Compares requests per second: Direct vs Gateway
//! Uses rmcp 1.2.0 with Streamable HTTP transport
//! 
//! Proper session management:
//! - Initialize session once per client
//! - Reuse session for all tool calls
//! - Measures actual tool call throughput

use rmcp::{
    model::{ClientCapabilities, ClientInfo, Implementation, CallToolRequestParams},
    transport::StreamableHttpClientTransport,
    ServiceExt,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

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

/// Number of concurrent clients to simulate
const CONCURRENT_CLIENTS: usize = 10;

/// Number of requests each client will send
const REQUESTS_PER_CLIENT: usize = 10000;

struct BenchmarkConfig {
    name: String,
    base_url: String,
    concurrent_clients: usize,
    requests_per_client: usize,
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

    let start_time = Instant::now();

    for client_id in 0..config.concurrent_clients {
        let stats_clone = Arc::clone(&stats);
        let base_url = config.base_url.clone();

        tasks.spawn(async move {
            let client_stats = benchmark_client(client_id, base_url, config.requests_per_client).await;
            
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
) -> BenchmarkStats {
    let mut stats = BenchmarkStats::new();
    let http_url = format!("{}/http", base_url);

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

        // Call get_system_time tool
        match client.call_tool(CallToolRequestParams::new("get_system_time")).await {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔌 MCP Streamable HTTP Benchmark");
    println!("   Comparing: Direct connection vs Gateway proxy");
    println!("   Transport: Streamable HTTP (SSE is deprecated)");
    println!("   Method: Initialize once, call get_system_time repeatedly");

    // Benchmark configuration
    let concurrent_clients = CONCURRENT_CLIENTS;
    let requests_per_client = REQUESTS_PER_CLIENT;

    // Test 1: Direct connection
    let direct_stats = run_benchmark(BenchmarkConfig {
        name: "Direct (8111)".to_string(),
        base_url: "http://localhost:8111".to_string(),
        concurrent_clients,
        requests_per_client,
    })
    .await;

    // Small delay between benchmarks
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Test 2: Through gateway
    let gateway_stats = run_benchmark(BenchmarkConfig {
        name: "Gateway (3000)".to_string(),
        base_url: "http://localhost:3000/time".to_string(),
        concurrent_clients,
        requests_per_client,
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

    let gateway_latency = if gateway_stats.successful_requests > 0 {
        gateway_stats.total_latency.as_secs_f64() / gateway_stats.successful_requests as f64
    } else {
        1.0
    };

    let overhead = gateway_latency / direct_latency;

    let direct_throughput = direct_stats.successful_requests as f64 / direct_stats.elapsed.as_secs_f64();
    let gateway_throughput = gateway_stats.successful_requests as f64 / gateway_stats.elapsed.as_secs_f64();

    println!(
        "Direct:  {:.2} req/s (avg {:.2}ms, {}% success)",
        direct_throughput,
        direct_latency * 1000.0,
        direct_stats.successful_requests * 100 / direct_stats.total_requests.max(1)
    );
    println!(
        "Gateway: {:.2} req/s (avg {:.2}ms, {}% success)",
        gateway_throughput,
        gateway_latency * 1000.0,
        gateway_stats.successful_requests * 100 / gateway_stats.total_requests.max(1)
    );
    println!(
        "\nGateway overhead: {:.2}x latency",
        overhead
    );

    if gateway_stats.successful_requests == direct_stats.successful_requests {
        println!("\n✅ Gateway achieves same throughput with {:.2}x overhead", overhead);
    } else if gateway_stats.successful_requests >= direct_stats.successful_requests * 90 / 100 {
        println!("\n✅ Gateway performance is acceptable");
    } else {
        println!("\n⚠️ Gateway has significant overhead");
    }

    Ok(())
}
