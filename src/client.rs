use rust_mcp_sdk::client::{Client, ClientOptions};
use rust_mcp_sdk::sse_transport::SseTransport;
use rust_mcp_sdk::mcp_types::{ListToolsRequest, ListResourcesRequest, ListPromptsRequest};
use tokio::time::{timeout, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔌 MCP Client - Testing connection through gateway\n");

    // Test 1: Connect through gateway (port 3000)
    println!("=== Test 1: Connecting via gateway (http://localhost:3000/time/sse) ===");
    match connect_via_gateway().await {
        Ok(_) => println!("✅ Gateway connection SUCCESS\n"),
        Err(e) => println!("❌ Gateway connection FAILED: {}\n", e),
    }

    // Test 2: Connect directly to time server (port 8111)
    println!("=== Test 2: Connecting directly (http://localhost:8111/sse) ===");
    match connect_direct().await {
        Ok(_) => println!("✅ Direct connection SUCCESS\n"),
        Err(e) => println!("❌ Direct connection FAILED: {}\n", e),
    }

    Ok(())
}

async fn connect_via_gateway() -> Result<(), Box<dyn std::error::Error>> {
    println!("   📡 Connecting to gateway...");
    
    let transport = SseTransport::new("http://localhost:3000/time/sse").await?;
    let client = Client::new(transport, ClientOptions::default()).await?;
    
    println!("   📡 Connected to gateway");
    
    // Get server info
    match client.server_info() {
        Some(info) => println!("   ✅ Server: {} v{}", info.name, info.version),
        None => println!("   ⚠️  No server info available"),
    }
    
    // Try to list tools
    match timeout(Duration::from_secs(5), client.list_tools(ListToolsRequest::default())).await {
        Ok(Ok(tools)) => {
            println!("   ✅ list_tools succeeded");
            println!("   📦 Tools available: {}", tools.tools.len());
            for tool in &tools.tools {
                println!("      - {}", tool.name);
            }
        }
        Ok(Err(e)) => println!("   ⚠️  list_tools error: {}", e),
        Err(_) => println!("   ⏱️  list_tools timeout"),
    }
    
    // Try to list resources
    match timeout(Duration::from_secs(5), client.list_resources(ListResourcesRequest::default())).await {
        Ok(Ok(resources)) => {
            println!("   ✅ list_resources succeeded");
            println!("   📦 Resources available: {}", resources.resources.len());
        }
        Ok(Err(e)) => println!("   ⚠️  list_resources error: {}", e),
        Err(_) => println!("   ⏱️  list_resources timeout"),
    }
    
    // Try to list prompts
    match timeout(Duration::from_secs(5), client.list_prompts(ListPromptsRequest::default())).await {
        Ok(Ok(prompts)) => {
            println!("   ✅ list_prompts succeeded");
            println!("   📦 Prompts available: {}", prompts.prompts.len());
        }
        Ok(Err(e)) => println!("   ⚠️  list_prompts error: {}", e),
        Err(_) => println!("   ⏱️  list_prompts timeout"),
    }
    
    Ok(())
}

async fn connect_direct() -> Result<(), Box<dyn std::error::Error>> {
    println!("   📡 Connecting directly to time server...");
    
    let transport = SseTransport::new("http://localhost:8111/sse").await?;
    let client = Client::new(transport, ClientOptions::default()).await?;
    
    println!("   📡 Connected directly to time server");
    
    // Get server info
    match client.server_info() {
        Some(info) => println!("   ✅ Server: {} v{}", info.name, info.version),
        None => println!("   ⚠️  No server info available"),
    }
    
    // Try to list tools
    match timeout(Duration::from_secs(5), client.list_tools(ListToolsRequest::default())).await {
        Ok(Ok(tools)) => {
            println!("   ✅ list_tools succeeded");
            println!("   📦 Tools available: {}", tools.tools.len());
            for tool in &tools.tools {
                println!("      - {}", tool.name);
            }
        }
        Ok(Err(e)) => println!("   ⚠️  list_tools error: {}", e),
        Err(_) => println!("   ⏱️  list_tools timeout"),
    }
    
    // Try to list resources
    match timeout(Duration::from_secs(5), client.list_resources(ListResourcesRequest::default())).await {
        Ok(Ok(resources)) => {
            println!("   ✅ list_resources succeeded");
            println!("   📦 Resources available: {}", resources.resources.len());
        }
        Ok(Err(e)) => println!("   ⚠️  list_resources error: {}", e),
        Err(_) => println!("   ⏱️  list_resources timeout"),
    }
    
    Ok(())
}
