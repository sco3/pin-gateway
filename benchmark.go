// MCP Streamable HTTP Benchmark
// Compares requests per second: Direct vs Gateway
// Uses mcp-go-sdk with Streamable HTTP transport
//
// Proper session management:
// - Initialize session once per client
// - Reuse session for all tool calls
// - Measures actual tool call throughput

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// McpServerConfig holds configuration for a single MCP server
type McpServerConfig struct {
	Name       string `toml:"name"`
	URL        string `toml:"url"`
	StripSlug  bool   `toml:"strip_slug"`
	UseTLS     bool   `toml:"use_tls"`
	Mcp        bool   `toml:"mcp"`
}

// McpConfig holds the full MCP configuration
type McpConfig struct {
	Servers []McpServerConfig `toml:"servers"`
}

// loadServerConfig loads server configuration from mcp-servers.toml
func loadServerConfig(serverName string) (*McpServerConfig, error) {
	var config McpConfig
	if _, err := toml.DecodeFile("mcp-servers.toml", &config); err != nil {
		return nil, fmt.Errorf("failed to read mcp-servers.toml: %w", err)
	}

	for _, server := range config.Servers {
		if server.Name == serverName {
			return &server, nil
		}
	}
	return nil, fmt.Errorf("server '%s' not found in mcp-servers.toml", serverName)
}

// buildDirectURL builds the direct connection URL from server config
func buildDirectURL(server *McpServerConfig) string {
	protocol := "http"
	if server.UseTLS {
		protocol = "https"
	}
	return fmt.Sprintf("%s://%s", protocol, server.URL)
}

// buildGatewayURL builds the gateway URL with /vs/<name> prefix
func buildGatewayURL(server *McpServerConfig) string {
	protocol := "http"
	if server.UseTLS {
		protocol = "https"
	}
	return fmt.Sprintf("%s://localhost:3000/vs/%s%s", protocol, server.Name, server.BasePath())
}

// BasePath extracts base path from URL (e.g., "127.0.0.1:8111/http" -> "/http")
func (s *McpServerConfig) BasePath() string {
	if pos := findSlash(s.URL); pos != -1 {
		return s.URL[pos:]
	}
	return "/"
}

func findSlash(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			return i
		}
	}
	return -1
}

// hasSuffix checks if s ends with suffix (simple implementation to avoid strings package)
func hasSuffix(s, suffix string) bool {
	if len(suffix) > len(s) {
		return false
	}
	return s[len(s)-len(suffix):] == suffix
}

// BenchmarkStats holds statistics for benchmark runs
type BenchmarkStats struct {
	TotalRequests      int
	SuccessfulRequests int
	FailedRequests     int
	TotalLatency       time.Duration
	Elapsed            time.Duration
}

// BenchmarkConfig holds configuration for a benchmark run
type BenchmarkConfig struct {
	Name              string
	BaseURL           string
	ConcurrentClients int
	RequestsPerClient int
	ToolName          string
}

// runBenchmark runs the benchmark with the given configuration
func runBenchmark(config BenchmarkConfig) BenchmarkStats {
	var (
		stats     BenchmarkStats
		statsMu   sync.Mutex
		wg        sync.WaitGroup
		startTime = time.Now()
	)

	fmt.Printf(
		"\n🚀 Starting benchmark: %s (%d clients × %d requests = %d total)\n",
		config.Name,
		config.ConcurrentClients,
		config.RequestsPerClient,
		config.ConcurrentClients*config.RequestsPerClient,
	)

	for clientID := 0; clientID < config.ConcurrentClients; clientID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			clientStats := benchmarkClient(id, config.BaseURL, config.RequestsPerClient, config.ToolName)

			statsMu.Lock()
			stats.TotalRequests += clientStats.TotalRequests
			stats.SuccessfulRequests += clientStats.SuccessfulRequests
			stats.FailedRequests += clientStats.FailedRequests
			stats.TotalLatency += clientStats.TotalLatency
			statsMu.Unlock()
		}(clientID)
	}

	// Wait for all tasks to complete
	wg.Wait()

	elapsed := time.Since(startTime)
	avgLatencyMs := float64(0)
	if stats.SuccessfulRequests > 0 {
		avgLatencyMs = float64(stats.TotalLatency) / float64(stats.SuccessfulRequests) / float64(time.Millisecond)
	}
	throughput := float64(stats.SuccessfulRequests) / elapsed.Seconds()

	fmt.Printf("\n📊 %s Results:\n", config.Name)
	fmt.Printf("   Total: %d requests (%d success, %d failed)\n",
		stats.TotalRequests, stats.SuccessfulRequests, stats.FailedRequests)
	fmt.Printf("   Elapsed: %.2fms\n", float64(elapsed)/float64(time.Millisecond))
	fmt.Printf("   Avg latency: %.2fms\n", avgLatencyMs)
	fmt.Printf("   Throughput: %.2f req/s\n", throughput)

	return BenchmarkStats{
		TotalRequests:      stats.TotalRequests,
		SuccessfulRequests: stats.SuccessfulRequests,
		FailedRequests:     stats.FailedRequests,
		TotalLatency:       stats.TotalLatency,
		Elapsed:            elapsed,
	}
}

// benchmarkClient runs benchmark for a single client
func benchmarkClient(clientID int, baseURL string, numRequests int, toolName string) BenchmarkStats {
	var stats BenchmarkStats
	// Append /http only if not already present
	var httpURL string
	if hasSuffix(baseURL, "/http") {
		httpURL = baseURL
	} else {
		httpURL = fmt.Sprintf("%s/http", baseURL)
	}

	// Step 1: Initialize session ONCE per client
	client := mcp.NewClient(&mcp.Implementation{
		Name:    "benchmark-client",
		Version: "1.0.0",
	}, nil)

	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint: httpURL,
	}, nil)
	if err != nil {
		log.Printf("   Client %d failed to initialize: %v", clientID, err)
		stats.FailedRequests = numRequests
		stats.TotalRequests = numRequests
		return stats
	}
	defer session.Close()

	// Step 2: Reuse session for all tool calls
	for i := 0; i < numRequests; i++ {
		start := time.Now()

		// Call specified tool
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
			Name: toolName,
		})

		latency := time.Since(start)
		stats.TotalLatency += latency
		stats.TotalRequests++

		if err != nil {
			stats.FailedRequests++
			if i == 0 {
				log.Printf("   Client %d error: %v", clientID, err)
			}
			continue
		}

		// Check if the tool call returned an error (IsError flag)
		if result.IsError {
			stats.FailedRequests++
			if i == 0 {
				// Extract error message from result content
				errorMsg := "Unknown error"
				if len(result.Content) > 0 {
					if textContent, ok := result.Content[0].(*mcp.TextContent); ok {
						errorMsg = textContent.Text
					}
				}
				log.Printf("   Client %d tool error: %s", clientID, errorMsg)
			}
			continue
		}

		if len(result.Content) > 0 {
			stats.SuccessfulRequests++
		} else {
			stats.FailedRequests++
			if i == 0 {
				log.Printf("   Client %d error: Empty response", clientID)
			}
		}
	}

	return stats
}

func main() {
	// CLI parameters
	users := flag.Int("u", 125, "Number of concurrent clients")
	requestsPerUser := flag.Int("r", 10000, "Number of requests per client")
	serverName := flag.String("s", "time", "Server name from mcp-servers.toml")
	toolName := flag.String("t", "get_system_time", "Tool name to call for benchmark")
	directOnly := flag.Bool("d", false, "Direct bench only (skip gateway)")
	flag.Parse()

	concurrentClients := *users
	requestsPerClient := *requestsPerUser

	fmt.Println("🔌 MCP Streamable HTTP Benchmark")
	fmt.Println("   Comparing: Direct connection vs Gateway proxy")
	fmt.Println("   Transport: Streamable HTTP (SSE is deprecated)")
	fmt.Println("   Method: Initialize once, call", *toolName, "repeatedly")
	fmt.Printf("   Users: %d, Requests per user: %d\n", concurrentClients, requestsPerClient)

	// Load server configuration from mcp-servers.toml
	server, err := loadServerConfig(*serverName)
	if err != nil {
		log.Fatalf("Error loading server config: %v", err)
	}

	fmt.Printf("   Server: %s (%s)\n", server.Name, server.URL)

	// Test 1: Direct connection (using configured URL)
	directURL := buildDirectURL(server)
	fmt.Printf("\n   Direct URL: %s\n", directURL)

	directStats := runBenchmark(BenchmarkConfig{
		Name:              fmt.Sprintf("Direct (%s)", server.Name),
		BaseURL:           directURL,
		ConcurrentClients: concurrentClients,
		RequestsPerClient: requestsPerClient,
		ToolName:          *toolName,
	})

	// Exit early if direct-only mode
	if *directOnly {
		return
	}

	// Small delay between benchmarks
	time.Sleep(2 * time.Second)

	// Test 2: Through gateway with /vs/<name> prefix (MCP mode)
	gatewayURL := buildGatewayURL(server)
	fmt.Printf("\n   Gateway URL: %s\n", gatewayURL)

	gatewayStats := runBenchmark(BenchmarkConfig{
		Name:              fmt.Sprintf("Gateway /vs/%s", server.Name),
		BaseURL:           gatewayURL,
		ConcurrentClients: concurrentClients,
		RequestsPerClient: requestsPerClient,
		ToolName:          *toolName,
	})

	// Summary
	fmt.Println("\n" + string(make([]byte, 60)))
	fmt.Println("📈 SUMMARY")
	fmt.Println(string(make([]byte, 60)))

	directLatency := float64(1.0)
	if directStats.SuccessfulRequests > 0 {
		directLatency = float64(directStats.TotalLatency) / float64(directStats.SuccessfulRequests)
	}

	gatewayLatency := float64(1.0)
	if gatewayStats.SuccessfulRequests > 0 {
		gatewayLatency = float64(gatewayStats.TotalLatency) / float64(gatewayStats.SuccessfulRequests)
	}

	directThroughput := float64(directStats.SuccessfulRequests) / directStats.Elapsed.Seconds()
	gatewayThroughput := float64(gatewayStats.SuccessfulRequests) / gatewayStats.Elapsed.Seconds()

	directSuccessRate := 0
	if directStats.TotalRequests > 0 {
		directSuccessRate = directStats.SuccessfulRequests * 100 / directStats.TotalRequests
	}
	gatewaySuccessRate := 0
	if gatewayStats.TotalRequests > 0 {
		gatewaySuccessRate = gatewayStats.SuccessfulRequests * 100 / gatewayStats.TotalRequests
	}

	fmt.Printf(
		"Direct:  %.2f req/s (avg %.2fms, %d%% success)\n",
		directThroughput,
		directLatency/float64(time.Millisecond),
		directSuccessRate,
	)
	fmt.Printf(
		"Gateway: %.2f req/s (avg %.2fms, %d%% success)\n",
		gatewayThroughput,
		gatewayLatency/float64(time.Millisecond),
		gatewaySuccessRate,
	)
	latencyOverheadMs := gatewayLatency/directLatency*directLatency - directLatency

	fmt.Printf(
		"\nGateway latency overhead: +%.2fms\n",
		latencyOverheadMs/float64(time.Millisecond),
	)

	if gatewayStats.SuccessfulRequests == directStats.SuccessfulRequests {
		fmt.Printf(
			"\n✅ Gateway achieves same throughput with +%.2fms latency overhead\n",
			latencyOverheadMs/float64(time.Millisecond),
		)
	} else if gatewayStats.SuccessfulRequests >= directStats.SuccessfulRequests*90/100 {
		fmt.Println("\n✅ Gateway performance is acceptable")
	} else {
		fmt.Println("\n⚠️ Gateway has significant overhead")
	}
}
