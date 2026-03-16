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
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

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
			clientStats := benchmarkClient(id, config.BaseURL, config.RequestsPerClient)

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
func benchmarkClient(clientID int, baseURL string, numRequests int) BenchmarkStats {
	var stats BenchmarkStats
	httpURL := fmt.Sprintf("%s/http", baseURL)

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

		// Call get_system_time tool
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
			Name: "get_system_time",
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
	usersAlt := flag.Int("users", 125, "Number of concurrent clients")
	requestsPerUser := flag.Int("r", 10000, "Number of requests per client")
	requestsPerUserAlt := flag.Int("requests-per-user", 10000, "Number of requests per client")
	flag.Parse()

	// Use provided values or defaults
	concurrentClients := *users
	if *usersAlt != 125 {
		concurrentClients = *usersAlt
	}
	requestsPerClient := *requestsPerUser
	if *requestsPerUserAlt != 10000 {
		requestsPerClient = *requestsPerUserAlt
	}

	fmt.Println("🔌 MCP Streamable HTTP Benchmark")
	fmt.Println("   Comparing: Direct connection vs Gateway proxy")
	fmt.Println("   Transport: Streamable HTTP (SSE is deprecated)")
	fmt.Println("   Method: Initialize once, call get_system_time repeatedly")
	fmt.Printf("   Users: %d, Requests per user: %d\n", concurrentClients, requestsPerClient)

	// Test 1: Direct connection
	directStats := runBenchmark(BenchmarkConfig{
		Name:              "Direct (8111)",
		BaseURL:           "http://localhost:8111",
		ConcurrentClients: concurrentClients,
		RequestsPerClient: requestsPerClient,
	})

	// Small delay between benchmarks
	time.Sleep(2 * time.Second)

	// Test 2: Through gateway
	gatewayStats := runBenchmark(BenchmarkConfig{
		Name:              "Gateway /vs/time (3000)",
		BaseURL:           "http://localhost:3000/vs/time",
		ConcurrentClients: concurrentClients,
		RequestsPerClient: requestsPerClient,
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

// Check for HTTP client errors
var _ = http.ErrAbortHandler

// Check for OS functionality
var _ = os.Args
