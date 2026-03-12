#!/usr/bin/env python3
"""
MCP SSE Client Test using official Python MCP SDK
Tests connection through gateway vs direct
"""
import asyncio
from mcp import ClientSession
from mcp.client.sse import sse_client


async def test_connection(name: str, base_url: str) -> bool:
    """Test full MCP connection flow"""
    print(f"\n{'='*60}")
    
    sse_url = f"{base_url}/sse"
    print(f"=== {name} ===")
    print(f"   SSE URL: {sse_url}")
    print(f"{'='*60}")

    print(f"   📡 Connecting to SSE...")
    
    try:
        async with sse_client(sse_url) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                # Initialize
                print("   🔄 Initializing...")
                init_result = await session.initialize()
                print(f"   ✅ Connected: {init_result.serverInfo.name} v{init_result.serverInfo.version}")
                
                # List tools
                print("\n--- List Tools ---")
                tools = await session.list_tools()
                if tools.tools:
                    print(f"   ✅ Found {len(tools.tools)} tools:")
                    for tool in tools.tools:
                        desc = tool.description or "No description"
                        print(f"      - {tool.name}: {desc}")
                else:
                    print("   ⚠️ No tools available")
                
                # List resources
                print("\n--- List Resources ---")
                resources = await session.list_resources()
                if resources.resources:
                    print(f"   ✅ Found {len(resources.resources)} resources:")
                    for res in resources.resources:
                        desc = res.description or "No description"
                        print(f"      - {res.name}: {desc}")
                else:
                    print("   ⚠️ No resources available")
                
                # Call get_system_time tool
                print("\n--- Call Tool: get_system_time ---")
                result = await session.call_tool("get_system_time")
                if result.content:
                    print(f"   ✅ Tool result:")
                    for content in result.content:
                        if hasattr(content, 'text'):
                            print(f"      🕐 {content.text}")
                else:
                    print(f"   ⚠️ No content in result")
                
                return True
                
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    print("🔌 MCP SSE Client Test - Official Python SDK")
    print("   Using: mcp.client.sse.sse_client")
    
    # Test 1: Through gateway
    gateway_ok = await test_connection(
        "Gateway (port 3000)",
        "http://localhost:3000/time"
    )
    
    # Test 2: Direct connection
    direct_ok = await test_connection(
        "Direct (port 8111)",
        "http://localhost:8111"
    )
    
    print("\n" + "="*60)
    print("📊 Final Results:")
    print(f"   Gateway: {'✅ PASS' if gateway_ok else '❌ FAIL'}")
    print(f"   Direct:  {'✅ PASS' if direct_ok else '❌ FAIL'}")
    
    if gateway_ok and direct_ok:
        print("\n✅ SUCCESS: Gateway works identically to direct connection!")
    elif direct_ok and not gateway_ok:
        print("\n⚠️ Gateway has issues. Direct connection works.")
    elif not direct_ok:
        print("\n⚠️ Time server may not be running.")


if __name__ == "__main__":
    asyncio.run(main())
