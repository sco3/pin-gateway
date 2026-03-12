#!/usr/bin/env python3
"""
MCP Streamable HTTP Client Test - Gateway vs Direct
Tests the new Streamable HTTP transport (SSE is deprecated)
"""
import asyncio
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


async def test_connection(name: str, base_url: str) -> bool:
    """Test full MCP connection flow using Streamable HTTP"""
    print(f"\n{'='*60}")
    
    http_url = f"{base_url}/http"
    print(f"=== {name} ===")
    print(f"   HTTP URL: {http_url}")
    print(f"{'='*60}")

    print(f"   📡 Connecting via Streamable HTTP...")

    try:
        async with streamable_http_client(http_url) as (read_stream, write_stream, _):
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
    print("🔌 MCP Streamable HTTP Client Test")
    print("   Using: mcp.client.streamable_http.streamable_http_client")
    print("   (SSE transport is deprecated, Streamable HTTP is the new standard)")

    # Test 1: Through gateway
    gateway_ok = await test_connection(
        "Gateway (port 3000)",
        "http://localhost:3000/time"
    )

    # Test 2: Direct connection with Streamable HTTP
    direct_ok = await test_connection(
        "Direct (port 8111)",
        "http://localhost:8111"
    )

    print("\n" + "="*60)
    print("📊 Final Results:")
    print(f"   Gateway (Streamable HTTP): {'✅ PASS' if gateway_ok else '❌ FAIL'}")
    print(f"   Direct (Streamable HTTP):  {'✅ PASS' if direct_ok else '❌ FAIL'}")

    if gateway_ok and direct_ok:
        print("\n✅ SUCCESS: Gateway works identically to direct connection (Streamable HTTP)!")
    elif direct_ok and not gateway_ok:
        print("\n⚠️ Gateway has issues. Direct connection works.")
    elif not direct_ok:
        print("\n⚠️ Time server Streamable HTTP may not be available.")


if __name__ == "__main__":
    asyncio.run(main())
