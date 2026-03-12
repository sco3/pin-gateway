#!/usr/bin/env python3
"""Simple MCP SSE client test"""
import asyncio
from mcp import ClientSession
from mcp.client.sse import sse_client


async def run():
    print("Testing Gateway: http://localhost:3000/time/sse")
    
    async with sse_client("http://localhost:3000/time/sse") as (read, write):
        async with ClientSession(read, write) as session:
            # Initialize
            print("\n1. Initializing...")
            result = await session.initialize()
            print(f"   ✅ Server: {result.serverInfo.name}")
            
            # List tools
            print("\n2. Listing tools...")
            tools = await session.list_tools()
            print(f"   ✅ Found {len(tools.tools)} tools")
            for t in tools.tools:
                print(f"      - {t.name}")
            
            # Call get_system_time tool
            print("\n3. Calling get_system_time...")
            result = await session.call_tool("get_system_time")
            print(f"   ✅ Result: {result.content}")
            
    print("\n✅ SUCCESS!")


if __name__ == "__main__":
    asyncio.run(run())
