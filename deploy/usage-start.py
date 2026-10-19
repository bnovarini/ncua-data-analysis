import os,sqlite3,runpy
os.environ["MCP_USAGE_DB"]="/data/usage.db"
if not os.path.exists("/data/usage.db"):sqlite3.connect("/data/usage.db").close()
import sys;sys.argv=["server","--http"]
runpy.run_module('ncua_data.mcp_server',run_name="__main__")
