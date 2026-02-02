"""
CockroachDB Connection Utilities

This module provides connection management for CockroachDB using pg8000.

Two APIs supported:
1. get_cockroachdb_connection() - Returns cursor-based connection (DBAPI 2.0)
2. get_cockroachdb_connection_native() - Returns native connection (pg8000.native)
"""

import pg8000
import pg8000.native
import ssl


def _test_connection(conn, use_native: bool = False) -> None:
    """
    Test a CockroachDB connection by executing SELECT version().
    
    Args:
        conn: pg8000 connection object (either cursor-based or native)
        use_native: If True, use native API (conn.run()), else use cursor API
    
    Raises:
        Exception: If connection test fails
    """
    try:
        if use_native:
            # Native API: conn.run() returns list of tuples directly
            result = conn.run("SELECT version()")
            version = result[0][0]
        else:
            # Cursor API: need to use cursor()
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
        
        print("✅ Connected to CockroachDB")
        print(f"   Version: {version[:50]}...")
    except Exception as e:
        conn.close()
        print(f"❌ Connection test failed: {e}")
        raise


def get_cockroachdb_connection(
    cockroachdb_host: str,
    cockroachdb_port: int,
    cockroachdb_user: str,
    cockroachdb_password: str,
    cockroachdb_database: str,
    test: bool = True
):
    """
    Create connection to CockroachDB using pg8000.
    
    Args:
        cockroachdb_host: CockroachDB host (without port)
        cockroachdb_port: CockroachDB port (default: 26257)
        cockroachdb_user: Database user
        cockroachdb_password: Database password
        cockroachdb_database: Database name
        test: If True, test the connection by executing SELECT version() (default: True)
    
    Returns:
        pg8000 connection object
    
    Raises:
        Exception: If connection fails or test query fails (when test=True)
    
    Example:
        >>> # With connection test (default)
        >>> conn = get_cockroachdb_connection(
        ...     cockroachdb_host="myhost.cockroachlabs.cloud",
        ...     cockroachdb_port=26257,
        ...     cockroachdb_user="myuser",
        ...     cockroachdb_password="mypassword",
        ...     cockroachdb_database="defaultdb"
        ... )
        >>> # Connection is tested automatically
        
        >>> # Without connection test
        >>> conn = get_cockroachdb_connection(
        ...     cockroachdb_host="myhost.cockroachlabs.cloud",
        ...     cockroachdb_port=26257,
        ...     cockroachdb_user="myuser",
        ...     cockroachdb_password="mypassword",
        ...     cockroachdb_database="defaultdb",
        ...     test=False
        ... )
    """
    # Create SSL context (required for CockroachDB Cloud)
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    # Parse host (in case port is accidentally included in host string)
    host = cockroachdb_host.split(':')[0] if ':' in cockroachdb_host else cockroachdb_host
    
    conn = pg8000.connect(
        user=cockroachdb_user,
        password=cockroachdb_password,
        host=host,
        port=cockroachdb_port,
        database=cockroachdb_database,
        ssl_context=ssl_context
    )
    
    # Test connection if requested
    if test:
        _test_connection(conn, use_native=False)
    
    return conn


def get_cockroachdb_connection_native(
    cockroachdb_host: str,
    cockroachdb_port: int,
    cockroachdb_user: str,
    cockroachdb_password: str,
    cockroachdb_database: str,
    test: bool = True
):
    """
    Create connection to CockroachDB using pg8000.native (no cursor needed).
    
    This is the MODERN API - use this for new code!
    
    Differences from get_cockroachdb_connection():
    - No cursor() method - use conn.run() directly
    - Returns results as list of tuples immediately
    - Simpler API with less boilerplate
    
    Args:
        cockroachdb_host: CockroachDB host (without port)
        cockroachdb_port: CockroachDB port (default: 26257)
        cockroachdb_user: Database user
        cockroachdb_password: Database password
        cockroachdb_database: Database name
        test: If True, test the connection by executing SELECT version() (default: True)
    
    Returns:
        pg8000.native.Connection object
    
    Raises:
        Exception: If connection fails or test query fails (when test=True)
    
    Example:
        >>> # With connection test (default)
        >>> conn = get_cockroachdb_connection_native(
        ...     cockroachdb_host="myhost.cockroachlabs.cloud",
        ...     cockroachdb_port=26257,
        ...     cockroachdb_user="myuser",
        ...     cockroachdb_password="mypassword",
        ...     cockroachdb_database="defaultdb"
        ... )
        >>> # Connection is tested automatically
        
        >>> # Without connection test
        >>> conn = get_cockroachdb_connection_native(
        ...     cockroachdb_host="myhost.cockroachlabs.cloud",
        ...     cockroachdb_port=26257,
        ...     cockroachdb_user="myuser",
        ...     cockroachdb_password="mypassword",
        ...     cockroachdb_database="defaultdb",
        ...     test=False
        ... )
    """
    # Create SSL context (required for CockroachDB Cloud)
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    # Parse host (in case port is accidentally included in host string)
    host = cockroachdb_host.split(':')[0] if ':' in cockroachdb_host else cockroachdb_host
    
    conn = pg8000.native.Connection(
        user=cockroachdb_user,
        password=cockroachdb_password,
        host=host,
        port=cockroachdb_port,
        database=cockroachdb_database,
        ssl_context=ssl_context
    )
    
    # Test connection if requested
    if test:
        _test_connection(conn, use_native=True)
    
    return conn
