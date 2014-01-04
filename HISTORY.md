0.3.1

- support for asynchronous data streaming in single row mode -
  asynchronous versions of get_result, get_last_result and
  their deferrable variants
- watcher improvements allowing to reset pending commands
- minor DRY improvements in code and specs
- single_row_mode? helper
- spec: Travis CI and Coverage

0.3.0

- dedicated asynchronous connection pool
- works on windows (with ruby 2.0+): uses PGConn#socket_io object instead of
  #socket file descriptor
- socket watch handler is not being detached between command calls
- no more separate em and em-synchrony client
- api changes: async_exec and async_query command are now fiber-synchronized
- api changes: other async_* methods are removed or deprecated
- api changes: asynchronous methods renamed to *_defer
- transaction() helper method that can be called recursively
- requirements updated: eventmachine >~ 1.0.0, pg >= 0.17.0, ruby >= 1.9.2
- spec: more auto re-connect test cases
- spec: more tests for connection establishing
- comply with pg: do not close the client on connection failure
- comply with pg: asynchronous connect_timeout fallbacks to environment variable
- fix: auto re-connect raises an error if the failed connection had unfinished
  transaction state
- yardoc docs

0.2.1

- support for pg >= 0.14 native PG::Result#check
- support for pg >= 0.14 native PG::Connection#set_default_encoding
- fix: connection option Hash argument was modified by Client.new and Client.async_connect

0.2.0

- disabled async_autoreconnect by default unless on_autoreconnect is set
- async_connect sets #internal_encoding to Encoding.default_internal
- fix: finish connection on async connect_timeout
- nice errors generated on missing dependencies
- blocking #reset() should clear async_command_aborted flag
- less calls to #is_busy in Watcher#notify_readable
- async_describe_portal() + specs
- async_describe_prepared() + specs

0.2.0.pre.3

- status() returns CONNECTION_BAD for connections with expired query
- spec: query timeout expiration
- non-blocking result processing for multiple data query statements sent at once
- refine code in em-synchrony/pg
- spec: more detailed tests
- spec: async_connect
- spec: autoreconnect

0.2.0.pre.2

- errors from consume_input fails deferrable
- query_timeout now measures only network response timeout,
  so it's not fired for large datasets

0.2.0.pre.1

- added query_timeout feature for async query commands
- added connect_timeout property for async connect/reset
- fix: async_autoreconnect for tcp/ip connections
- fix: async_* does not raise errors; errors handled by deferrable
- rework async_autoreconnect in fully async manner
- added async_connect() and async_reset()
- API change: on_reconnect -> on_autoreconnect

0.1.1

- added on_reconnect callback
- docs updated
- added development dependency for eventmachine >= 1.0.0.beta.1
- added separate client specs for eventmachine = 0.12.10
- added error checking to eventmachine specs

0.1.0

- first release
