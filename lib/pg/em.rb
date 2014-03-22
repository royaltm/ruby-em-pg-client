require 'fiber'
begin
  require 'pg'
rescue LoadError => error
  raise 'Missing pg driver: gem install pg'
end
unless defined? EventMachine
  begin
    require 'eventmachine'
  rescue LoadError => error
    raise 'Missing EventMachine: gem install eventmachine'
  end
end
require 'pg/em-version'
require 'pg/em/featured_deferrable'
require 'pg/em/client/watcher'
require 'pg/em/client/connect_watcher'

module PG
  module EM
    ROOT_FIBER = Fiber.current

    # == PostgreSQL EventMachine client
    #
    # Author:: Rafal Michalski
    #
    # {PG::EM::Client} is a PG::Connection[http://deveiate.org/code/pg/PG/Connection.html]
    # wrapper designed for EventMachine[http://rubyeventmachine.com/].
    #
    # The following new methods:
    #
    # - {#exec_defer} (alias: +query_defer+)
    # - {#exec_params_defer}
    # - {#prepare_defer}
    # - {#exec_prepared_defer}
    # - {#describe_prepared_defer}
    # - {#describe_portal_defer}
    # - {#get_result_defer}
    # - {#get_last_result_defer}
    #
    # are added to execute queries asynchronously,
    # returning +Deferrable+ object.
    #
    # The following methods of PG::Connection[http://deveiate.org/code/pg/PG/Connection.html]
    # are overloaded:
    #
    # - {#exec} (alias: +query+, +async_exec+, +async_query+)
    # - {#exec_params}
    # - {#prepare}
    # - {#exec_prepared}
    # - {#describe_prepared}
    # - {#describe_portal}
    # - {#get_result}
    # - {#get_last_result}
    #
    # and are now auto-detecting if EventMachine is running and
    # performing commands asynchronously (blocking only current fiber) or
    # calling parent thread-blocking methods.
    #
    # If {#async_autoreconnect} option is set to +true+, all of the above
    # methods (in asynchronous mode) try to re-connect after a connection
    # error occurs. It's performed behind the scenes, so no error is raised,
    # except if there was a transaction in progress. In such instance the error
    # is raised after establishing connection to signal that
    # the transaction was aborted.
    #
    # If you want to detect auto re-connect event use {#on_autoreconnect}
    # property/option.
    #
    # To enable auto-reconnecting set:
    #   client.async_autoreconnect = true
    #
    # or pass as {new} hash argument:
    #   PG::EM::Client.new dbname: 'bar', async_autoreconnect: true
    #
    # There are also new methods:
    #
    # - {Client.connect_defer}
    # - {#reset_defer}
    #
    # which are asynchronous versions of PG::Connection.new and
    # PG:Connection#reset.
    #
    # Additionally the following methods are overloaded:
    #
    # - {new} (alias: +connect+, +open+, +setdb+, +setdblogin+ )
    # - {#reset}
    #
    # providing auto-detecting asynchronous (fiber-synchronized) or
    # thread-blocking methods for (re)connecting.
    #
    # Otherwise nothing changes in PG::Connection API.
    # See PG::Connection[http://deveiate.org/code/pg/PG/Connection.html] docs
    # for explanation of arguments to the above methods.
    #
    # *Warning:*
    #
    # {#describe_prepared} and {#exec_prepared} after
    # {#prepare} should only be invoked on the *same* connection.
    # If you are using a connection pool, make sure to acquire a single
    # connection first.
    #
    class Client < PG::Connection

      # @!attribute connect_timeout
      #   @return [Float] connection timeout in seconds
      #   Connection timeout. Affects {#reset} and {#reset_defer}.
      #
      #   Changing this property does not affect thread-blocking {#reset}.
      #
      #   However if passed as initialization option, it also affects blocking
      #   {#reset}.
      #
      #   To enable it set to some positive value. To disable it: set to 0.
      #   You can also specify this as an option to {new} or {connect_defer}.
      attr_accessor :connect_timeout

      # @!attribute query_timeout
      #   @return [Float] query timeout in seconds
      #   Aborts async command processing if server response time
      #   exceedes +query_timeout+ seconds. This does not apply to
      #   {#reset} and {#reset_defer}.
      #
      #   To enable it set to some positive value. To disable it: set to 0.
      #   You can also specify this as an option to {new} or {connect_defer}.
      attr_accessor :query_timeout

      # @!attribute async_autoreconnect
      #   @return [Boolean] asynchronous auto re-connect status
      #   Enable/disable auto re-connect feature (+true+/+false+).
      #   Defaults to +false+ unless {#on_autoreconnect} is specified
      #   as an initialization option.
      #
      #   Changing {#on_autoreconnect} with accessor method doesn't change
      #   the state of {#async_autoreconnect}.
      #
      #   You can also specify this as an option to {new} or {connect_defer}.
      attr_accessor :async_autoreconnect

      # @!attribute on_autoreconnect
      #   @return [Proc<Client, Error>] auto re-connect hook
      #   Proc that is called after a connection with the server has been
      #   automatically re-established. It's being invoked just before the
      #   pending command is sent to the server.
      #
      #   The first argument it receives is the +connection+ instance.
      #   The second is the original +exception+ that caused the reconnecting
      #   process.
      #
      #   If exception is raised during execution of the on_autoreconnect proc
      #   the reset operation will fail with that exception.
      #
      #   It's possible to execute queries from inside of the proc.
      #   The proc is being wrapped in a fiber, so both deferrable and
      #   fiber-synchronized query commands may be used.
      #
      #   The proc can control the later action with its return value:
      #
      #   - +false+ (explicitly, +nil+ is ignored) - the original +exception+
      #     is raised/passed back and the pending query command is not sent
      #     again to the server.
      #   - +true+ (explicitly, truish values are ignored), the pending command
      #     is called regardless of the connection's last transaction status.
      #   - Exception object - is raised/passed back and the pending command
      #     is not sent.
      #   - Deferrable object - the chosen action will depend on the deferred
      #     status.
      #   - Other values are ignored and the pending query command is
      #     immediately sent to the server unless there was a pending
      #     transaction before the connection was reset.
      #
      #   If both +on_connect+ and +on_autoreconnect+ hooks are set,
      #   the +on_connect+ is being called first and +on_autoreconnect+ is
      #   called only when +on_connect+ succeeds.
      #
      #   You may pass this proc as an option to {new} or {connect_defer}.
      #
      #   @example How to use deferrable in on_autoreconnect hook
      #     pg.on_autoreconnect = proc do |conn, ex|
      #       logger.warn "PG connection was reset: #{ex.inspect}, delaying 1 sec."
      #       EM::DefaultDeferrable.new.tap do |df|
      #         EM.add_timer(1) { df.succeed }
      #       end
      #     end
      #
      attr_writer :on_autoreconnect

      def on_autoreconnect(&hook)
        if block_given?
          @on_autoreconnect = hook
        else
          @on_autoreconnect
        end
      end

      # @!attribute on_connect
      #   @return [Proc<Client,is_async,is_reset>] connect hook
      #   Proc that is called after a connection with the server has been
      #   established.
      #
      #   The first argument it receives is the +connection+ instance.
      #   The second argument is +true+ if the connection was established in
      #   asynchronous manner, +false+ otherwise.
      #   The third argument is +true+ when the connection has been reset or
      #   +false+ on new connection.
      #
      #   It's possible to execute queries from inside of the proc.
      #   The proc is being wrapped in a fiber, so both deferrable and
      #   fiber-synchronized query commands may be used. However asynchronous
      #   deferrable commands are only allowed while eventmachine reactor
      #   is running, so check if +is_async+ argument is +true+.
      #
      #   If exception is raised during execution of the on_connect proc
      #   the connecting/reset operation will fail with that exception.
      #
      #   The proc can control the later action with its return value:
      #
      #   - Deferrable object - the connection establishing status will depend
      #     on the deferred status (only in asynchronous mode).
      #   - Other values are ignored.
      #
      #   If both +on_connect+ and +on_autoreconnect+ hooks are set,
      #   the +on_connect+ is being called first and +on_autoreconnect+ is
      #   called only when +on_connect+ succeeds.
      #
      #   You may pass this proc as an option to {new} or {connect_defer}.
      #
      #   @example How to use prepare in on_connect hook
      #     pg.on_connect = proc do |conn|
      #       conn.prepare("species_by_name", 
      #        "select id, name from animals where species=$1 order by name")
      #     end
      #
      attr_writer :on_connect

      def on_connect(&hook)
        if block_given?
          @on_connect = hook
        else
          @on_connect
        end
      end

      # @!visibility private
      # Used internally for marking connection as aborted on query timeout.
      attr_accessor :async_command_aborted

      # Returns +true+ if +pg+ supports single row mode or +false+ otherwise.
      # Single row mode is available since +libpq+ 9.2.
      # @return [Boolean]
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-set_single_row_mode PG::Connection#set_single_row_mode
      def self.single_row_mode?
        method_defined? :set_single_row_mode
      end

      # Returns +true+ if +pg+ supports single row mode or +false+ otherwise.
      # @return [Boolean]
      # @see single_row_mode?
      def single_row_mode?
        self.class.single_row_mode?
      end

      # environment variable name for connect_timeout fallback value
      @@connect_timeout_envvar = conndefaults.find{|d| d[:keyword] == "connect_timeout" }[:envvar]

      DEFAULT_ASYNC_VARS = {
        :@async_autoreconnect => nil,
        :@connect_timeout => nil,
        :@query_timeout => 0,
        :@on_connect => nil,
        :@on_autoreconnect => nil,
        :@async_command_aborted => false,
      }.freeze

      # @!visibility private
      def self.parse_async_options(args)
        options = DEFAULT_ASYNC_VARS.dup
        if args.last.is_a? Hash
          args[-1] = args.last.reject do |key, value|
            case key.to_sym
            when :async_autoreconnect
              options[:@async_autoreconnect] = value
              true
            when :on_connect
              if value.respond_to? :call
                options[:@on_connect] = value
              else
                raise ArgumentError, "on_connect must respond to `call'"
              end
              true
            when :on_reconnect
              raise ArgumentError, "on_reconnect is no longer supported, use on_autoreconnect"
            when :on_autoreconnect
              if value.respond_to? :call
                options[:@on_autoreconnect] = value
                options[:@async_autoreconnect] = true if options[:@async_autoreconnect].nil?
              else
                raise ArgumentError, "on_autoreconnect must respond to `call'"
              end
              true
            when :connect_timeout
              options[:@connect_timeout] = value.to_f
              false
            when :query_timeout
              options[:@query_timeout] = value.to_f
              true
            end
          end
        end
        options[:@async_autoreconnect] = !!options[:@async_autoreconnect]
        options[:@connect_timeout] ||= ENV[@@connect_timeout_envvar].to_f
        options
      end

      # @!group Deferrable connection methods

      # Attempts to establish the connection asynchronously.
      #
      # @return [FeaturedDeferrable]
      # @yieldparam pg [Client|PG::Error] new and connected client instance on
      #                             success or an instance of raised PG::Error
      #
      # Pass the block to the returned deferrable's +callback+ to obtain newly
      # created and already connected {Client} object. In case of connection
      # error +errback+ hook receives an error object as an argument.
      # If the block is provided it's bound to both +callback+ and +errback+
      # hooks of the returned deferrable.
      #
      # Special {Client} options (e.g.: {#async_autoreconnect}) must be
      # provided as +connection_hash+ argument variant. They will be ignored
      # if passed as a +connection_string+.
      #
      # +client_encoding+ *will* be set according to +Encoding.default_internal+.
      #
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-c-new PG::Connection.new
      def self.connect_defer(*args, &blk)
        df = PG::EM::FeaturedDeferrable.new(&blk)
        async_args = parse_async_options(args)
        conn = df.protect { connect_start(*args) }
        if conn
          async_args.each {|k, v| conn.instance_variable_set(k, v) }
          ::EM.watch(conn.socket_io, ConnectWatcher, conn, df, false).
            poll_connection_and_check
        end
        df
      end

      class << self
        # @deprecated Use {connect_defer} instead.
        alias_method :async_connect, :connect_defer
      end

      # Attempts to reset the connection asynchronously.
      #
      # @return [FeaturedDeferrable]
      # @yieldparam pg [Client|PG::Error] reconnected client instance on
      #                             success or an instance of raised PG::Error
      #
      # Pass the block to the returned deferrable's +callback+ to execute
      # after successfull reset.
      # If the block is provided it's bound to +callback+ and +errback+ hooks
      # of the returned deferrable.
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-reset PG::Connection#reset
      def reset_defer(&blk)
        @async_command_aborted = false
        df = FeaturedDeferrable.new(&blk)
        # there can be only one watch handler over the socket
        # apparently eventmachine has hard time dealing with more than one
        if @watcher
          @watcher.detach if @watcher.watching?
          @watcher = nil
        end
        ret = df.protect(:fail) { reset_start }
        unless ret == :fail
          ::EM.watch(self.socket_io, ConnectWatcher, self, df, true).
            poll_connection_and_check
        end
        df
      end

      # @deprecated Use {reset_defer} instead.
      alias_method :async_reset, :reset_defer

      # @!endgroup

      # @!group Auto-sensing fiber-synchronized connection methods

      # Attempts to reset the connection.
      #
      # Performs command asynchronously yielding from current fiber
      # if EventMachine reactor is running and current fiber isn't the root
      # fiber. Other fibers can process while waiting for the server to
      # complete the request.
      #
      # Otherwise performs a thread-blocking call to the parent method.
      #
      # @raise [PG::Error]
      # @see #reset_defer
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-reset PG::Connection#reset
      def reset
        if ::EM.reactor_running? && !(f = Fiber.current).equal?(ROOT_FIBER)
          reset_defer {|r| f.resume(r) }

          conn = Fiber.yield
          raise conn if conn.is_a?(::Exception)
          conn
        else
          @async_command_aborted = false
          if @watcher
            @watcher.detach if @watcher.watching?
            @watcher = nil
          end
          super
          @on_connect.call(self, false, true) if @on_connect
          self
        end
      end

      # Creates new instance of PG::EM::Client and attempts to establish
      # connection.
      #
      # Performs command asynchronously yielding from current fiber
      # if EventMachine reactor is running and current fiber isn't the root
      # fiber. Other fibers can process while waiting for the server to
      # complete the request.
      #
      # Otherwise performs a thread-blocking call to the parent method.
      #
      # @raise [PG::Error]
      #
      # Special {Client} options (e.g.: {#async_autoreconnect}) must be
      # provided as +connection_hash+ argument variant. They will be ignored
      # if passed as a +connection_string+.
      #
      # +client_encoding+ *will* be set according to +Encoding.default_internal+.
      #
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-c-new PG::Connection.new
      def self.new(*args, &blk)
        if ::EM.reactor_running? && !(f = Fiber.current).equal?(ROOT_FIBER)
          connect_defer(*args) {|r| f.resume(r) }

          conn = Fiber.yield
          raise conn if conn.is_a?(::Exception)
          if block_given?
            begin
              yield conn
            ensure
              conn.finish
            end
          else
            conn
          end
        else
          conn = super(*args)
          if on_connect = conn.on_connect
            on_connect.call(conn, false, false)
          end
          conn
        end
      end

      # @!visibility private
      def initialize(*args)
        Client.parse_async_options(args).each {|k, v| instance_variable_set(k, v) }
        super(*args)
      end

      class << self
        alias_method :connect,    :new
        alias_method :open,       :new
        alias_method :setdb,      :new
        alias_method :setdblogin, :new
      end

      # @!endgroup

      # Closes the backend connection.
      #
      # Detaches watch handler to prevent memory leak after
      # calling parent PG::Connection#finish[http://deveiate.org/code/pg/PG/Connection.html#method-i-finish].
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-finish PG::Connection#finish
      def finish
        super
        if @watcher
          @watcher.detach if @watcher.watching?
          @watcher = nil
        end
      end

      alias_method :close, :finish

      # Returns status of connection: PG::CONNECTION_OK or PG::CONNECTION_BAD.
      #
      # @return [Number]
      # Returns +PG::CONNECTION_BAD+ for connections with +async_command_aborted+
      # flag set by expired query timeout. Otherwise return whatever PG::Connection#status returns.
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-status PG::Connection#status
      def status
        if @async_command_aborted
          CONNECTION_BAD
        else
          super
        end
      end

      # @!visibility private
      # Perform auto re-connect. Used internally.
      def async_autoreconnect!(deferrable, error, &send_proc)
        # reconnect only if connection is bad and flag is set
        if self.status == CONNECTION_BAD && async_autoreconnect
          # check if transaction was active
          was_in_transaction = case @last_transaction_status
          when PQTRANS_IDLE, PQTRANS_UNKNOWN
            false
          else
            true
          end
          # reset asynchronously
          reset_df = reset_defer
          # just fail on reset failure
          reset_df.errback { |ex| deferrable.fail ex }
          # reset succeeds
          reset_df.callback do
            # handle on_autoreconnect
            if on_autoreconnect
              # wrap in a fiber, so on_autoreconnect code may yield from it
              Fiber.new do
                # call on_autoreconnect handler and fail if it raises an error
                returned_df = begin
                  on_autoreconnect.call(self, error)
                rescue => ex
                  ex
                end
                if returned_df.respond_to?(:callback) && returned_df.respond_to?(:errback)
                  # the handler returned a deferrable
                  returned_df.callback do
                    if was_in_transaction || !send_proc
                      # there was a transaction in progress, fail anyway
                      deferrable.fail error
                    else
                      # try to call failed query command again
                      deferrable.protect(&send_proc)
                    end
                  end
                  # fail when handler's deferrable fails
                  returned_df.errback { |ex| deferrable.fail ex }
                elsif returned_df.is_a?(Exception)
                  # tha handler returned an exception object, so fail with it
                  deferrable.fail returned_df
                elsif returned_df == false || !send_proc || (was_in_transaction && returned_df != true)
                  # tha handler returned false or raised an exception
                  # or there was an active transaction and handler didn't return true
                  deferrable.fail error
                else
                  # try to call failed query command again
                  deferrable.protect(&send_proc)
                end
              end.resume
            elsif was_in_transaction || !send_proc
              # there was a transaction in progress, fail anyway
              deferrable.fail error
            else
              # no on_autoreconnect handler, no transaction, then
              # try to call failed query command again
              deferrable.protect(&send_proc)
            end
          end
        else
          # connection is good, or the async_autoreconnect is not set
          deferrable.fail error
        end
      end

      # @!macro deferrable_api
      #   @return [FeaturedDeferrable]
      #   Use the returned Deferrable's +callback+ and +errback+ methods to
      #   get the result. If the block is provided it's bound to both the
      #   +callback+ and +errback+ hooks of the returned deferrable.

      # @!macro deferrable_query_api
      #   @yieldparam result [PG::Result|Error] command result on success or a PG::Error instance on error.
      #   @macro deferrable_api

      # @!group Deferrable command methods

      # @!method exec_defer(sql, params=nil, result_format=nil, &blk)
      #   Sends SQL query request specified by +sql+ to PostgreSQL for asynchronous processing,
      #   and immediately returns with +deferrable+.
      #
      #   @macro deferrable_query_api
      #
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec PG::Connection#exec
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec_params PG::Connection#exec_params
      #
      # @!method prepare_defer(stmt_name, sql, param_types=nil, &blk)
      #   Prepares statement +sql+ with name +stmt_name+ to be executed later asynchronously,
      #   and immediately returns with a Deferrable.
      #
      #   @macro deferrable_query_api
      #
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-prepare PG::Connection#prepare
      #
      # @!method exec_prepared_defer(statement_name, params=nil, result_format=nil, &blk)
      #   Execute prepared named statement specified by +statement_name+ asynchronously,
      #   and immediately returns with a Deferrable.
      #
      #   @macro deferrable_query_api
      #
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-send_query_prepared PG::Connection#send_query_prepared
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec_prepared PG::Connection#send_exec_prepared
      #
      # @!method describe_prepared_defer(statement_name, &blk)
      #   Asynchronously sends command to retrieve information about the prepared statement +statement_name+,
      #   and immediately returns with a Deferrable.
      #
      #   @macro deferrable_query_api
      #
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-describe_prepared PG::Connection#describe_prepared
      #
      # @!method describe_portal_defer(portal_name, &blk)
      #   Asynchronously sends command to retrieve information about the portal +portal_name+,
      #   and immediately returns with a Deferrable.
      #
      #   @macro deferrable_query_api
      #
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-describe_portal PG::Connection#describe_portal
      #
      %w(
        exec_defer              send_query
        prepare_defer           send_prepare
        exec_prepared_defer     send_query_prepared
        describe_prepared_defer send_describe_prepared
        describe_portal_defer   send_describe_portal
      ).each_slice(2) do |defer_name, send_name|

        class_eval <<-EOD, __FILE__, __LINE__
        def #{defer_name}(*args, &blk)
          df = FeaturedDeferrable.new(&blk)
          send_proc = proc do
            #{send_name}(*args)
            setup_emio_watcher.watch_results(df, send_proc)
          end
          begin
            check_async_command_aborted!
            @last_transaction_status = transaction_status
            send_proc.call
          rescue Error => e
            ::EM.next_tick { async_autoreconnect!(df, e, &send_proc) }
          rescue Exception => e
            ::EM.next_tick { df.fail(e) }
          end
          df
        end
        EOD

      end

      alias_method :query_defer,       :exec_defer
      alias_method :async_query_defer, :exec_defer
      alias_method :async_exec_defer,  :exec_defer
      alias_method :exec_params_defer, :exec_defer

      # Asynchronously retrieves the next result from a call to
      # #send_query (or another asynchronous command) and immediately
      # returns with a Deferrable.
      # It then receives the result object on :succeed, or +nil+
      # if no results are available.
      #
      # @macro deferrable_api
      # @yieldparam result [PG::Result|Error|nil] command result on success or a PG::Error instance on error
      #                                             or +nil+ if no results are available.
      #
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-send_query PG::Connection#send_query
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-get_result PG::Connection#get_result
      #
      def get_result_defer(&blk)
        begin
          df = FeaturedDeferrable.new(&blk)
          if status == CONNECTION_OK
            if is_busy
              check_async_command_aborted!
              setup_emio_watcher.watch_results(df, nil, true)
            else
              df.succeed blocking_get_result
            end
          else
            df.succeed
          end
        rescue Error => e
          ::EM.next_tick { async_autoreconnect!(df, e) }
        rescue Exception => e
          ::EM.next_tick { df.fail(e) }
        end
        df
      end

      # Asynchronously retrieves all available results on the current
      # connection (from previously issued asynchronous commands like
      # +send_query()+) and immediately returns with a Deferrable.
      # It then receives the last non-NULL result on :succeed, or +nil+
      # if no results are available.
      #
      # @macro deferrable_api
      # @yieldparam result [PG::Result|Error|nil] command result on success or a PG::Error instance on error
      #                                             or +nil+ if no results are available.
      #
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-send_query PG::Connection#send_query
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-get_last_result PG::Connection#get_last_result
      #
      def get_last_result_defer(&blk)
        begin
          df = FeaturedDeferrable.new(&blk)
          if status == CONNECTION_OK
            check_async_command_aborted!
            setup_emio_watcher.watch_results(df)
          else
            df.succeed
          end
        rescue Error => e
          ::EM.next_tick { async_autoreconnect!(df, e) }
        rescue Exception => e
          ::EM.next_tick { df.fail(e) }
        end
        df
      end

      # @!endgroup

      alias_method :blocking_get_result, :get_result

      def raise_error(klass=Error, message=error_message)
        error = klass.new(message)
        error.instance_variable_set(:@connection, self)
        raise error
      end

      private

      def fiber_sync(df, fiber)
        f = nil
        df.completion do |res|
          if f then f.resume res else return res end
        end
        f = fiber
        Fiber.yield
      end

      def check_async_command_aborted!
        if @async_command_aborted
          raise_error ConnectionBad, "previous query expired, need connection reset"
        end
      end

      def setup_emio_watcher
        if @watcher && @watcher.watching?
          @watcher
        else
          @watcher = ::EM.watch(self.socket_io, Watcher, self)
        end
      end

      public

      # @!macro auto_synchrony_api_intro
      #   If EventMachine reactor is running and the current fiber isn't the
      #   root fiber this method performs command asynchronously yielding
      #   current fiber. Other fibers can process while waiting for the server
      #   to complete the request.
      #
      #   Otherwise performs a blocking call to a parent method.
      #
      #   @yieldparam result [PG::Result] command result on success
      #   @raise [PG::Error]

      # @!macro auto_synchrony_api
      #   @macro auto_synchrony_api_intro
      #   @return [PG::Result] if block wasn't given
      #   @return [Object] result of the given block

      # @!group Auto-sensing fiber-synchronized command methods

      # @!method exec(sql, &blk)
      #   Sends SQL query request specified by +sql+ to PostgreSQL.
      #
      #   @macro auto_synchrony_api
      #
      #   @see PG::EM::Client#exec_defer
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec PG::Connection#exec
      #
      # @!method exec_params(sql, params=nil, result_format=nil, &blk)
      #   Sends SQL query request specified by +sql+ with optional +params+ and +result_format+ to PostgreSQL.
      #
      #   @macro auto_synchrony_api
      #
      #   @see PG::EM::Client#exec_params_defer
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec_params PG::Connection#exec_params
      #
      # @!method prepare(stmt_name, sql, param_types=nil, &blk)
      #   Prepares statement +sql+ with name +stmt_name+ to be executed later.
      #
      #   @macro auto_synchrony_api
      #
      #   @see PG::EM::Client#prepare_defer
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-prepare PG::Connection#prepare
      #
      # @!method exec_prepared(statement_name, params=nil, result_format=nil, &blk)
      #   Executes prepared named statement specified by +statement_name+.
      #
      #   @macro auto_synchrony_api
      #
      #   @see PG::EM::Client#exec_prepared_defer
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec_prepared PG::Connection#exec_prepared
      #
      # @!method describe_prepared(statement_name, &blk)
      #   Retrieves information about the prepared statement +statement_name+,
      #
      #   @macro auto_synchrony_api
      #
      #   @see PG::EM::Client#describe_prepared_defer
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-describe_prepared PG::Connection#describe_prepared
      #
      # @!method describe_portal(portal_name, &blk)
      #   Retrieves information about the portal +portal_name+,
      #
      #   @macro auto_synchrony_api
      #
      #   @see PG::EM::Client#describe_portal_defer
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-describe_portal PG::Connection#describe_portal
      #
      # @!method get_last_result
      #   Retrieves all available results on the current connection
      #   (from previously issued asynchronous commands like +send_query()+)
      #   and returns the last non-NULL result, or +nil+ if no results are
      #   available.
      #
      #   @macro auto_synchrony_api
      #   @return [nil] if no more results
      #
      #   @see #get_last_result_defer
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-get_last_result PG::Connection#get_last_result
      %w(
        exec              exec_defer
        exec_params       exec_defer
        exec_prepared     exec_prepared_defer
        prepare           prepare_defer
        describe_prepared describe_prepared_defer
        describe_portal   describe_portal_defer
        get_last_result   get_last_result_defer
        ).each_slice(2) do |name, defer_name|

        class_eval <<-EOD, __FILE__, __LINE__
          def #{name}(*args, &blk)
            if ::EM.reactor_running? && !(f = Fiber.current).equal?(ROOT_FIBER)
              if (result = fiber_sync #{defer_name}(*args), f).is_a?(::Exception)
                raise result
              end
              if block_given? && result
                begin
                  yield result
                ensure
                  result.clear
                end
              else
                result
              end
            else
              super
            end
          end
        EOD
      end

      alias_method :query,       :exec
      alias_method :async_query, :exec
      alias_method :async_exec,  :exec

      # Retrieves the next result from a call to #send_query (or another
      # asynchronous command). If no more results are available returns
      # +nil+ and the block (if given) is never called.
      #
      # @macro auto_synchrony_api
      # @return [nil] if no more results
      #
      # @see #get_result_defer
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-get_result PG::Connection#get_result
      def get_result
        if is_busy && ::EM.reactor_running? && !(f = Fiber.current).equal?(ROOT_FIBER)
          if (result = fiber_sync get_result_defer, f).is_a?(::Exception)
            raise result
          end
          if block_given? && result
            begin
              yield result
            ensure
              result.clear
            end
          else
            result
          end
        else
          super
        end
      end

      # @!endgroup


      TRAN_BEGIN_QUERY = 'BEGIN'
      TRAN_ROLLBACK_QUERY = 'ROLLBACK'
      TRAN_COMMIT_QUERY = 'COMMIT'

      # Executes a BEGIN at the start of the block and a COMMIT at the end
      # of the block or ROLLBACK if any exception occurs.
      #
      # @note Avoid using PG::EM::Client#*_defer calls inside the block or make sure
      #       all queries are completed before the provided block terminates.
      # @return [Object] result of the block
      # @yieldparam client [self]
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-transaction PG::Connection#transaction
      #
      # Calls to {#transaction} may be nested, however without sub-transactions
      # (save points). If the innermost transaction block raises an error
      # the transaction is rolled back to the state before the outermost
      # transaction began.
      #
      # This is an extension to the +PG::Connection#transaction+ method
      # as it does not support nesting in this way.
      #
      # The method is sensitive to the transaction status and will safely
      # rollback on any sql error even when it was catched by some rescue block.
      # But consider that rescuing any sql error within an utility method
      # is a bad idea.
      #
      # This method works in both blocking/async modes (regardles of the reactor state)
      # and is considered as a generic extension to the +PG::Connection#transaction+
      # method.
      #
      # @example Nested transaction example
      #  def add_comment(user_id, text)
      #    db.transaction do
      #      cmt_id = db.query(
      #        'insert into comments (text) where user_id=$1 values ($2) returning id',
      #        [user_id, text]).getvalue(0,0)
      #      db.query(
      #        'update users set last_comment_id=$2 where id=$1', [user_id, cmt_id])
      #      cmt_id
      #    end
      #  end
      #  
      #  def update_comment_count(page_id)
      #    db.transaction do
      #      count = db.query('select count(*) from comments where page_id=$1', [page_id]).getvalue(0,0)
      #      db.query('update pages set comment_count=$2 where id=$1', [page_id, count])
      #    end
      #  end
      #
      #  # to run add_comment and update_comment_count within the same transaction
      #  db.transaction do
      #    add_comment(user_id, some_text)
      #    update_comment_count(page_id)
      #  end
      #
      def transaction
        raise ArgumentError, 'Must supply block for PG::EM::Client#transaction' unless block_given?
        tcount = @client_tran_count.to_i

        case transaction_status
        when PQTRANS_IDLE
          # there is no transaction yet, so let's begin
          exec(TRAN_BEGIN_QUERY)
          # reset transaction count in case user code rolled it back before
          tcount = 0 if tcount != 0
        when PQTRANS_INTRANS
          # transaction in progress, leave it be
        else
          # transaction failed, is in unknown state or command is active
          # in any case calling begin will raise server transaction error
          exec(TRAN_BEGIN_QUERY) # raises PG::InFailedSqlTransaction
        end
        # memoize nested count
        @client_tran_count = tcount + 1
        begin

          result = yield self

        rescue
          # error was raised
          case transaction_status
          when PQTRANS_INTRANS, PQTRANS_INERROR
            # do not rollback if transaction was rolled back before
            # or is in unknown state, which means connection reset is needed
            # and rollback only from the outermost transaction block
            exec(TRAN_ROLLBACK_QUERY) if tcount.zero?
          end
          # raise again
          raise
        else
          # we are good (but not out of woods yet)
          case transaction_status
          when PQTRANS_INTRANS
            # commit only from the outermost transaction block
            exec(TRAN_COMMIT_QUERY) if tcount.zero?
          when PQTRANS_INERROR
            # no ruby error was raised (or an error was rescued in code block)
            # but there was an sql error anyway
            # so rollback after the outermost block
            exec(TRAN_ROLLBACK_QUERY) if tcount.zero?
          when PQTRANS_IDLE
            # the code block has terminated the transaction on its own
            # so just reset the counter
            tcount = 0
          else
            # something isn't right, so provoke an error just in case
            exec(TRAN_ROLLBACK_QUERY) if tcount.zero?
          end
          result
        ensure
          @client_tran_count = tcount
        end
      end

    end
  end

end
