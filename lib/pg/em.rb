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

module PG
  module EM

    class FeaturedDeferrable < ::EM::DefaultDeferrable

      def initialize(&blk)
        if block_given?
          callback(&blk)
          errback(&blk)
        end
      end

      def protect(fail_value = nil)
        yield
      rescue Exception => e
        ::EM.next_tick { fail e }
        fail_value
      end

      def protect_and_succeed(fail_value = nil)
        ret = yield
      rescue Exception => e
        ::EM.next_tick { fail e }
        fail_value
      else
        ::EM.next_tick { succeed ret }
        ret
      end
    end
    # == PostgreSQL EventMachine client
    #
    # Author:: Rafal Michalski (mailto:royaltm75@gmail.com)
    # Licence:: MIT License
    #
    #
    # PG::EM::Client is a wrapper for PG::Connection[http://deveiate.org/code/pg/PG/Connection.html]
    # which (re)defines methods:
    #
    # - {#send_query} (aliased as: +async_exec+, +async_query+, +async_exec_params+)
    # - {#send_prepare} (aliased as: +async_prepare+)
    # - {#send_query_prepared} (aliased as: +async_exec_prepared+)
    # - {#send_describe_prepared} (aliased as: +async_describe_prepared+)
    # - {#send_describe_portal} (aliased as: +async_describe_portal+)
    #
    # which are suitable to run in EM event loop (they return +Deferrable+)
    #
    # and following:
    #
    # - +exec+ (alias: +query+)
    # - +exec_params+ (if supported by underlying pg)
    # - +prepare+
    # - +exec_prepared+
    # - +describe_prepared+
    # - +describe_portal+
    #
    # auto-detecting if EventMachine is running and using the appropriate
    # (async or sync) method version.
    #
    # Additionally to the above, there are asynchronous methods defined for
    # establishing connection and re-connecting:
    #
    # - {Client.async_connect}
    # - {#async_reset}
    #
    # They are async equivalents of PG::Connection.new (which is also
    # aliased by PG::Connection as +connect+, +open+, +setdb+, +setdblogin+) and
    # +reset+.
    #
    # When #async_autoreconnect is +true+, async methods might try to
    # re-connect after a connection error. You won't even notice that
    # (except for warning message from PG).
    # If you want to detect such event use #on_autoreconnect property.
    #
    # To enable auto-reconnecting set:
    #   client.async_autoreconnect = true
    #
    # or pass as new() hash argument:
    #   ::new database: 'bar', async_autoreconnect: true
    #
    # Otherwise nothing changes in PG::Connection API.
    # See PG::Connection[http://deveiate.org/code/pg/PG/Connection.html] docs
    # for arguments to above methods.
    #
    # *Warning:*
    #
    # +describe_prepared+ and +exec_prepared+ after
    # +prepare+ should only be invoked on the *same* connection.
    # If you are using a connection pool, make sure to acquire single connection first.
    #
    class Client < PG::Connection

      # @!attribute connect_timeout
      #   @return [Float] connection timeout in seconds
      #   Connection timeout. Changing this property only affects
      #   {async_connect} and {#async_reset}.
      #
      #   However if passed as initialization option, it also affects blocking
      #   +new+ and {#reset}.
      attr_accessor :connect_timeout

      # @!attribute query_timeout
      #   @return [Float] query timeout in seconds
      #   Aborts async command processing if waiting for response from server
      #   exceedes +query_timeout+ seconds. This does not apply to
      #   {async_connect} and {#async_reset}. For them
      #   use +connect_timeout+ instead.
      #
      #   To enable it set to seconds (> 0). To disable: set to 0.
      #   You can also specify this as initialization option.
      attr_accessor :query_timeout

      # @!attribute async_autoreconnect
      #   @return [Boolean] asynchronous auto re-connect status
      #   Enable/disable auto re-connect feature (+true+/+false+).
      #   Defaults to +false+. However it is implicitly set to +true+
      #   if {#on_autoreconnect} is specified as initialization option.
      #
      #   However changing {#on_autoreconnect} with accessor method doesn't change
      #   #async_autoreconnect.
      attr_accessor :async_autoreconnect

      # @!attribute on_autoreconnect
      #   @return [Proc<Client, Error>] auto re-connect hook
      #   +on_autoreconnect+ is a user defined Proc that is called after a connection
      #   with the server has been re-established.
      #   It's invoked with two arguments. First one is the +connection+.
      #   The second is the original +exception+ that caused the reconnecting process.
      #
      #   Certain rules should apply to +#on_autoreconnect+ proc:
      #
      #   - If proc returns +false+ (explicitly, +nil+ is ignored),
      #     the original +exception+ is passed to Defferable's +errback+ and
      #     the send query command is not invoked at all.
      #   - If return value is an instance of exception, it is passed to
      #     Defferable's +errback+ and the send query command is not invoked at all.
      #   - If return value responds to +callback+ and +errback+ methods,
      #     the send query command will be bound to value's success +callback+
      #     and the original Defferable's +errback+ or value's +errback+.
      #   - Other return values are ignored and the send query command is called
      #     immediately after #on_autoreconnect proc is executed.
      #
      #   You may pass this proc as +:on_autoreconnect+ option to ::new.
      #
      #   @example How to use prepare in on_autoreconnect hook
      #     pg.on_autoreconnect = proc do |conn, ex|
      #       conn.prepare("species_by_name", 
      #        "select id, name from animals where species=$1 order by name")
      #     end
      #
      attr_accessor :on_autoreconnect

      # @!visibility private
      # Used internally for marking connection as aborted on query timeout.
      attr_accessor :async_command_aborted

      module Watcher

        def initialize(client)
          @client = client
          @is_connected = true
        end

        def watching?
          @is_connected
        end

        def watch_query(deferrable, send_proc)
          self.notify_readable = true
          @last_result = nil
          @deferrable = deferrable
          @send_proc = send_proc
          @timer.cancel if @timer
          if (timeout = @client.query_timeout) > 0
            @notify_timestamp = Time.now
            setup_timer timeout
          else
            @timer = nil
          end
          self
        end

        def setup_timer(timeout, adjustment = 0)
          @timer = ::EM::Timer.new(timeout - adjustment) do
            if (last_interval = Time.now - @notify_timestamp) >= timeout
              @timer = nil
              self.notify_readable = false
              @client.async_command_aborted = true
              @deferrable.protect do
                error = ConnectionBad.new("query timeout expired (async)")
                error.instance_variable_set(:@connection, @client)
                raise error
              end
            else
              setup_timer timeout, last_interval
            end
          end
        end

        def cancel_timer
          if @timer
            @timer.cancel
            @timer = nil
          end
        end

        def notify_readable
          result = false
          @client.consume_input
          until @client.is_busy
            if (single_result = @client.get_result).nil?
              if (result = @last_result).nil?
                error = Error.new(@client.error_message)
                error.instance_variable_set(:@connection, @client)
                raise error
              end
              result.check
              cancel_timer
              break
            end
            @last_result.clear if @last_result
            @last_result = single_result
          end
        rescue Exception => e
          self.notify_readable = false
          cancel_timer
          if e.is_a?(PG::Error)
            @client.async_autoreconnect!(@deferrable, e, &@send_proc)
          else
            @deferrable.fail(e)
          end
        else
          if result == false
            @notify_timestamp = Time.now if @timer
          else
            self.notify_readable = false
            @deferrable.succeed(result) 
          end
        end

        def unbind
          @is_connected = false
        end
      end

      module ConnectWatcher

        def initialize(client, deferrable, is_reset)
          @client = client
          @deferrable = deferrable
          @is_reset = is_reset
          @poll_method = is_reset ? :reset_poll : :connect_poll
          if (timeout = client.connect_timeout) > 0
            @timer = ::EM::Timer.new(timeout) do
              begin
                detach
                @deferrable.protect do
                  error = ConnectionBad.new("timeout expired (async)")
                  error.instance_variable_set(:@connection, @client)
                  raise error
                end
              ensure
                @client.finish unless reconnecting?
              end
            end
          end
        end

        def reconnecting?
          @is_reset
        end

        def poll_connection_and_check
          case @client.__send__(@poll_method)
          when PG::PGRES_POLLING_READING
            self.notify_readable = true
            self.notify_writable = false
            return
          when PG::PGRES_POLLING_WRITING
            self.notify_writable = true
            self.notify_readable = false
            return
          when PG::PGRES_POLLING_OK
            polling_ok = true if @client.status == PG::CONNECTION_OK
          end
          @timer.cancel if @timer
          detach
          @deferrable.protect_and_succeed do
            unless polling_ok
              begin
                error = ConnectionBad.new(@client.error_message)
                error.instance_variable_set(:@connection, @client)
                raise error
              ensure
                @client.finish unless reconnecting?
              end
            end
            @client.set_default_encoding unless reconnecting?
            @client
          end
        end

        alias_method :notify_writable, :poll_connection_and_check
        alias_method :notify_readable, :poll_connection_and_check

      end

      @@connect_timeout_envvar = conndefaults.find{|d| d[:keyword] == "connect_timeout" }[:envvar]

      # @!visibility private
      def self.parse_async_options(args)
        options = {
          :@async_autoreconnect => nil,
          :@connect_timeout => nil,
          :@query_timeout => 0,
          :@on_autoreconnect => nil,
          :@async_command_aborted => false,
        }
        if args.last.is_a? Hash
          args[-1] = args.last.reject do |key, value|
            case key.to_sym
            when :async_autoreconnect
              options[:@async_autoreconnect] = value
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

      # @!group Asynchronous connection methods

      # Attempts to establish the connection asynchronously.
      #
      # @return [FeaturedDeferrable]
      # @yieldparam pg [Client|Error] new and connected client instance on success or an {Error}
      #
      # Use the returned deferrable's hooks +callback+ and +errback+ to obtain
      # newly created and already connected {Client} object.
      # If the block is provided it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      # Special {Client} options (e.g.: {#async_autoreconnect}) must be provided
      # as +connection_hash+ argument variant. They will be ignored in +connection_string+.
      #
      # +client_encoding+ *will* be set for you according to +Encoding.default_internal+.
      #
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-c-new PG::Connection.new
      # @raise [PG::ConnectionBad] if there was an immediate connection error
      #  (it may happen while using unix sockets)
      def self.async_connect(*args, &blk)
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

      # Attempts to reset the connection asynchronously.
      #
      # @return [FeaturedDeferrable]
      # @yieldparam pg [Client|Error] reconnected client instance on success or an {Error}
      #
      # Use the returned deferrable's hooks +callback+ and +errback+ to obtain result.
      # If the block is provided it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      # @raise [PG::ConnectionBad] if there was an immediate connection error
      #  (it may happen while using unix sockets)
      def async_reset(&blk)
        @async_command_aborted = false
        df = PG::EM::FeaturedDeferrable.new(&blk)
        # there can be only one watch handler over the socket
        # apparently eventmachine has hard time dealing with more than one
        # for blocking reset this is not needed
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

      # Closes the backend connection.
      #
      # Detaches watch handler to prevent memory leak.
      def finish
        super
        if @watcher
          @watcher.detach if @watcher.watching?
          @watcher = nil
        end
      end

      alias_method :close, :finish

      # @!endgroup
      # @!group Synchronous connection methods

      # Resets {#async_command_aborted} on blocking reset then
      # calls original PG::Connection#reset[http://deveiate.org/code/pg/PG/Connection.html#method-i-reset].
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-reset PG::Connection#reset
      def reset
        @async_command_aborted = false
        super
      end

      # Creates new instance of PG::EM::Client and attempts to establish connection synchronously.
      #
      # Special {Client} options (e.g.: {#async_autoreconnect}) must be provided
      # as +connection_hash+ argument variant. They will be ignored in +connection_string+.
      #
      # +em-synchrony+ version *will* do set +client_encoding+ for you according to
      # +Encoding.default_internal+.
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-c-new PG::Connection.new
      def initialize(*args)
        Client.parse_async_options(args).each {|k, v| instance_variable_set(k, v) }
        super(*args)
      end

      # @!endgroup

      # Returns status of connection.
      #
      # @return [Number]
      # Returns +PG::CONNECTION_BAD+ for connections with +async_command_aborted+
      # flag set by expired query timeout. Otherwise return whatever PG::Connection#status returns.
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-status PG::Connection#status
      def status
        if @async_command_aborted
          PG::CONNECTION_BAD
        else
          super
        end
      end

      # @!visibility private
      # Perform auto re-connect. Used internally.
      def async_autoreconnect!(deferrable, error, &send_proc)
        if self.status != PG::CONNECTION_OK
          if async_autoreconnect
            was_in_transaction = case @last_transaction_status
            when PG::PQTRANS_IDLE, PG::PQTRANS_UNKNOWN
              false
            else
              true
            end
            reset_df = async_reset
            reset_df.errback { |ex| deferrable.fail ex }
            reset_df.callback do
              if on_autoreconnect
                returned_df = on_autoreconnect.call(self, error)
                if returned_df.respond_to?(:callback) && returned_df.respond_to?(:errback)
                  returned_df.callback do
                    if was_in_transaction
                      deferrable.fail error
                    else
                      deferrable.protect(&send_proc)
                    end
                  end
                  returned_df.errback { |ex| deferrable.fail ex }
                elsif returned_df.is_a?(Exception)
                  ::EM.next_tick { deferrable.fail returned_df }
                elsif returned_df == false || (was_in_transaction && returned_df != true)
                  ::EM.next_tick { deferrable.fail error }
                else
                  deferrable.protect(&send_proc)
                end
              elsif was_in_transaction
                # after re-connecting transaction is lost anyway
                ::EM.next_tick { deferrable.fail error }
              else
                deferrable.protect(&send_proc)
              end
            end
          else
            ::EM.next_tick { deferrable.fail error }
          end
        else
          ::EM.next_tick { deferrable.fail error }
        end
      end

      # @!group Asynchronous command methods

      # @!method send_query(sql, params=nil, result_format=nil, &blk)
      #   Sends SQL query request specified by +sql+ to PostgreSQL for asynchronous processing,
      #   and immediately returns with +deferrable+.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an {Error}
      #   @return [FeaturedDeferrable]
      #   Use the returned deferrable's hooks +callback+ and +errback+ to obtain result.
      #   If the block is provided it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-send_query PG::Connection#send_query
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec PG::Connection#exec
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec_params PG::Connection#exec_params
      #
      # @!parse alias_method :async_exec, :send_query
      # @!parse alias_method :async_exec_params, :send_query
      # @!parse alias_method :async_query, :send_query
      #
      # @!method send_prepare(stmt_name, sql, param_types=nil, &blk)
      #   Prepares statement +sql+ with name +stmt_name+ to be executed later asynchronously,
      #   and immediately returns with deferrable.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an {Error}
      #   @return [FeaturedDeferrable]
      #   Use the returned deferrable's hooks +callback+ and +errback+ to obtain result.
      #   If the block is provided it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-send_prepare PG::Connection#send_prepare
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-prepare PG::Connection#prepare
      #
      # @!parse alias_method :async_prepare, :send_prepare
      #
      # @!method send_query_prepared(statement_name, params=nil, result_format=nil, &blk)
      #   Execute prepared named statement specified by +statement_name+ asynchronously,
      #   and immediately returns with deferrable.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an {Error}
      #   @return [FeaturedDeferrable]
      #   Use the returned deferrable's hooks +callback+ and +errback+ to obtain result.
      #   If the block is provided it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-send_query_prepared PG::Connection#send_query_prepared
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec_prepared PG::Connection#send_exec_prepared
      #
      # @!parse alias_method :async_exec_prepared, :send_query_prepared
      #
      # @!method send_describe_prepared(statement_name, &blk)
      #   Asynchronously sends command to retrieve information about the prepared statement +statement_name+,
      #   and immediately returns with deferrable.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an {Error}
      #   @return [FeaturedDeferrable]
      #   Use the returned deferrable's hooks +callback+ and +errback+ to obtain result.
      #   If the block is provided it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-send_describe_prepared PG::Connection#send_describe_prepared
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-describe_prepared PG::Connection#describe_prepared
      #
      # @!parse alias_method :async_describe_prepared, :send_describe_prepared
      #
      # @!method send_describe_portal(portal_name, &blk)
      #   Asynchronously sends command to retrieve information about the portal +portal_name+,
      #   and immediately returns with deferrable.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an {Error}
      #   @return [FeaturedDeferrable]
      #   Use the returned deferrable's hooks +callback+ and +errback+ to obtain result.
      #   If the block is provided it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-send_describe_portal PG::Connection#send_describe_portal
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-describe_portal PG::Connection#describe_portal
      #
      # @!parse alias_method :async_describe_portal, :send_describe_portal
      %w(
        send_query
        send_prepare
        send_query_prepared
        send_describe_prepared
        send_describe_portal
          ).each do |send_name|

        class_eval <<-EOD, __FILE__, __LINE__
        def #{send_name}(*args, &blk)
          df = PG::EM::FeaturedDeferrable.new(&blk)
          send_proc = proc do
            super(*args)
            if @watcher && @watcher.watching?
              @watcher.watch_query(df, send_proc)
            else
              @watcher = ::EM.watch(self.socket_io, Watcher, self).
                            watch_query(df, send_proc)
            end
          end
          begin
            if @async_command_aborted
              error = ConnectionBad.new("previous query expired, need connection reset")
              error.instance_variable_set(:@connection, self)
              raise error
            end
            @last_transaction_status = transaction_status
            send_proc.call
          rescue PG::Error => e
            async_autoreconnect!(df, e, &send_proc)
          rescue Exception => e
            ::EM.next_tick { df.fail(e) }
          end
          df
        end
        EOD

      end

      # @!endgroup

      # @!group Auto-sensing sync/async command methods

      # @!method exec(sql, &blk)
      #   Sends SQL query request specified by +sql+ to PostgreSQL.
      #
      #   Returns immediately with deferrable if EM reactor is running, otherwise acts exactly like PG::Connection#exec.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an {Error}
      #   @return [FeaturedDeferrable] if EventMachine reactor is running (asynchronous)
      #   @return [PG::Result] if EventMachine reactor is stopped (synchronous)
      #   If the block is provided and EM reactor is running it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      #   @see #send_query
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec PG::Connection#exec
      #
      # @!method exec_params(sql, params=nil, result_format=nil, &blk)
      #   Sends SQL query request specified by +sql+ with optional +params+ and +result_format+ to PostgreSQL.
      #
      #   Returns immediately with deferrable if EM reactor is running, otherwise acts exactly like PG::Connection#exec_params.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an {Error}
      #   @return [FeaturedDeferrable] if EventMachine reactor is running (asynchronous)
      #   @return [PG::Result] if EventMachine reactor is stopped (synchronous)
      #   If the block is provided and EM reactor is running it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      #   @see #send_query
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec_params PG::Connection#exec_params
      #
      # @!method prepare(stmt_name, sql, param_types=nil, &blk)
      #   Prepares statement +sql+ with name +stmt_name+ to be executed later.
      #
      #   Returns immediately with deferrable if EM reactor is running, otherwise acts exactly like PG::Connection#prepare.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an {Error}
      #   @return [FeaturedDeferrable] if EventMachine reactor is running (asynchronous)
      #   @return [PG::Result] if EventMachine reactor is stopped (synchronous)
      #   If the block is provided and EM reactor is running it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      #   @see #send_prepare
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-prepare PG::Connection#prepare
      #
      # @!method exec_prepared(statement_name, params=nil, result_format=nil, &blk)
      #   Execute prepared named statement specified by +statement_name+.
      #
      #   Returns immediately with deferrable if EM reactor is running, otherwise acts exactly like PG::Connection#exec_prepared.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an {Error}
      #   @return [FeaturedDeferrable] if EventMachine reactor is running (asynchronous)
      #   @return [PG::Result] if EventMachine reactor is stopped (synchronous)
      #   If the block is provided and EM reactor is running it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      #   @see #send_query_prepared
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec_prepared PG::Connection#exec_prepared
      #
      # @!method describe_prepared(statement_name, &blk)
      #   Retrieve information about the prepared statement +statement_name+,
      #
      #   Returns immediately with deferrable if EM reactor is running, otherwise acts exactly like PG::Connection#describe_prepared.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an {Error}
      #   @return [FeaturedDeferrable] if EventMachine reactor is running (asynchronous)
      #   @return [PG::Result] if EventMachine reactor is stopped (synchronous)
      #   If the block is provided and EM reactor is running it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      #   @see #send_describe_prepared
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-describe_prepared PG::Connection#describe_prepared
      #
      # @!method describe_portal(portal_name, &blk)
      #   Retrieve information about the portal +portal_name+,
      #
      #   Returns immediately with deferrable if EM reactor is running, otherwise acts exactly like PG::Connection#describe_portal.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an {Error}
      #   @return [FeaturedDeferrable] if EventMachine reactor is running (asynchronous)
      #   @return [PG::Result] if EventMachine reactor is stopped (synchronous)
      #   If the block is provided and EM reactor is running it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      #   @see #send_describe_portal
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-describe_portal PG::Connection#describe_portal
      %w(
        exec              send_query
        exec_params       send_query
        prepare           send_prepare
        exec_prepared     send_query_prepared
        describe_prepared send_describe_prepared
        describe_portal   send_describe_portal
          ).each_slice(2) do |name, send_name|

        alias_method :"async_#{name}", :"#{send_name}"

        class_eval <<-EOD, __FILE__, __LINE__
        def #{name}(*args, &blk)
          if ::EM.reactor_running?
            async_#{name}(*args, &blk)
          else
            super(*args, &blk)
          end
        end
        EOD

      end

      # @!endgroup

      alias_method :query, :exec
      alias_method :async_query, :send_query

      def transaction
        if ::EM.reactor_running?
          raise NotImplementedError
        else
          super
        end
      end

    end
  end

end
