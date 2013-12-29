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

    # == PostgreSQL EventMachine client
    #
    # Author:: Rafal Michalski
    #
    # {PG::EM::Client} is a PG::Connection[http://deveiate.org/code/pg/PG/Connection.html]
    # wrapper designed for EventMachine[http://rubyeventmachine.com/].
    #
    # The following new methods:
    #
    # - {#exec_defer}
    # - {#prepare_defer}
    # - {#exec_prepared_defer}
    # - {#describe_prepared_defer}
    # - {#describe_portal_defer}
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
    #
    # and are now auto-detecting if EventMachine is running and
    # performing commands asynchronously (blocking only current fiber) or
    # are calling parent blocking methods if reactor is off.
    #
    # When {#async_autoreconnect} option is set to +true+, all of the above
    # methods (in asynchronous mode) try to re-connect after a connection error occurs.
    # It's performed behind the scenes, so no error is raised,
    # except if there was a transaction in progress. In such a case the error
    # is raised after establishing connection to signal that
    # the transaction was aborted.
    #
    # If you want to detect auto re-connect event use {#on_autoreconnect} property/option.
    #
    # To enable auto-reconnecting set:
    #   client.async_autoreconnect = true
    #
    # or pass as {new} hash argument:
    #   ::new dbname: 'bar', async_autoreconnect: true
    #
    # There are also new methods for establishing
    # and reseting connections:
    #
    # - {Client.connect_defer}
    # - {#reset_defer}
    #
    # which are asynchronous versions of PG::Connection.new and PG:Connection#reset
    # methods.
    #
    # Additionally the following are overloaded:
    #
    # - {new} (alias: +connect+, +open+, +setdb+, +setdblogin+ )
    # - {#reset}
    #
    # providing auto-detecting asynchronous (fiber blocking) or thread blocking methods for
    # (re)connecting.
    #
    # Otherwise nothing changes in PG::Connection API.
    # See PG::Connection[http://deveiate.org/code/pg/PG/Connection.html] docs
    # for explanation of arguments to the above methods.
    #
    # *Warning:*
    #
    # {#describe_prepared} and {#exec_prepared} after
    # {#prepare} should only be invoked on the *same* connection.
    # If you are using a connection pool, make sure to acquire single connection first.
    #
    class Client < PG::Connection

      ROOT_FIBER = Fiber.current

      # @!attribute connect_timeout
      #   @return [Float] connection timeout in seconds
      #   Connection timeout.
      #   Changing this property does not affect thread-blocking {#reset}.
      #
      #   However if passed as initialization option, it also affects blocking {#reset}.
      #   To enable it set to seconds (> 0). To disable: set to 0.
      attr_accessor :connect_timeout

      # @!attribute query_timeout
      #   @return [Float] query timeout in seconds
      #   Aborts async command processing if waiting for response from server
      #   exceedes +query_timeout+ seconds. This does not apply to
      #   ::new and {#reset}. For them use +connect_timeout+ instead.
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
      #   Changing {#on_autoreconnect} with accessor method doesn't change
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
      #   - If proc returns +true+ (explicitly, truish values are ignored),
      #     the send query command is called regardless of last transaction status.
      #   - If return value is an instance of an Exception, it is passed to
      #     Defferable's +errback+ and the send query command is not invoked at all.
      #   - If return value responds to +callback+ and +errback+ methods,
      #     the send query command will be bound to value's success +callback+
      #     and the original Defferable's +errback+ to value's +errback+.
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

      # environment variable name for connect_timeout option's fallback value
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

      # @!group Deferrable connection methods

      # Attempts to establish the connection asynchronously.
      #
      # @return [FeaturedDeferrable]
      # @yieldparam pg [Client|PG::Error] new and connected client instance on success or an PG::Error
      #
      # Use the returned deferrable's hooks +callback+ to obtain newly created and already connected {Client} object.
      # In case of connection error +errback+ hook is called instead with a raised error object as its argument.
      # If the block is provided it's bound to both +callback+ and +errback+ hooks of the returned deferrable.
      #
      # Special {Client} options (e.g.: {#async_autoreconnect}) must be provided
      # as +connection_hash+ argument variant. They will be ignored in +connection_string+.
      #
      # +client_encoding+ *will* be set for you according to +Encoding.default_internal+.
      #
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-c-new PG::Connection.new
      # @raise [PG::ConnectionBad] if there was an immediate connection error
      #  (it may happen while using unix sockets)
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
        alias_method :async_connect, :connect_defer
      end

      # Attempts to reset the connection asynchronously.
      #
      # @return [FeaturedDeferrable]
      # @yieldparam pg [Client|PG::Error] reconnected client instance on success or an PG::Error
      #
      # Use the returned deferrable's hooks +callback+ and +errback+ to obtain result.
      # If the block is provided it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      # @raise [PG::ConnectionBad] if there was an immediate connection error
      #  (it may happen while using unix sockets)
      def reset_defer(&blk)
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

      alias_method :async_reset, :reset_defer

      # @!endgroup

      # @!group Auto-sensing thread or fiber blocking connection methods

      # Attempts to reset the connection.
      #
      # Performs command asynchronously yielding current fiber
      # if EventMachine reactor is running and current fiber isn't the root fiber.
      # Ensures that other fibers can process while waiting for the server
      # to complete the request.
      #
      # Otherwise performs a blocking call to parent method.
      #
      # @raise [PG::Error]
      #
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-reset PG::Connection#reset
      def reset
        @async_command_aborted = false
        if ::EM.reactor_running? && !(f = Fiber.current).equal?(ROOT_FIBER)
          reset_defer {|r| f.resume(r) }

          conn = Fiber.yield
          raise conn if conn.is_a?(::Exception)
          conn
        else
          super
        end
      end

      # Creates new instance of PG::EM::Client and attempts to establish connection.
      #
      # Performs command asynchronously yielding current fiber
      # if EventMachine reactor is running and current fiber isn't the root fiber.
      # Ensures that other fibers can process while waiting for the server
      # to complete the request.
      #
      # Otherwise performs a blocking call to parent method.
      #
      # @raise [PG::Error]
      #
      # Special {Client} options (e.g.: {#async_autoreconnect}) must be provided
      # as +connection_hash+ argument variant. They will be ignored in +connection_string+.
      #
      # +client_encoding+ *will* be set for you according to +Encoding.default_internal+.
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
          super(*args)
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
      # Detaches watch handler to prevent memory leak then
      # calls parent PG::Connection#finish[http://deveiate.org/code/pg/PG/Connection.html#method-i-finish].
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-finish PG::Connection#finish
      def finish
        super
        if @watcher
          @watcher.detach if @watcher.watching?
          @watcher = nil
        end
      end

      alias_method :close, :finish

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
        # reconnect only if connection is bad and flag is set
        if self.status != PG::CONNECTION_OK && async_autoreconnect
          # check if transaction was active
          was_in_transaction = case @last_transaction_status
          when PG::PQTRANS_IDLE, PG::PQTRANS_UNKNOWN
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
                    if was_in_transaction
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
                  ::EM.next_tick { deferrable.fail returned_df }
                elsif returned_df == false || (was_in_transaction && returned_df != true)
                  # tha handler returned false or raised an exception
                  # or there was an active transaction and handler didn't return true
                  ::EM.next_tick { deferrable.fail error }
                else
                  # try to call failed query command again
                  deferrable.protect(&send_proc)
                end
              end.resume
            elsif was_in_transaction
              # there was a transaction in progress, fail anyway
              ::EM.next_tick { deferrable.fail error }
            else
              # no on_autoreconnect handler, no transaction, then
              # try to call failed query command again
              deferrable.protect(&send_proc)
            end
          end
        else
          # connection is good, or the async_autoreconnect is not set
          ::EM.next_tick { deferrable.fail error }
        end
      end

      # @!group Deferrable command methods

      # @!method exec_defer(sql, params=nil, result_format=nil, &blk)
      #   Sends SQL query request specified by +sql+ to PostgreSQL for asynchronous processing,
      #   and immediately returns with +deferrable+.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an PG::Error
      #   @return [FeaturedDeferrable]
      #   Use the returned deferrable's hooks +callback+ and +errback+ to obtain result.
      #   If the block is provided it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec PG::Connection#exec
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec_params PG::Connection#exec_params
      #
      # @!method prepare_defer(stmt_name, sql, param_types=nil, &blk)
      #   Prepares statement +sql+ with name +stmt_name+ to be executed later asynchronously,
      #   and immediately returns with deferrable.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an PG::Error
      #   @return [FeaturedDeferrable]
      #   Use the returned deferrable's hooks +callback+ and +errback+ to obtain result.
      #   If the block is provided it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-prepare PG::Connection#prepare
      #
      # @!method exec_prepared_defer(statement_name, params=nil, result_format=nil, &blk)
      #   Execute prepared named statement specified by +statement_name+ asynchronously,
      #   and immediately returns with deferrable.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an PG::Error
      #   @return [FeaturedDeferrable]
      #   Use the returned deferrable's hooks +callback+ and +errback+ to obtain result.
      #   If the block is provided it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-send_query_prepared PG::Connection#send_query_prepared
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec_prepared PG::Connection#send_exec_prepared
      #
      # @!method describe_prepared_defer(statement_name, &blk)
      #   Asynchronously sends command to retrieve information about the prepared statement +statement_name+,
      #   and immediately returns with deferrable.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an PG::Error
      #   @return [FeaturedDeferrable]
      #   Use the returned deferrable's hooks +callback+ and +errback+ to obtain result.
      #   If the block is provided it's bound to +callback+ and +errback+ hooks of the returned deferrable.
      #
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-describe_prepared PG::Connection#describe_prepared
      #
      # @!method describe_portal_defer(portal_name, &blk)
      #   Asynchronously sends command to retrieve information about the portal +portal_name+,
      #   and immediately returns with deferrable.
      #
      #   @yieldparam result [PG::Result|Error] command result on success or an PG::Error
      #   @return [FeaturedDeferrable]
      #   Use the returned deferrable's hooks +callback+ and +errback+ to obtain result.
      #   If the block is provided it's bound to +callback+ and +errback+ hooks of the returned deferrable.
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
          df = PG::EM::FeaturedDeferrable.new(&blk)
          send_proc = proc do
            #{send_name}(*args)
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

      alias_method :query_defer,       :exec_defer
      alias_method :async_query_defer, :exec_defer
      alias_method :async_exec_defer,  :exec_defer
      alias_method :exec_params_defer, :exec_defer

      # @!endgroup

      # @!macro auto_synchrony_api
      #   Performs command asynchronously yielding current fiber
      #   if EventMachine reactor is running and current fiber isn't the root fiber.
      #   Ensures that other fibers can process while waiting for the server
      #   to complete the request.
      #
      #   Otherwise performs a blocking call to parent method.
      #
      #   @yieldparam result [PG::Result] command result on success
      #   @return [PG::Result] if block wasn't given
      #   @return [Object] result of the given block
      #   @raise [PG::Error]

      # @!group Auto-sensing thread or fiber blocking command methods

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
      #   Execute prepared named statement specified by +statement_name+.
      #
      #   @macro auto_synchrony_api
      #
      #   @see PG::EM::Client#exec_prepared_defer
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec_prepared PG::Connection#exec_prepared
      #
      # @!method describe_prepared(statement_name, &blk)
      #   Retrieve information about the prepared statement +statement_name+,
      #
      #   @macro auto_synchrony_api
      #
      #   @see PG::EM::Client#describe_prepared_defer
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-describe_prepared PG::Connection#describe_prepared
      #
      # @!method describe_portal(portal_name, &blk)
      #   Retrieve information about the portal +portal_name+,
      #
      #   @macro auto_synchrony_api
      #
      #   @see PG::EM::Client#describe_portal_defer
      #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-describe_portal PG::Connection#describe_portal
      #
      %w(
        exec              exec_defer
        exec_params       exec_defer
        exec_prepared     exec_prepared_defer
        prepare           prepare_defer
        describe_prepared describe_prepared_defer
        describe_portal   describe_portal_defer
        ).each_slice(2) do |name, defer_name|

        class_eval <<-EOD, __FILE__, __LINE__
          def #{name}(*args, &blk)
            if ::EM.reactor_running? && !(f = Fiber.current).equal?(ROOT_FIBER)
              #{defer_name}(*args) do |res|
                f.resume(res)
              end

              result = Fiber.yield
              raise result if result.is_a?(::Exception)
              if block_given?
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
        when PG::PQTRANS_IDLE
          # there is no transaction yet, so let's begin
          exec(TRAN_BEGIN_QUERY)
          # reset transaction count in case user code rolled it back before
          tcount = 0 if tcount != 0
        when PG::PQTRANS_INTRANS
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
          when PG::PQTRANS_INTRANS, PG::PQTRANS_INERROR
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
          when PG::PQTRANS_INTRANS
            # commit only from the outermost transaction block
            exec(TRAN_COMMIT_QUERY) if tcount.zero?
          when PG::PQTRANS_INERROR
            # no ruby error was raised (or an error was rescued in code block)
            # but there was an sql error anyway
            # so rollback after the outermost block
            exec(TRAN_ROLLBACK_QUERY) if tcount.zero?
          when PG::PQTRANS_IDLE
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
