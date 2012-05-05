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

module PG

  class Result
    class << self
      unless method_defined? :check_result
        # pg_check_result is internal ext function. I wish it was rubified.
        # https://bitbucket.org/ged/ruby-pg/issue/123/rubify-pg_check_result
        def check_result(connection, result)
          if result.nil?
            if (error_message = connection.error_message)
              error = PG::Error.new(error_message)
            end
          else
            case result.result_status
              when PG::PGRES_BAD_RESPONSE,
                   PG::PGRES_FATAL_ERROR,
                   PG::PGRES_NONFATAL_ERROR
                error = PG::Error.new(result.error_message)
                error.instance_variable_set('@result', result)
            end
          end
          if error
            error.instance_variable_set('@connection', connection)
            raise error 
          end
        end
      end
    end
  end

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
        ::EM.next_tick { fail(e) }
        fail_value
      end
      
      def protect_and_succeed(fail_value = nil)
        ret = yield
      rescue Exception => e
        ::EM.next_tick { fail(e) }
        fail_value
      else
        ::EM.next_tick { succeed(ret) }
        ret
      end
    end
    # == PostgreSQL EventMachine client
    #
    # Author:: Rafal Michalski (mailto:royaltm75@gmail.com)
    # Licence:: MIT License
    #
    #
    # PG::EM::Client is a wrapper for PG::Connection which (re)defines methods:
    #
    # - +async_exec+ (alias: +async_query+)
    # - +async_prepare+
    # - +async_exec_prepared+
    # - +async_describe_prepared+
    # - +async_describe_portal+
    #
    # which are suitable to run in EM event loop (they return +Deferrable+)
    #
    # and following:
    #
    # - +exec+ (alias: +query+)
    # - +prepare+
    # - +exec_prepared+
    # - +describe_prepared+
    # - +describe_portal+
    #
    # autodetecting if EventMachine is running and using appropriate
    # (async or sync) method version.
    #
    # Additionally to the above, there are asynchronous methods defined for
    # establishing connection and re-connecting:
    #
    # - +Client.async_connect+
    # - +async_reset+
    #
    # They are async equivalents of +Client.connect+ (which is also aliased
    # by PG::Connection as +new+, +open+, +setdb+, +setdblogin+) and +reset+.
    #
    # Async methods might try to re-connect on connection error.
    # You won't even notice that (except for warning message from PG).
    # If you want to detect such event use +on_autoreconnect+ property.
    #
    # To disable such behavior set:
    #   client.async_autoreconnect = false
    #
    # or pass as new() hash argument:
    #   PG::EM::Client.new database: 'bar', async_autoreconnect: false
    #
    # Otherwise nothing changes in PG::Connection API.
    # See PG::Connection docs for arguments to above methods.
    #
    # *Warning:*
    #
    # +(async_)describe_prepared+ and +(async_)exec_prepared+ after
    # +(async_)prepare+ should only be invoked on the *same* connection.
    # If you are using connection pool, make sure to acquire single connection first.
    #
    class Client < PG::Connection


      # Connection timeout. Changing this property only affects
      # +Client.async_connect+ and +async_reset+.
      # However if passed as initialization option also affects blocking
      # +Client.connect+ and +reset+.
      attr_accessor :connect_timeout

      # *EXPERIMENTAL*
      #
      # Aborts async command processing if waiting for response from server
      # exceedes +query_timeout+ seconds. This does not apply to
      # +Client.async_connect+ and +async_reset+. For those two use
      # +connect_timeout+ instead.
      #
      # To enable it set to seconds (> 0). To disable: set to 0.
      # You can also specify this as initialization option.
      attr_accessor :query_timeout

      # Enable/disable auto-reconnect feature (+true+/+false+).
      # Default is +true+.
      attr_accessor :async_autoreconnect

      # +on_autoreconnect+ is a user defined Proc that is called after a connection
      # with the server has been re-established.
      # It's invoked with +connection+ as first argument and original
      # +exception+ that caused the reconnecting process as second argument.
      #
      # Certain rules should apply to on_autoreconnect proc:
      #
      # - If proc returns +false+ (explicitly, +nil+ is ignored)
      #   the original +exception+ is passed to Defferable's +errback+ and
      #   the send query command is not invoked at all.
      # - If return value is an instance of exception it is passed to
      #   Defferable's +errback+ and the send query command is not invoked at all.
      # - If return value responds to +callback+ and +errback+ methods
      #   the send query command will be bound to value's success +callback+
      #   and the original Defferable's +errback+ or value's +errback+.
      # - Other return values are ignored and the send query command is called
      #   immediately after on_autoreconnect proc is executed.
      #
      # You may pass this proc as +:on_autoreconnect+ option to PG::EM::Client.new.
      #
      # Example:
      #   pg.on_autoreconnect = proc do |conn, ex|
      #     conn.prepare("species_by_name", 
      #      "select id, name from animals where species=$1 order by name")
      #   end
      #
      attr_accessor :on_autoreconnect

      # Used internally for marking connection as aborted on query timeout.
      attr_accessor :async_command_aborted

      module Watcher
        def initialize(client, deferrable, send_proc)
          @last_result = nil
          @client = client
          @deferrable = deferrable
          @send_proc = send_proc
          if (timeout = client.query_timeout) > 0
            @notify_timestamp = Time.now
            setup_timer timeout
          else
            @timer = nil
          end
        end

        def setup_timer(timeout, adjustment = 0)
          @timer = ::EM::Timer.new(timeout - adjustment) do
            if (last_interval = Time.now - @notify_timestamp) >= timeout
              detach
              @client.async_command_aborted = true
              @deferrable.protect do
                raise PG::Error, "query timeout expired (async)"
              end
            else
              setup_timer timeout, last_interval
            end
          end
        end

        def notify_readable
          result = false
          @client.consume_input
          until @client.is_busy
            if (single_result = @client.get_result).nil?
              result = @last_result
              Result.check_result(@client, result)
              detach
              @timer.cancel if @timer
              break
            end
            @last_result.clear if @last_result
            @last_result = single_result
          end
        rescue Exception => e
          detach
          @timer.cancel if @timer
          if e.is_a?(PG::Error)
            @client.async_autoreconnect!(@deferrable, e, &@send_proc)
          else
            @deferrable.fail(e)
          end
        else
          if result == false
            @notify_timestamp = Time.now if @timer
          else
            @deferrable.succeed(result) 
          end
        end
      end

      module ConnectWatcher
        def initialize(client, deferrable, poll_method)
          @client = client
          @deferrable = deferrable
          @poll_method = :"#{poll_method}_poll"
          if (timeout = client.connect_timeout) > 0
            @timer = ::EM::Timer.new(timeout) do
              begin
                detach
                @deferrable.protect do
                  raise PG::Error, "timeout expired (async)"
                end
              ensure
                @client.finish unless poll_method.intern == :reset
              end
            end
          end
        end

        def notify_writable
          poll_connection_and_check
        end

        def notify_readable
          poll_connection_and_check
        end

        def poll_connection_and_check
          case @client.__send__(@poll_method)
          when PG::PGRES_POLLING_READING
            self.notify_readable = true
            self.notify_writable = false
          when PG::PGRES_POLLING_WRITING
            self.notify_writable = true
            self.notify_readable = false
          when PG::PGRES_POLLING_OK, PG::PGRES_POLLING_FAILED
            @timer.cancel if @timer
            detach
            success = @deferrable.protect_and_succeed do
              unless @client.status == PG::CONNECTION_OK
                raise PG::Error, @client.error_message
              end
              @client
            end
          end
        end
      end

      def self.parse_async_args(*args)
        async_args = {
          :@async_autoreconnect => true,
          :@connect_timeout => 0,
          :@query_timeout => 0,
          :@on_autoreconnect => nil,
          :@async_command_aborted => false,
        }
        if args.last.is_a? Hash
          args.last.reject! do |key, value|
            case key.to_s
            when 'async_autoreconnect'
              async_args[:@async_autoreconnect] = !!value
              true
            when 'on_reconnect'
              raise ArgumentError.new("on_reconnect is no longer supported, use on_autoreconnect")
            when 'on_autoreconnect'
              async_args[:@on_autoreconnect] = value if value.respond_to? :call
              true
            when 'connect_timeout'
              async_args[:@connect_timeout] = value.to_f
              false
            when 'query_timeout'
              async_args[:@query_timeout] = value.to_f
              true
            end
          end
        end
        async_args
      end

      # Attempt connection asynchronously.
      # Pass the same arguments as to +Client.new+.
      #
      # Returns +Deferrable+. Use it's +callback+ to obtain newly created and
      # already connected EM::PG::Client object.
      # If block is provided it's bound to +callback+ and +errback+ of returned
      # +Deferrable+.
      def self.async_connect(*args, &blk)
        df = PG::EM::FeaturedDeferrable.new(&blk)
        async_args = parse_async_args(*args)
        conn = df.protect { connect_start(*args) }
        if conn
          async_args.each {|k, v| conn.instance_variable_set(k, v) }
          ::EM.watch(conn.socket, ConnectWatcher, conn, df, :connect).poll_connection_and_check
        end
        df
      end

      # Attempt connection reset asynchronously.
      # There are no arguments.
      #
      # Returns +Deferrable+. Use it's +callback+ to handle success.
      # If block is provided it's bound to +callback+ and +errback+ of returned
      # +Deferrable+.
      def async_reset(&blk)
        @async_command_aborted = false
        df = PG::EM::FeaturedDeferrable.new(&blk)
        ret = df.protect(:fail) { reset_start }
        unless ret == :fail
          ::EM.watch(self.socket, ConnectWatcher, self, df, :reset).poll_connection_and_check
        end
        df
      end

      # Uncheck async_command_aborted on blocking reset.
      def reset
        @async_command_aborted = false
        super
      end

      def initialize(*args)
        Client.parse_async_args(*args).each {|k, v| self.instance_variable_set(k, v) }
        super(*args)
      end

      # Return +CONNECTION_BAD+ for connections with +async_command_aborted+
      # flag set by expired query timeout. Otherwise return whatever
      # PG::Connection#status provides.
      def status
        if @async_command_aborted
          PG::CONNECTION_BAD
        else
          super
        end
      end

      # Perform autoreconnect. Used internally.
      def async_autoreconnect!(deferrable, error, &send_proc)
        if async_autoreconnect && self.status != PG::CONNECTION_OK
          reset_df = async_reset
          reset_df.errback { |ex| deferrable.fail(ex) }
          reset_df.callback do
            if on_autoreconnect
              returned_df = on_autoreconnect.call(self, error)
              if returned_df == false
                ::EM.next_tick { deferrable.fail(error) }
              elsif returned_df.respond_to?(:callback) && returned_df.respond_to?(:errback)
                returned_df.callback { deferrable.protect(&send_proc) }
                returned_df.errback { |ex| deferrable.fail(ex) }
              elsif returned_df.is_a?(Exception)
                ::EM.next_tick { deferrable.fail(returned_df) }
              else
                deferrable.protect(&send_proc)
              end
            else
              deferrable.protect(&send_proc)
            end
          end
        else
          ::EM.next_tick { deferrable.fail(error) }
        end
      end

      %w(
        exec              send_query
        prepare           send_prepare
        exec_prepared     send_query_prepared
        describe_prepared send_describe_prepared
        describe_portal   send_describe_portal
          ).each_slice(2) do |name, send_name|

        class_eval <<-EOD
        def async_#{name}(*args, &blk)
          df = PG::EM::FeaturedDeferrable.new(&blk)
          send_proc = proc do
            #{send_name}(*args)
            ::EM.watch(self.socket, Watcher, self, df, send_proc).notify_readable = true
          end
          begin
            raise PG::Error, "previous query expired, need connection reset" if @async_command_aborted
            send_proc.call
          rescue PG::Error => e
            async_autoreconnect!(df, e, &send_proc)
          rescue Exception => e
            ::EM.next_tick { df.fail(e) }
          end
          df
        end
        EOD

        class_eval <<-EOD
        def #{name}(*args, &blk)
          if ::EM.reactor_running?
            async_#{name}(*args, &blk)
          else
            super(*args, &blk)
          end
        end
        EOD

      end

      alias_method :query, :exec
      alias_method :async_query, :async_exec
    end
  end
end
