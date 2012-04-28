require 'pg'
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
        begin
          yield
        rescue Exception => e
          ::EM.next_tick { fail(e) }
          fail_value
        end
      end
      
      def protect_and_succeed(fail_value = nil)
        begin
          ret = yield
        rescue Exception => e
          ::EM.next_tick { fail(e) }
          return fail_value
        else
          ::EM.next_tick { succeed(ret) }
        end
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
    #
    # and following:
    #
    # - +exec+ (alias: +query+)
    # - +exec_prepared+
    # - +prepare+
    #
    # which autodetects if EventMachine is running and uses appropriate
    # (async or sync) method version.
    #
    # Async methods might try to reset connection on connection error,
    # you won't even notice that (except for warning message from PG).
    #
    # To disable such behavior set:
    #   client.async_autoreconnect = false
    #
    # or pass as new() hash argument:
    #   PG::EM::Client.new database: 'bar', async_autoreconnect: false
    #
    # Otherwise nothing changes in PG::Connect API.
    # See PG::Connect docs for arguments to above methods.
    #
    # *Warning:*
    #
    # +async_exec_prepared+ after +async_prepare+ should only be invoked on
    # the *same* connection.
    # If you are using connection pool, make sure to acquire single connection first.
    #
    class Client < PG::Connection

      attr_accessor :async_autoreconnect, :connect_timeout
      
      # +on_autoreconnect+ is a user defined Proc that is called after a connection
      # with the server has been re-established.
      # It's invoked with +connection+ as first argument and original
      # +exception+ that caused the reconnecting process as second argument.
      #
      # Certain rules should apply to on_autoreconnect proc:
      #
      # - If proc returns +false+ (explicitly, +nil+ is ignored)
      #   the original +exception+ is passed to +Defferable#fail+ and the send
      #   query command is not invoked at all.
      # - If return value is an instance of exception it is passed to
      #   +Defferable#fail+ and the send query command is not invoked at all.
      # - If return value responds to +callback+ and +errback+ methods
      #   (like +Deferrable+), the send query command will be bound to this
      #   deferrable's success callback. Otherwise the send query command is
      #   called immediately after on_autoreconnect proc is executed.
      # - Other return values are ignored and the send query command is called
      #   immediately after on_autoreconnect proc is executed.
      #
      # You may pass this proc as +:on_autoreconnect+ option to PG::EM::Client.new.
      #
      # Example:
      #   pg.on_autoreconnect = proc do |conn, ex|
      #     conn.prepare("birds_by_name", "select id, name from animals order by name where species=$1", ['birds'])
      #   end
      #
      attr_accessor :on_autoreconnect

      module Watcher
        def initialize(client, deferrable, send_proc)
          @client = client
          @deferrable = deferrable
          @send_proc = send_proc
        end

        def notify_readable
          @client.consume_input
          return if @client.is_busy
          detach
          begin
            result = @client.get_last_result
          rescue PG::Error => e
            @client.async_autoreconnect!(@deferrable, e, &@send_proc)
          rescue Exception => e
            @deferrable.fail(e)
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
              detach
              @deferrable.protect do
                raise PG::Error, "timeout expired (async)"
              end
              client.finish
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
            @client.finish unless success
          end
        end
      end

      def self.parse_async_args(*args)
        async_args = {
          :@async_autoreconnect => true,
          :@connect_timeout => 0,
          :@on_autoreconnect => nil,
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
            end
          end
        end
        async_args
      end

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

      def async_reset(&blk)
        df = PG::EM::FeaturedDeferrable.new(&blk)
        ret = df.protect(:fail) { reset_start }
        unless ret == :fail
          ::EM.watch(self.socket, ConnectWatcher, self, df, :reset).poll_connection_and_check
        end
        df
      end

      def initialize(*args)
        Client.parse_async_args(*args).each {|k, v| self.instance_variable_set(k, v) }
        super(*args)
      end

      def async_autoreconnect!(deferrable, error, &send_proc)
        if async_autoreconnect && (self.finished? || self.status != PG::CONNECTION_OK)
          reset_df = async_reset
          reset_df.errback { |ex| deferrable.fail(ex) }
          reset_df.callback do
            if on_autoreconnect
              returned_df = on_autoreconnect.call(self, error)
              if returned_df == false
                deferrable.fail(error)
              elsif returned_df.respond_to?(:callback) && returned_df.respond_to?(:errback)
                returned_df.callback { deferrable.protect(&send_proc) }
                returned_df.errback { |ex| deferrable.fail(ex) }
              elsif returned_df.is_a?(Exception)
                deferrable.fail(returned_df)
              else
                deferrable.protect(&send_proc)
              end
            else
              deferrable.protect(&send_proc)
            end
          end
        else
          deferrable.fail(error)
        end
      end

      %w(
        exec          send_query
        prepare       send_prepare
        exec_prepared send_query_prepared
          ).each_slice(2) do |name, send_name|

        class_eval <<-EOD
        def async_#{name}(*args, &blk)
          df = PG::EM::FeaturedDeferrable.new(&blk)
          send_proc = proc do
            #{send_name}(*args)
            ::EM.watch(self.socket, Watcher, self, df, send_proc).notify_readable = true
          end
          begin
            send_proc.call
          rescue PG::Error => e
            async_autoreconnect!(df, e, &send_proc)
          end
          df
        rescue Exception => e
          df.fail(e)
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