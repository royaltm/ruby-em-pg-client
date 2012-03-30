require 'pg'
module PG
  module EM
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

      attr_accessor :async_autoreconnect

      module Watcher
        def initialize(client, deferrable)
          @client = client
          @deferrable = deferrable
        end

        def notify_readable
          @client.consume_input
          return if @client.is_busy
          detach
          begin
            result = @client.get_last_result
          rescue Exception => e
            @deferrable.fail(e)
          else
            @deferrable.succeed(result)
          end
        end
      end

      def initialize(*args)
        @async_autoreconnect = true
        if args.last.is_a? Hash
          args.last.reject! do |key, value|
            if key.to_s == 'async_autoreconnect'
              @async_autoreconnect = !!value
              true
            end
          end
        end
        super(*args)
      end

      %w(
        exec          send_query
        prepare       send_prepare
        exec_prepared send_query_prepared
          ).each_slice(2) do |name, send_name|

        class_eval <<-EOD
        def async_#{name}(*args, &blk)
          begin
            #{send_name}(*args)
          rescue PG::Error => e
            if self.status != PG::CONNECTION_OK && async_autoreconnect
              reset
              #{send_name}(*args)
            else
              raise e
            end
          end
          df = ::EM::DefaultDeferrable.new
          ::EM.watch(self.socket, Watcher, self, df).notify_readable = true
          if block_given?
            df.callback(&blk)
            df.errback(&blk)
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
