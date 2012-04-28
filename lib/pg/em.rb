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
      
      # +on_reconnect+ is a user defined Proc that is called after a connection
      # with the server has been re-established.
      # It's invoked with +connection+ as first argument and original
      # +exception+ that caused the reconnecting process as second argument.
      #
      # Certain rules should apply to on_reconnect proc:
      #
      # - +async_autoreconnect+ is switched off (do not try to change it from
      #   inside on_reconnect proc).
      # - If proc returns +false+ (explicitly, +nil+ is ignored)
      #   the original +exception+ is raised and the send query command is
      #   not invoked at all.
      # - If return value responds to +callback+ and +errback+ methods
      #   (like +Deferrable+), the send query command will be bound to this
      #   deferrable's success callback. Otherwise the send query command is called
      #   immediately after on_reconnect proc is executed.
      # - Other return values are ignored.
      #
      # You may pass this proc as +:on_reconnect+ option to PG::EM::Client.new.
      #
      # Example:
      #   pg.on_reconnect = proc do |conn, ex|
      #     conn.prepare("birds_by_name", "select id, name from animals order by name where species=$1", ['birds'])
      #   end
      #
      attr_accessor :on_reconnect

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
        @on_reconnect = nil
        if args.last.is_a? Hash
          args.last.reject! do |key, value|
            case key.to_s
            when 'async_autoreconnect'
              @async_autoreconnect = !!value
              true
            when 'on_reconnect'
              @on_reconnect = value if value.respond_to? :call
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
          df = ::EM::DefaultDeferrable.new
          if block_given?
            df.callback(&blk)
            df.errback(&blk)
          end
          begin
            #{send_name}(*args)
          rescue PG::Error => e
            if self.status != PG::CONNECTION_OK && async_autoreconnect
              reset
              if on_reconnect
                begin
                  self.async_autoreconnect = false
                  returned_df = on_reconnect.call(self, e)
                  raise e if returned_df == false
                  if returned_df.respond_to?(:callback) && returned_df.respond_to?(:errback)
                    returned_df.callback do
                      #{send_name}(*args)
                      ::EM.watch(self.socket, Watcher, self, df).notify_readable = true
                    end
                    returned_df.errback do |ex|
                      df.fail(ex)
                    end
                    return df
                  end
                ensure
                  self.async_autoreconnect = true
                end
              end
              #{send_name}(*args) 
            else
              raise e
            end
          end
          ::EM.watch(self.socket, Watcher, self, df).notify_readable = true
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
