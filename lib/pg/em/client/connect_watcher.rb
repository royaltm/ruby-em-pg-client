module PG
  module EM
    class Client < PG::Connection

      # This module is used as a handler to ::EM.watch connection socket and
      # it performs connection handshake with postgres server asynchronously.
      #
      # Author:: Rafal Michalski
      module ConnectWatcher

        def initialize(client, deferrable, is_reset)
          @client = client
          @deferrable = deferrable
          @is_reset = is_reset
          @poll_method = is_reset ? :reset_poll : :connect_poll
          if (timeout = client.connect_timeout) > 0
            @timer = ::EM::Timer.new(timeout) do
              detach
              @deferrable.protect do
                @client.raise_error ConnectionBad, "timeout expired (async)"
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
          @deferrable.protect do
            @client.raise_error ConnectionBad unless polling_ok
            @client.set_default_encoding unless reconnecting?
            if on_connect = @client.on_connect
              succeed_connection_with_hook(on_connect)
            else
              succeed_connection
            end
          end
        end

        def succeed_connection
          ::EM.next_tick { @deferrable.succeed @client }
        end

        def succeed_connection_with_hook(on_connect)
          ::EM.next_tick do
            Fiber.new do
              # call on_connect handler and fail if it raises an error
              begin
                returned_df = on_connect.call(@client, true, @is_reset)
              rescue => ex
                @deferrable.fail ex
              else
                if returned_df.respond_to?(:callback) && returned_df.respond_to?(:errback)
                  # the handler returned a deferrable
                  returned_df.callback { @deferrable.succeed(@client) }
                  # fail when handler's deferrable fails
                  returned_df.errback { |ex| @deferrable.fail ex }
                else
                  @deferrable.succeed @client
                end
              end
            end.resume
          end
        end

        alias_method :notify_writable, :poll_connection_and_check
        alias_method :notify_readable, :poll_connection_and_check

      end

    end
  end
end
