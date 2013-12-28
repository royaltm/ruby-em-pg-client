module PG
  module EM
    class Client < PG::Connection

      # This module is used as a handler to ::EM.watch connection socket and
      # performs connection handshake with postgres server in asynchronous manner.
      #
      # Author:: Rafal Michalski (mailto:royaltm75@gmail.com)
      # Licence:: MIT License
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
                error = ConnectionBad.new("timeout expired (async)")
                error.instance_variable_set(:@connection, @client)
                raise error
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
              error = ConnectionBad.new(@client.error_message)
              error.instance_variable_set(:@connection, @client)
              raise error
            end
            @client.set_default_encoding unless reconnecting?
            @client
          end
        end

        alias_method :notify_writable, :poll_connection_and_check
        alias_method :notify_readable, :poll_connection_and_check

      end

    end
  end
end
