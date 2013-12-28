module PG
  module EM
    class Client < PG::Connection

      # This module is used as a handler to ::EM.watch connection socket and
      # extract query results in a non-blocking manner.
      #
      # Author:: Rafal Michalski (mailto:royaltm75@gmail.com)
      # Licence:: MIT License
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

        # Carefully extract the last result without
        # blocking the EventMachine reactor.
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

    end
  end
end
