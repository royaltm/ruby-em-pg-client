module PG
  module EM
    class Client < PG::Connection

      # This module is used as a handler to ::EM.watch connection socket and
      # it extracts query results in a non-blocking manner.
      #
      # Author:: Rafal Michalski
      module Watcher

        def initialize(client)
          @client = client
          @is_connected = true
        end

        def watching?
          @is_connected
        end

        def one_result_mode?
          @one_result_mode
        end

        def set_single_result_mode
          @one_result_mode = true
          @send_proc = nil
        end

        def watch_results(deferrable, send_proc=nil)
          @one_result_mode = false
          @last_result = nil
          @deferrable = deferrable
          @send_proc = send_proc
          cancel_timer
          if @client.is_busy
            if @client.status == PG::CONNECTION_OK
              self.notify_readable = true
              if (timeout = @client.query_timeout) > 0
                @notify_timestamp = Time.now
                setup_timer timeout
              end
            else
              @deferrable.protect do
                @client.raise_error ConnectionBad
              end
            end
          else
            ::EM.next_tick { fetch_results }
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
                @client.raise_error ConnectionBad, "query timeout expired (async)"
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

        # Carefully extract results without
        # blocking the EventMachine reactor.
        def fetch_results
          result = false
          @client.consume_input
          until @client.is_busy
            single_result = @client.blocking_get_result
            if one_result_mode?
              result = single_result
              break
            elsif single_result.nil?
              if result = @last_result
                result.check
              end
              break
            end
            @last_result.clear if @last_result
            @last_result = single_result
          end
        rescue Exception => e
          self.notify_readable = false
          cancel_timer
          send_proc = @send_proc
          @send_proc = nil
          df = @deferrable
          # prevent unbind error on auto re-connect
          @deferrable = false
          if e.is_a?(PG::Error)
            @client.async_autoreconnect!(df, e, &send_proc)
          else
            df.fail(e)
          end
        else
          if result == false
            @notify_timestamp = Time.now if @timer
          else
            self.notify_readable = false
            cancel_timer
            @send_proc = nil
            @deferrable.succeed(result)
          end
        end

        alias_method :notify_readable, :fetch_results

        def unbind
          @is_connected = false
          @deferrable.protect do
            @client.raise_error ConnectionBad, "connection reset"
          end if @deferrable
        end
      end

    end
  end
end
