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
          @one_result_mode = false
          @deferrable = nil
          @notify_deferrable = nil
          @timer = nil
          @notify_timer = nil
        end

        def watching?
          @is_connected
        end

        def one_result_mode?
          @one_result_mode
        end

        def watch_results(deferrable, send_proc = nil, one_result_mode = false)
          @one_result_mode = one_result_mode
          @last_result = nil
          @deferrable = deferrable
          @send_proc = send_proc
          cancel_timer
          self.notify_readable = true unless notify_readable?
          if (timeout = @client.query_timeout) > 0
            @readable_timestamp = Time.now
            setup_timer timeout
          end
          fetch_results
        end

        def watch_notify(deferrable, timeout = nil)
          notify_df = @notify_deferrable
          @notify_deferrable = deferrable
          cancel_notify_timer
          self.notify_readable = true unless notify_readable?
          if timeout
            @notify_timer = ::EM::Timer.new(timeout) do
              @notify_timer = nil
              succeed_notify
            end
          end
          notify_df.fail nil if notify_df
          check_notify
        end

        def setup_timer(timeout, adjustment = 0)
          @timer = ::EM::Timer.new(timeout - adjustment) do
            if (last_interval = Time.now - @readable_timestamp) >= timeout
              @timer = nil
              cancel_notify_timer
              self.notify_readable = false
              @client.async_command_aborted = true
              @send_proc = nil
              begin
                @client.raise_error ConnectionBad, "query timeout expired (async)"
              rescue Exception => e
                fail_result e
                # notify should also fail: query timeout is like connection error
                fail_notify e
              end
            else
              setup_timer timeout, last_interval
            end
          end
        end

        def cancel_notify_timer
          if @notify_timer
            @notify_timer.cancel
            @notify_timer = nil
          end
        end

        def cancel_timer
          if @timer
            @timer.cancel
            @timer = nil
          end
        end

        def notify_readable
          @client.consume_input
        rescue Exception => e
          handle_error e
        else
          fetch_results if @deferrable
          check_notify if @notify_deferrable
        end

        def check_notify
          if notify_hash = @client.notifies
            cancel_notify_timer
            succeed_notify notify_hash
          end
        end

        # Carefully extract results without
        # blocking the EventMachine reactor.
        def fetch_results
          result = false
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
          handle_error e
        else
          if result == false
            @readable_timestamp = Time.now if @timer
          else
            cancel_timer
            self.notify_readable = false unless @notify_deferrable
            df = @deferrable
            @deferrable = @send_proc = nil
            df.succeed result
          end
        end

        def unbind
          @is_connected = false
          cancel_timer
          cancel_notify_timer
          if @deferrable || @notify_deferrable
            @client.raise_error ConnectionBad, "connection reset"
          end
        rescue Exception => e
          fail_result e
          fail_notify e
        end

        private

        def fail_result(e)
          df = @deferrable
          @deferrable = nil
          df.fail e if df
        end

        def succeed_notify(notify_hash = nil)
          self.notify_readable = false unless @deferrable
          df = @notify_deferrable
          @notify_deferrable = nil
          df.succeed notify_hash
        end

        def fail_notify(e)
          df = @notify_deferrable
          @notify_deferrable = nil
          df.fail e if df
        end

        def handle_error(e)
          cancel_timer
          send_proc = @send_proc
          @send_proc = nil
          df = @deferrable || FeaturedDeferrable.new
          # prevent unbind error on auto re-connect
          @deferrable = nil
          notify_df = @notify_deferrable
          self.notify_readable = false unless notify_df
          if e.is_a?(PG::Error)
            @client.async_autoreconnect!(df, e, send_proc) do
              # there was a connection error so stop any remaining activity
              if notify_df
                @notify_deferrable = nil
                cancel_notify_timer
                self.notify_readable = false
                # fail notify_df after deferrable completes
                # handler might setup listen again then immediately
                df.completion { notify_df.fail e }
              end
            end
          else
            df.fail e
          end
        end

      end

    end
  end
end
