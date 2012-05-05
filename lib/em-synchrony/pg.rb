require 'pg/em'
module PG
  module EM
    class Client
      # Author:: Rafal Michalski (mailto:royaltm75@gmail.com)
      # Licence:: MIT License
      #
      # =PostgreSQL Client for EM-Synchrony/Fibered EventMachine
      #

      # conform to *standard*
      alias_method :aquery, :async_query

      # fiber aware methods:
      # - exec (aliased as query)
      # - exec_prepared
      # - prepare
      # - describe_prepared
      # - describe_portal
      # - reset
      # - Client.connect
      %w(exec
         exec_prepared
         prepare
         describe_prepared
         describe_portal
         reset
         self.connect).each do |name|
        async_name = "async_#{name.split('.').last}"
        blocking_call = case name
        when 'reset'
          '@async_command_aborted = false
            super(*args, &blk)'
        else
          'super(*args, &blk)'
        end
        clear_method = case name
        when 'reset', 'self.connect'
          'finish'
        else
          'clear'
        end
        class_eval <<-EOD
          def #{name}(*args, &blk)
            if ::EM.reactor_running?
              f = Fiber.current
              #{async_name}(*args) do |res|
                f.resume(res)
              end

              result = Fiber.yield
              raise result if result.is_a?(::Exception)
              if block_given?
                begin
                  yield result
                ensure
                  result.#{clear_method}
                end
              else
                result
              end
            else
              #{blocking_call}
            end
          end
        EOD
      end

      class << self
        alias_method :new, :connect
        alias_method :open, :connect
        alias_method :setdb, :connect
        alias_method :setdblogin, :connect
      end

      alias_method :query, :exec

      def async_autoreconnect!(deferrable, error, &send_proc)
         if async_autoreconnect && self.status != PG::CONNECTION_OK
          reset_df = async_reset
          reset_df.errback { |ex| deferrable.fail(ex) }
          reset_df.callback do
            Fiber.new do
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
            end.resume
          end
        else
          ::EM.next_tick { deferrable.fail(error) }
        end
      end

    end
  end
end
