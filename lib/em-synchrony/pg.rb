require 'fiber'
require 'pg/em'
module PG
  module EM
    module Errors
      class TransactionError < QueryError; end
    end
    class Client
      # Author:: Rafal Michalski (mailto:royaltm75@gmail.com)
      # Licence:: MIT License
      #
      # =PostgreSQL Client for EM-Synchrony/Fibered EventMachine
      #

      alias_method :aquery, :send_query

      # asynchronous methods that return deferrable:
      # - send_query (aliased as aquery)
      # - send_query_prepared
      # - send_prepare
      # - send_describe_prepared
      # - send_describe_portal
      #
      # fiber entangled asynchronous methods:
      # - async_exec (aliased as async_query)
      # - async_exec_prepared
      # - async_prepare
      # - async_describe_prepared
      # - async_describe_portal
      #
      # fiber aware and reactor-sensitive methods:
      # (synchronous while reactor is not running)
      # - exec (aliased as query)
      # - exec_prepared
      # - prepare
      # - describe_prepared
      # - describe_portal
      # - reset
      # - Client.connect
      %w(
        exec              send_query
        exec_prepared     send_query_prepared
        prepare           send_prepare
        describe_prepared send_describe_prepared
        describe_portal   send_describe_portal
        reset             *
        self.connect      *
          ).each_slice(2) do |name, send_name|
        async_name = "async_#{name.split('.').last}"
        if send_name == '*'
          blocking_call = case name
          when 'reset'
            '@async_command_aborted = false
                super(*args, &blk)'
          else
            'super(*args, &blk)'
          end

          class_eval <<-EOD, __FILE__, __LINE__
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
                    result.finish
                  end
                else
                  result
                end
              else
                #{blocking_call}
              end
            end
          EOD

        else

          class_eval <<-EOD, __FILE__, __LINE__
            def #{async_name}(*args, &blk)
              f = Fiber.current
              #{send_name}(*args) do |res|
                f.resume(res)
              end

              result = Fiber.yield
              raise result if result.is_a?(::Exception)
              if block_given?
                begin
                  yield result
                ensure
                  result.clear
                end
              else
                result
              end
            end
          EOD
        end
      end

      class << self
        alias_method :new, :connect
        alias_method :open, :connect
        alias_method :setdb, :connect
        alias_method :setdblogin, :connect
      end

      alias_method :async_query, :async_exec

      # Executes a BEGIN at the start of the block
      # and a COMMIT at the end of the block
      # or ROLLBACK if any exception occurs.
      # Calls to transaction may be nested,
      # however without sub-transactions (save points).
      TRAN_BEGIN_QUERY = 'BEGIN'
      TRAN_ROLLBACK_QUERY = 'ROLLBACK'
      TRAN_COMMIT_QUERY = 'COMMIT'
      def transaction(&blk)
        tcount = @client_tran_count.to_i
        case transaction_status
        when PG::PQTRANS_IDLE
          if tcount.zero?
            exec(TRAN_BEGIN_QUERY)
          else
            raise TransactionError.new('transaction status was idle, but transaction count != 0', self)
          end
        when PG::PQTRANS_INTRANS
        else
          raise TransactionError.new('error in transaction, need ROLLBACK', self)
        end
        @client_tran_count = tcount + 1
        begin
          blk.call self
        rescue
          case transaction_status
          when PG::PQTRANS_INTRANS, PG::PQTRANS_INERROR
            exec(TRAN_ROLLBACK_QUERY)
          end
          raise
        else
          case transaction_status
          when PG::PQTRANS_INTRANS
            exec(TRAN_COMMIT_QUERY) if tcount.zero?
          when PG::PQTRANS_INERROR
            exec(TRAN_ROLLBACK_QUERY)
          when PG::PQTRANS_IDLE # rolled back before
          else
            raise TransactionError.new('unkown transaction status', self)
          end
        ensure
          @client_tran_count = tcount
        end
      end

      def async_autoreconnect!(deferrable, error, &send_proc)
        if self.status != PG::CONNECTION_OK
          if async_autoreconnect
            error = QueryError.wrap(error)
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
            ::EM.next_tick { deferrable.fail(QueryBadStateError.wrap(error)) }
          end
        else
          ::EM.next_tick { deferrable.fail(QueryError.wrap(error)) }
        end
      end

    end
  end
end
