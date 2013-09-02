require 'fiber'
require 'pg/em'
module PG
  module EM
    module Errors
      class TransactionError < QueryError; end
    end
    # @!macro auto_synchrony_api
    #   Performs command asynchronously using {#async_$0}
    #   if EM reactor is running, otherwise acts exactly like PG::Connection#$0.
    #
    #   @return [PG::Result] if block wasn't given
    #   @return [Object] if block given returns result of the block
    #   @raise [Error]
    #   @see #async_$0
    #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-$0 PG::Connection#$0

    # @!macro pure_synchrony_api
    #   Performs command asynchronously yielding current fiber and ensuring
    #   that other fibers can process while waiting for the server
    #   to complete the request.
    #
    #   @yieldparam result [PG::Result] command result on success
    #   @return [PG::Result] if block wasn't given
    #   @return [Object] if block given returns result of the block
    #   @raise [Error]

    # @!parse
    #   # @note This class is non-existent. Don't try to use it. Instead use {Client} class.
    #   # Its purpose here is solely for documenting methods of {Client} which are redefined
    #   # when this file is being loaded:
    #   #   require 'em-synchrony/pg'
    #   # The methods described here are designed to be used in Fiber-entangled environment for EM.
    #   class SynchronyClient < Client
    #     include Errors
    #
    #     # @!group Auto-sensing sync/async command methods
    #
    #     # Sends SQL query request specified by +sql+ to PostgreSQL.
    #     #
    #     # @macro auto_synchrony_api
    #     def exec(sql, &blk); end
    #     alias_method :query, :exec
    #
    #     # Sends SQL query request specified by +sql+ with optional +params+ and +result_format+ to PostgreSQL.
    #     #
    #     # @macro auto_synchrony_api
    #     def exec_params(sql, params=nil, result_format=nil, &blk); end
    #
    #     # Prepares statement +sql+ with name +stmt_name+ to be executed later.
    #     #
    #     # @macro auto_synchrony_api
    #     def prepare(stmt_name, sql, param_types=nil, &blk); end
    #
    #     # Execute prepared named statement specified by +statement_name+.
    #     #
    #     # @macro auto_synchrony_api
    #     def exec_prepared(statement_name, params=nil, result_format=nil, &blk); end
    #
    #     # Retrieve information about the prepared statement +statement_name+,
    #     #
    #     # @macro auto_synchrony_api
    #     def describe_prepared(statement_name, &blk); end
    #
    #     # Retrieve information about the portal +portal_name+.
    #     #
    #     # @macro auto_synchrony_api
    #     def describe_portal(portal_name, &blk); end
    #
    #     # @!endgroup
    #
    #
    #     # @!group Asynchronous command methods
    #
    #     # @!method async_exec(sql, &blk)
    #     #   Sends SQL query request specified by +sql+ to PostgreSQL.
    #     #
    #     #   @macro pure_synchrony_api
    #     #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-async_exec PG::Connection#async_exec
    #     alias_method :async_query, :async_exec
    #
    #     # @!method async_exec_params(sql, params=nil, result_format=nil, &blk)
    #     #   Sends SQL query request specified by +sql+ with optional +params+ and +result_format+ to PostgreSQL.
    #     #
    #     #   @macro pure_synchrony_api
    #     #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-async_exec PG::Connection#async_exec
    #
    #     # @!method async_prepare(stmt_name, sql, param_types=nil, &blk)
    #     #   Prepares statement +sql+ with name +stmt_name+ to be executed later.
    #     #
    #     #   @macro pure_synchrony_api
    #     #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-prepare PG::Connection#prepare
    #
    #     # @!method async_exec_prepared(statement_name, params=nil, result_format=nil, &blk)
    #     #   Execute prepared named statement specified by +statement_name+.
    #     #
    #     #   @macro pure_synchrony_api
    #     #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-exec_prepared PG::Connection#exec_prepared
    #
    #     # @!method async_describe_prepared(statement_name, &blk)
    #     #   Retrieve information about the prepared statement +statement_name+,
    #     #
    #     #   @macro pure_synchrony_api
    #     #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-describe_prepared PG::Connection#describe_prepared
    #
    #     # @!method async_describe_portal(portal_name, &blk)
    #     #   Retrieve information about the portal +portal_name+.
    #     #
    #     #   @macro pure_synchrony_api
    #     #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-describe_portal PG::Connection#describe_portal
    #
    #     # @!endgroup
    #
    #     # @!group Auto-sensing sync/async connection methods
    #
    #     # @!method reset
    #     #   Attempts to reset the connection asynchronously (yielding current fiber)
    #     #   if EM reactor is running, otherwise acts exactly like its synchronous version.
    #     #
    #     #   @return [Client] reconnected client instance
    #     #   @raise [ConnectionRefusedError] if there was a connection error
    #     #   @raise [ConnectionTimeoutError] on timeout
    #     #   @see http://deveiate.org/code/pg/PG/Connection.html#method-i-reset PG::Connection#reset
    #
    #     # @!method self.connect(*args, &blk)
    #     #   Attempts to establish the connection asynchronously (yielding current fiber)
    #     #   if EM reactor is running, otherwise acts exactly like its synchronous version.
    #     #
    #     #   @return [Client] new and connected client instance
    #     #   @raise [ConnectionRefusedError] if there was a connection error
    #     #   @raise [ConnectionTimeoutError] on timeout
    #     #   @see http://deveiate.org/code/pg/PG/Connection.html#method-c-new PG::Connection.new
    #
    #     # @!scope class
    #     alias_method :new, :connect
    #     # @!scope class
    #     alias_method :open, :connect
    #     # @!scope class
    #     alias_method :setdb, :connect
    #     # @!scope class
    #     alias_method :setdblogin, :connect
    #
    #     # @!endgroup
    #
    #   end
    class Client < PG::Connection
      # Author:: Rafal Michalski (mailto:royaltm75@gmail.com)
      # Licence:: MIT License
      #
      # =PostgreSQL Client for EM-Synchrony/Fibered EventMachine

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
      # - exec_params
      # - exec_prepared
      # - prepare
      # - describe_prepared
      # - describe_portal
      # - reset
      # - Client.connect
      %w(
        exec              send_query
        exec_params       send_query
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

      TRAN_BEGIN_QUERY = 'BEGIN'
      TRAN_ROLLBACK_QUERY = 'ROLLBACK'
      TRAN_COMMIT_QUERY = 'COMMIT'
      # Executes a BEGIN at the start of the block and a COMMIT at the end
      # of the block or ROLLBACK if any exception occurs.
      #
      # @note This method may *only* be used with +em-synchrony/pg+ version
      # @return [Object] result of the block
      # @yieldparam client [self]
      # @version em-synchrony/pg only
      # @see http://deveiate.org/code/pg/PG/Connection.html#method-i-transaction PG::Connection#transaction
      #
      # Calls to {#transaction} may be nested, however without sub-transactions
      # (save points). If the innermost transaction block raises an error
      # the transaction is rolled back to the state before the outermost
      # transaction began.
      # This is an extension to the +PG::Connection#transaction+ method
      # as it does not support nesting.
      #
      # The method is sensitive to the transaction status and will safely
      # rollback on any sql error even when it was catched by some rescue block.
      # But consider that rescuing any sql error within an utility method
      # is a bad idea.
      #
      # This method works in both sync/async modes (regardles of the reactor state)
      # and is considered as a generic extension to the +PG::Connection#transaction+
      # method.
      #
      # @example Nested transaction example
      #  def add_comment(user_id, text)
      #    db.transaction do
      #      cmt_id = db.query(
      #        'insert into comments (text) where user_id=$1 values ($2) returning id',
      #        [user_id, text]).getvalue(0,0)
      #      db.query(
      #        'update users set last_comment_id=$2 where id=$1', [user_id, cmt_id])
      #      cmt_id
      #    end
      #  end
      #  
      #  def update_comment_count(page_id)
      #    db.transaction do
      #      count = db.query('select count(*) from comments where page_id=$1', [page_id]).getvalue(0,0)
      #      db.query('update pages set comment_count=$2 where id=$1', [page_id, count])
      #    end
      #  end
      #
      #  # to run add_comment and update_comment_count within the same transaction
      #  db.transaction do
      #    add_comment(user_id, some_text)
      #    update_comment_count(page_id)
      #  end
      #
      def transaction
        raise ArgumentError, 'Must supply block for PG::EM::Client#transaction' unless block_given?
        tcount = @client_tran_count.to_i
        case transaction_status
        when PG::PQTRANS_IDLE
          if tcount.zero?
            exec(TRAN_BEGIN_QUERY)
          else
            raise TransactionError.new('unable to begin nested transaction, current transaction was finished prematurely', self)
          end
        when PG::PQTRANS_INTRANS
        else
          raise TransactionError.new('error in transaction, need ROLLBACK', self)
        end
        @client_tran_count = tcount + 1
        begin
          result = yield self
        rescue
          case transaction_status
          when PG::PQTRANS_INTRANS, PG::PQTRANS_INERROR
            exec(TRAN_ROLLBACK_QUERY) if tcount.zero?
          end
          raise
        else
          case transaction_status
          when PG::PQTRANS_INTRANS
            exec(TRAN_COMMIT_QUERY) if tcount.zero?
          when PG::PQTRANS_INERROR
            exec(TRAN_ROLLBACK_QUERY) if tcount.zero?
          when PG::PQTRANS_IDLE
            raise TransactionError.new('transaction was finished prematurely', self)
          else
            raise TransactionError.new('unkown transaction status', self)
          end
          result
        ensure
          @client_tran_count = tcount
        end
      end

      # @!visibility private
      # Perform auto re-connect. Used internally. Synchrony version.
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
