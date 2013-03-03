require 'em-synchrony/pg'

module PG
  module EM
    module Errors
      class TransactionError < QueryError; end
    end
    class ConnectionPool
      include Errors

      SIZE_KEYS = [:size, :pool_size, 'pool', 'pool_size']
      def initialize(opts, &blk)
        @available = []
        @pending = []
        @reserved = {}
        @acquire_cb = blk
        @disconnect_error_class = QueryBadStateError

        size = 1
        opts = opts.reject do |key, value|
          case key.to_sym
          when :size, :pool_size
            size = value.to_i
            true
          when :disconnect_class
            @disconnect_error_class = value
            true
          end
        end

        raise ArgumentError, "#{self.class}.new: pool size must be > 1" if size < 1
        
        @acquire_cb = proc { PG::EM::Client.new(opts.merge(async_autoreconnect: true)) } unless block_given?

        size.times do
          @available << @acquire_cb.call
        end
      end

      def finish
        (@available + @reserved.values).each {|conn| conn.finish}
      end

      alias_method :close, :finish

      %w[connect_timeout query_timeout async_autoreconnect on_autoreconnect].each do |name|
        class_eval <<-EOD, __FILE__, __LINE__
          def #{name}=(value)
            (@available + @reserved.values).each {|pg| pg.#{name} = value}
          end
        EOD
      end

      %w(
        exec              send_query
        exec_prepared     send_query_prepared
        prepare           send_prepare
        describe_prepared send_describe_prepared
        describe_portal   send_describe_portal
          ).each_slice(2) do |name, send_name|
        async_name = "async_#{name}"

        class_eval <<-EOD, __FILE__, __LINE__
          def #{name}(*args, &blk)
            execute { |pg| pg.#{name}(*args, &blk) }
          end
        EOD
        class_eval <<-EOD, __FILE__, __LINE__
          def #{async_name}(*args, &blk)
            execute { |pg| pg.#{async_name}(*args, &blk) }
          end
        EOD
        class_eval <<-EOD, __FILE__, __LINE__
          def #{send_name}(*args, &blk)
            execute(true) do |pg|
              f = Fiber.current
              pg.#{send_name}(*args) do |res|
                release(f)
                blk.call if blk
              end
            end
          end
        EOD
      end

      alias_method :async_query, :async_exec
      alias_method :aquery, :send_query
      alias_method :query, :exec

      # Executes a BEGIN at the start of the block
      # and a COMMIT at the end of the block
      # or ROLLBACK if any exception occurs.
      # Calls to transaction may be nested,
      # however without sub-transactions (save points).
      def transaction(&blk)
        execute do |pg|
          local = Thread.current
          tcount = (local[:pg_em_client_tran_count] ||= 0)
          case pg.transaction_status
          when PG::PQTRANS_IDLE
            if tcount.zero?
              pg.exec('BEGIN')
            else
              raise TransactionError.new(pg)
            end
          when PG::PQTRANS_INTRANS
          else
            raise TransactionError.new(pg)
          end
          local[:pg_em_client_tran_count] = tcount + 1
          begin
            blk.call self
          rescue
            case pg.transaction_status
            when PG::PQTRANS_INTRANS, PG::PQTRANS_INERROR
              pg.exec('ROLLBACK')
            end
            raise
          else
            case pg.transaction_status
            when PG::PQTRANS_INTRANS
              pg.exec('COMMIT') if tcount.zero?
            when PG::PQTRANS_INERROR
              pg.exec('ROLLBACK')
            when PG::PQTRANS_IDLE # rolled back before
            else
              raise TransactionError.new(pg)
            end
          ensure
            local[:pg_em_client_tran_count] = tcount
          end
        end
      end

      def execute(async = false)
        fiber = Fiber.current
        if conn = @reserved[fiber.object_id]
          async = true
        else
          conn = acquire(fiber)
        end
        begin
          yield conn
        rescue @disconnect_error_class => e
          @reserved[fiber.object_id] = @acquire_cb.call conn
          raise
        end
      ensure
        release(fiber) unless async
      end

      private

      def acquire(fiber)
        if conn = @available.pop
          @reserved[fiber.object_id] = conn
          conn
        else
          Fiber.yield @pending.push fiber
          acquire(fiber)
        end
      end

      def release(fiber)
        @available.push(@reserved.delete(fiber.object_id))

        if pending = @pending.shift
          EM.next_tick { pending.resume }
        end
      end
    end
  end
end
