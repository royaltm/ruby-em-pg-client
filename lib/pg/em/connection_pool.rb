require 'em-synchrony/pg'

module PG
  module EM
    # This class is to be used only with synchrony version of {PG::EM::Client}
    # required by
    #   require 'em-synchrony/pg'
    #
    # To create a connection pool you need to pass +db_options+ hash
    # to the ConnectionPool.new:
    #
    # @example Basic usage
    #   pg = PG::EM::ConnectionPool.new size: 10, dbname: 'foo'
    #   res = pg.query 'select * from bar'
    #
    # All of the below {Client} instance methods are available on {ConnectionPool} instance:
    #
    # * {SynchronyClient#exec}
    # * {SynchronyClient#exec_params}
    # * {SynchronyClient#exec_prepared}
    # * {SynchronyClient#prepare}
    # * {SynchronyClient#describe_prepared}
    # * {SynchronyClient#describe_portal}
    # * {SynchronyClient#async_exec}
    # * {SynchronyClient#async_exec_params}
    # * {SynchronyClient#async_exec_prepared}
    # * {SynchronyClient#async_prepare}
    # * {SynchronyClient#async_describe_prepared}
    # * {SynchronyClient#async_describe_portal}
    # * {Client#send_query}
    # * {Client#send_query_prepared}
    # * {Client#send_prepare}
    # * {Client#send_describe_prepared}
    # * {Client#send_describe_portal}
    #
    class ConnectionPool
      include Errors
      # Creates and initializes new connection pool
      #
      # Pass PG::EM::Client +options+ and optional Client factory code as +blk+
      #
      # There are two custom ConnectionPool +options+:
      #
      # - +:size+ or +:pool_size+ = +1+ - the number of Client connections
      # - +:disconnect_class+ = +QueryBadStateError+ - error class
      #   when raised during query execution will trigger
      #   re-creation of the Client connection;
      #   set to nil to disable this feature
      #
      # When +:async_autoreconnect+ option is true the +QueryBadStateError+ is never raised,
      # so the +:disconnect_class+ option only affects cases when you turn off +async_autoreconnect+
      # and want to have more control over server disconnects.
      #
      # If you don't specify Client factory block the default Client factory
      # will add +:async_autoreconnect+ = +true+ to the +options+.
      #
      # @raise [Error]
      # @raise [ArgumentError]
      def initialize(opts = {}, &blk)
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
        
        @acquire_cb = proc { Client.new(opts.merge(async_autoreconnect: true)) } unless block_given?

        size.times do
          @available << @acquire_cb.call
        end
      end

      # Finishes all Client connections
      def finish
        (@available + @reserved.values).each {|conn| conn.finish}
      end

      alias_method :close, :finish

      # @!attribute [w] connect_timeout
      #   Sets {Client#connect_timeout} on all connections in this pool
      # @!attribute [w] query_timeout
      #   Sets {Client#query_timeout} on all connections in this pool
      # @!attribute [w] async_autoreconnect
      #   Sets {Client#async_autoreconnect} on all connections in this pool
      # @!attribute [w] on_autoreconnect
      #   Sets {Client#on_autoreconnect} on all connections in this pool
      %w[connect_timeout query_timeout async_autoreconnect on_autoreconnect].each do |name|
        class_eval <<-EOD, __FILE__, __LINE__
          def #{name}=(value)
            (@available + @reserved.values).each {|pg| pg.#{name} = value}
          end
        EOD
      end

      %w(
        exec              async_exec
        exec_params       async_exec_params
        exec_prepared     async_exec_prepared
        prepare           async_prepare
        describe_prepared async_describe_prepared
        describe_portal   async_describe_portal
          ).each do |name|

        class_eval <<-EOD, __FILE__, __LINE__
          def #{name}(*args, &blk)
            execute { |pg| pg.#{name}(*args, &blk) }
          end
        EOD
      end

      %w(
        send_query
        send_query_prepared
        send_prepare
        send_describe_prepared
        send_describe_portal
          ).each do |send_name|

        class_eval <<-EOD, __FILE__, __LINE__
          def #{send_name}(*args, &blk)
            execute(true) do |pg|
              f = Fiber.current
              pg.#{send_name}(*args) do |res|
                release(f)
                blk.call res if blk
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
      #
      # @see Client#transaction
      # @see #execute
      def transaction(&blk)
        execute do |pg|
          pg.transaction(&blk)
        end
      end

      # Acquires connection and passes it to the given block.
      #
      # If +async+ is true the connection is not released back to the
      # available pool upon block termination.
      # The async feature is used only internally - +#release()+ is a private method.
      #
      # It is possible to nest execute calls from the same fiber,
      # so each time the block will be given the same {Client} instance.
      # This feature is needed e.g. for nesting transaction calls.
      # @yieldparam [Client] pg
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
