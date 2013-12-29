require 'pg/em'

module PG
  module EM

    # Connection pool for PG::EM::Client
    #
    # Author:: Rafal Michalski
    #
    # The ConnectionPool allocates new connections asynchronously when
    # there are no available connections left up to the {#max_size} number.
    #
    # @example Basic usage
    #   pg = PG::EM::ConnectionPool.new size: 10, dbname: 'foo'
    #   res = pg.query 'select * from bar'
    #
    # The list of {Client} command methods that are available in {ConnectionPool}:
    #
    # Blocking command methods:
    #
    # * {Client#exec}
    # * {Client#query}
    # * {Client#async_exec}
    # * {Client#async_query}
    # * {Client#exec_params}
    # * {Client#exec_prepared}
    # * {Client#prepare}
    # * {Client#describe_prepared}
    # * {Client#describe_portal}
    #
    # Deferrable command methods:
    #
    # * {Client#exec_defer}
    # * {Client#query_defer}
    # * {Client#async_exec_defer}
    # * {Client#async_query_defer}
    # * {Client#exec_params_defer}
    # * {Client#exec_prepared_defer}
    # * {Client#prepare_defer}
    # * {Client#describe_prepared_defer}
    # * {Client#describe_portal_defer}
    #
    # If {Client#async_autoreconnect} option is not set or the re-connect fails
    # the failed connection is dropped from the pool.
    class ConnectionPool

      DEFAULT_SIZE = 4

      # Maximum number of connections in the connection pool
      attr_reader :max_size

      attr_reader :available, :allocated

      # Creates and initializes new connection pool.
      #
      # The connection pool allocates its first connection upon initialization
      # unless +lazy: true+ option is given.
      #
      # Pass PG::EM::Client +options+ together with ConnectionPool +options+:
      #
      # - +:size+ = +4+ - the maximum number of Client connections
      # - +:lazy+ = false - should lazy allocate first connection
      # - +:connection_class+ = {PG::EM::Client}
      #
      # @raise [PG::Error]
      # @raise [ArgumentError]
      def initialize(options = {})
        @available = []
        @pending = []
        @allocated = {}
        @connection_class = Client

        lazy = false
        @options = options.reject do |key, value|
          case key.to_sym
          when :size, :max_size
            @max_size = value.to_i
            true
          when :connection_class
            @connection_class = value
            true
          when :lazy
            lazy = value
            true
          end
        end

        @max_size ||= DEFAULT_SIZE

        raise ArgumentError, "#{self.class}.new: pool size must be > 1" if @max_size < 1

        # allocate first connection, unless we are lazy
        execute unless lazy
      end

      # Creates and initializes new connection pool.
      #
      # Attempts to establish the first connection asynchronously.
      #
      # @return [FeaturedDeferrable]
      # @yieldparam pg [Client|PG::Error] new and connected client instance
      #             on success or a PG::Error
      #
      # Use the returned deferrable's hooks +callback+ to obtain newly created
      # {ConnectionPool}.
      # In case of a connection error +errback+ hook is called instead with
      # a raised error object as its argument.
      #
      # If the block is provided it's bound to both +callback+ and +errback+
      # hooks of the returned deferrable.
      #
      # Pass PG::EM::Client +options+ together with ConnectionPool +options+:
      #
      # - +:size+ = +4+ - the maximum number of Client connections
      # - +:connection_class+ = {PG::EM::Client}
      #
      # @raise [ArgumentError]
      def self.connect_defer(options = {}, &blk)
        pool = new options.merge(lazy: true)
        pool.__send__(:execute_deferred, blk) do
          ::EM::DefaultDeferrable.new.tap { |df| df.succeed pool }
        end
      end

      class << self
        alias_method :connect, :new
        alias_method :async_connect, :connect_defer
      end

      # Current number of connections in the connection pool
      #
      # @return [Integer]
      def size
        @available.length + @allocated.length
      end

      # Finishes all available connections and clears the available pool.
      #
      # After call to this method the pool is still usable and will allocate
      # new client connections when needed.
      def finish
        @available.each { |c| c.finish }
        @available.clear
        self
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
            @available.each { |c| c.#{name} = value }
            @allocated.each_value { |c| c.#{name} = value if c.is_a?(@connection_class) }
          end
        EOD
      end

      %w(
        exec
        exec_params
        exec_prepared
        prepare
        describe_prepared
        describe_portal
          ).each do |name|

        class_eval <<-EOD, __FILE__, __LINE__
          def #{name}(*args, &blk)
            execute { |c| c.#{name}(*args, &blk) }
          end
        EOD
      end

      alias_method :query,       :exec
      alias_method :async_query, :exec
      alias_method :async_exec,  :exec

      %w(
        exec_defer
        exec_prepared_defer
        prepare_defer
        describe_prepared_defer
        describe_portal_defer
          ).each do |name|

        class_eval <<-EOD, __FILE__, __LINE__
          def #{name}(*args, &blk)
            execute_deferred(blk) { |c| c.#{name}(*args) }
          end
        EOD
      end

      alias_method :query_defer,       :exec_defer
      alias_method :async_query_defer, :exec_defer
      alias_method :async_exec_defer,  :exec_defer
      alias_method :exec_params_defer, :exec_defer

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
      # It is possible to nest execute calls from the same fiber,
      # so each time the block will be given the same {Client} instance.
      # This feature is needed e.g. for nesting transaction calls.
      # @yieldparam [Client] pg
      def execute
        fiber = Fiber.current
        id = fiber.object_id

        if conn = @allocated[id]
          skip_release = true
        else
          conn = acquire(fiber)
        end

        begin
          yield conn if block_given?

        rescue PG::Error
          if conn.status != PG::CONNECTION_OK
            conn.finish unless conn.finished?
            @allocated.delete(id)
            skip_release = true
          end
          raise
        ensure
          release(id) unless skip_release
        end
      end

      private

      def acquire(fiber)
        if conn = @available.pop
          @allocated[fiber.object_id] = conn
        else
          if size < max_size
            begin
              id = fiber.object_id
              @allocated[id] = fiber
              conn = @connection_class.new(@options)
            ensure
              if conn
                @allocated[id] = conn
              else
                @allocated.delete(id)
              end
            end
          else
            @pending << fiber
            Fiber.yield
          end
        end
      end

      def execute_deferred(blk = nil)
        if conn = @available.pop
          id = conn.object_id
          @allocated[id] = conn
          df = yield conn
        else
          df = FeaturedDeferrable.new
          id = df.object_id
          if size < max_size
            @allocated[id] = df
            conn_df = @connection_class.connect_defer(@options)
            conn_df.errback do |err|
              @allocated.delete(id)
              df.fail(err)
            end
          else
            @pending << (conn_df = ::EM::DefaultDeferrable.new)
          end
          conn_df.callback do |nc|
            @allocated[id] = conn = nc
            df.bind_status yield conn
          end
        end
        df.callback { release(id) }
        df.errback do |err|
          if conn
            if err.is_a?(PG::Error) &&
                conn.status != PG::CONNECTION_OK
              conn.finish unless conn.finished?
              @allocated.delete(id)
            else
              release(id)
            end
          end
        end
        df.completion(&blk) if blk
        df
      end

      def release(id)
        conn = @allocated.delete(id)
        if pending = @pending.shift
          if pending.is_a?(Fiber)
            @allocated[pending.object_id] = conn
            ::EM.next_tick { pending.resume conn }
          else
            pending.succeed conn
          end
        else
          @available << conn
        end
      end

    end
  end
end
