require 'pg/em'
module PG
  module EM
    # Author:: Rafal Michalski
    #
    # Asynchrony aware iterator for retrieving results from previous command
    # query providing methods for both callback-style deferrables and
    # fiber-synchronized enumerators.
    #
    # The +iterator+ is a deferrable itself and will receive a +succeeded+ status
    # upon completion of the iteration or +failed+ status on an error.
    class Iterator
      include DeferrableFeatures
      include Enumerable

      attr_reader :client

      # @!attribute [rw] foreach
      #   @return [Proc] iterator handler
      #   Callback invoked with each result upon iteration.
      attr_accessor :foreach

      def initialize(client = nil)
        @client = client
        @foreach = nil
        @last_result = nil
        @deferred_status = :unknown
        @next = nil
        @stop = nil
      end

      # Return +true+ if all the results has been retrieved or the iteration
      # has been stopped or when there was an error.
      # Otherwise returns +false+.
      # @return [Boolean]
      def finished?
        @deferred_status != :unknown
      end

      # @!macro iterator_each_deferrable_api
      #   Each result will automatically be cleared on next iteration and before
      #   completion.
      #   
      #   Calling +iter.stop+ will interrupt current query, asynchronously
      #   reset the connection and terminate iteration. It is safe to call
      #   +iter.stop+ from inside of the iterator block. In this instance
      #   iterator succeeds passing :stop to callbacks.
      #   
      #   When there are no more results the iteration terminates invoking
      #   success callbacks on +iterator+.
      #   
      #   Whenever there is a result error the iteration terminates immediately
      #   invoking errback on +iterator+. Remaining results are fetched and
      #   cleared beforehand.

      # Iterates asynchronously using the specified block or proc over each
      # result from a call to +Client#send_query+ (or another asynchronous command)
      # and immediately returns with an iterator.
      #
      # @return [PG::EM::Iterator] - self
      #
      # @yieldparam result [PG::Result] - single result from a query
      # @yieldparam iter [PG::EM::Iterator] - iterator instance
      #
      # The block should invoke +iter.next+ when it's finished processing
      # the +result+ to indicate that it's ready to receive the next one.
      #
      # @macro iterator_each_deferrable_api
      #
      # @example
      #  pg.send_query('select * from foo; select * from bar')
      #  PG::EM::Iterator.new(pg).each_result_defer do |result, iter|
      #    # do smthng with result asynchronously
      #    EM.add_timer(0.25) do
      #      puts result.to_a
      #      iter.next
      #    end
      #  end.callback do
      #    puts 'all done'
      #  end.errback do |err|
      #    puts "error: #{err.inspect}"
      #  end
      #
      def each_result_defer(foreach = nil, after = nil, &block)
        foreach ||= block or raise ArgumentError, 'proc or block required for iteration' 
        @foreach = foreach

        completion(&after) if after

        ::EM.next_tick { self.next }

        self
      end

      # @!macro iterator_each_api
      #   Each result will automatically be cleared on next iteration and before
      #   completion.
      #   
      #   When there are no more results the iteration terminates returning
      #   +self+.
      #   
      #   Calling {#stop} will interrupt current query, reset the connection and
      #   terminate iteration. It is safe to call {#stop} from inside of the
      #   iterator block. In this instance +:stop+ is returned.
      #   
      #   Returns +nil+ if +break+ is invoked form inside of the iterator block.
      #   In this instance it's possible to call this method again to retrieve
      #   the remaining results.
      #   
      #   Returns an +Enumerator+ if block is not provided.
      #   
      #   Whenever there is a result error the iteration terminates immediately
      #   and the error is being raised.
      #   
      #   @note Since Ruby 2.0 the +lazy+ enumerator might come in handy while
      #         dealing with large datasets. Unfortunataley due to the bug:
      #         {https://bugs.ruby-lang.org/issues/9664} this method
      #         won't be usable in MRI 2 until it's fixed there. A workaround
      #         is to use MRI 1.9.x and backbort lazy enumerator using
      #         {https://github.com/marcandre/backports backports}.
      #   
      #   If EventMachine reactor is running and the current fiber isn't the
      #   root fiber this method performs command asynchronously transfering
      #   control from current fiber. Other fibers can process while waiting
      #   for the server to complete the request.
      #
      #   Otherwise performs a blocking call.
      #
      #   @return [self|Enumerator|:stop|nil]
      #   @raise [PG::Error]

      # Iterate, using the specified block, over each result from
      # a call to +Client#send_query+ (or another asynchronous command).
      #
      # @macro iterator_each_api
      #
      # @yieldparam result [PG::Result] - single result from a query
      #
      # @see #each_result_defer
      def each_result(&block)
        if block_given?
          if ::EM.reactor_running? && !(f = Fiber.current).equal?(ROOT_FIBER)
            ret = nil
            fiber = f
            result = nil
            each_result_defer do |res|
              if fiber
                fiber.transfer res
              else
                result = res
              end
            end.completion do |arg|
              ::EM.next_tick do
                ret = arg || self
                if fiber
                  fiber.transfer
                elsif result && ret.is_a?(Exception)
                  raise ret
                end
              end
            end
            while result ||= ROOT_FIBER.transfer
              fiber = nil
              call_block_safely(block, result)
              result = nil
              self.next
              break if ret
              fiber = f
            end
            fiber = nil
            raise ret if ret.is_a?(Exception)
            ret
          else
            # blocking implementation
            ret = nil
            callback do |arg|
              ret = arg || self
            end
            errback do |err|
              raise err
            end
            while result = client.blocking_get_result
              begin
                result.check
              rescue PG::Error => e
                client.get_last_result
                fail(e)
              end
              begin
                block.call result
              rescue => e
                client.reset
                fail(e)
              ensure
                result.clear
                break if ret
              end
            end
            succeed
            ret
          end
        else
          to_enum(:each_result)
        end
      end

      # @!attribute client
      #   @return [PG::EM::Client] postgres client instance
      #   A connection used to retrieve results.
      def client=(client)
        ::EM.next_tick do
          if @next == :start
            @next = nil
            self.next
          end
        end if @client.nil? && @next == :start
        @client = client
      end

      # Fetches next result asynchronously and invokes +foreach+ handler
      # with received result or finishes iteration setting status to either
      # +succeeded+ or +failed+.
      #
      # This method returns immediately unless called outside of the +forach+
      # handler and there are immediately available results.
      #
      # After fetching a new result, clears previous one before calling
      # +foreach+. Clears the last result before completing.
      #
      # This method is also used by {#each_result} and {#each_result_defer} to 
      # actually start the iteration. It is safe to call it more than once
      # before the result is actually received.
      #
      # If results are available immediately, performs a loop to avoid tail
      # call recursion. If called from inside of the +foreach+ handler,
      # ensures that the handler is never called recursively and
      # returns before it's called again with another result.
      # @return [nil]
      def next
        if @next || @stop || finished?
          clear_last_result
          return
        end
        if client
          if @next == false
            # mark only, next result is expected immediately
            @next = true
            return
          end
          (@next = client.get_result_defer).callback do |result|
            @next = false
            handle_result(result)
            # quick loop instead of tail call
            while @next
              if client.is_busy
                @next = nil
                self.next
                break
              end
              @next = false
              handle_result client.blocking_get_result
            end
            # clear @next == false
            @next ||= nil
          end.errback do |err|
            @next = nil
            clear_last_result
            fail(err)
          end
        else
          # start as soon as client is set
          @next = :start
        end
        nil
      end

      private

      def handle_result(result)
        last_result = clear_last_result
        if result
          begin
            result.check
          rescue PG::Error => e
            result.clear
            send_proc = client.instance_variable_get(:@watcher).send_proc
            client.get_last_result_defer { fail(e) }
            # restart after autoreconnect only if no results has been processed
            if send_proc && !last_result
              client.instance_variable_get(:@watcher).set_send_proc send_proc
            end
          else
            @last_result = result
            call_block_safely(foreach, result)
          end
        else
          succeed
        end
      end

      def call_block_safely(block, result)
        block.call result, self
      rescue => e
        @next = false
        clear_last_result
        client.get_last_result_defer { fail(e) }
      end

      public

      # Retrieves the next result from a call to +Client#send_query+ (or another
      # asynchronous command). If no more results are available returns
      # +nil+. Clears any previous result.
      # @return [PG::Result|nil]
      #
      # @see #each_result #each_result for more advanced iteration
      def next_result
        return if finished?
        each_result.first
      end

      # Resets client connection and finishes iteration
      # either with success passing +:stop+ to callback or
      # with failure when connection reset fails.
      # Clears any remaining result.
      # May be called in both blocking and asynchronous context,
      # inside or outside of +foreach+ handler safely.
      # @return [self]
      def stop
        return self if finished? || !client || @stop
        if ::EM.reactor_running?
          if @next.is_a?(::EM::Deferrable)
            @stop = ::PG::EM::FeaturedDeferrable.new
            @next.completion do
              @stop.bind_status client.reset_defer.
                callback { succeed :stop }.
                errback  {|e| fail e }
            end
          else
            (@stop = client.reset_defer).
              callback { succeed :stop }.
              errback  {|e| fail e }
          end
          self
        else
          begin
            client.reset
          rescue => e
            fail(e)
          else
            succeed :stop
            :stop
          end
        end
      end

      # Calls asynchronous #stop and waits for the reset to finish.
      #
      # May be called in both blocking and asynchronous context.
      # @return [self]
      def stop_sync
        if ::EM.reactor_running? && !(f = Fiber.current).equal?(ROOT_FIBER)
          sync stop, f
        else
          stop
        end
      end

      # Synchronizes +fiber+ with provided +df+ deferrable.
      def sync(df, fiber = Fiber.current)
        f = nil
        df.completion do |res|
          if f then f.resume res else return res end
        end
        f = fiber
        Fiber.yield
      end

      private

      def clear_last_result
        if @last_result
          @last_result.clear
          @last_result = nil
          true
        end
      end
    end

  end
end
