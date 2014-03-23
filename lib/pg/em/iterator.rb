require 'pg/em'
module PG
  module EM
    # Author:: Rafal Michalski
    #
    # Asynchrony aware iterator over results from previous command query
    # providing methods for both callback-style deferrables and
    # fiber-synchronized enumerators.
    class Iterator
      include DeferrableFeatures
      include Enumerable

      attr_reader :client, :foreach

      def initialize(client = nil)
        @client = client
        @foreach = nil
        @last_result = nil
        @deferred_status = :unknown
        @next = nil
        @stop = nil
      end

      # Return +true+ if iteration has finished, failed or has been stopped.
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
      #   success callback on +iterator+.
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
      # The block should invoke +iter.next+ when it has finished
      # processing the +result+.
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
      #   When there are no more results the iteration terminates returning self.
      #   
      #   Calling {#stop} will interrupt current query, reset the connection and
      #   terminate iteration. It is safe to call {#stop} from inside of the
      #   iterator block. In this instance +:stop+ is returned.
      #   
      #   Returns +nil+ if +break+ is invoked form inside of the iterator block.
      #   In this instance it's possible to call this method again to retrieve
      #   the remaining results.
      #   
      #   Returns an Enumerator if block is not provided.
      #   
      #   Whenever there is a result error the iteration terminates immediately
      #   and the error is being raised.
      #   
      #   @note Since Ruby 2.0 the #lazy enumerator may come in handy while
      #         dealing with large datasets. Unfortunataley due to the bug:
      #         https://bugs.ruby-lang.org/issues/9664 in MRI 2, this method
      #         will not be usable until it is fixed.
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
            ret = self
            fiber = f
            result = nil
            each_result_defer do |res|
              if fiber
                fiber.transfer res
              else
                result = res
              end
            end.completion do |arg|
              ret = arg if arg
              fiber.transfer if fiber
            end
            while result ||= ROOT_FIBER.transfer
              fiber = nil
              block.call result
              result = nil
              self.next
              fiber = f
            end
            fiber = nil
            # if @stop
            #   if (err = sync @stop, f).is_a?(Exception) then raise err end
            # end
            raise ret if ret.is_a?(Exception)
            ret
          else
            # blocking implementation
            callback do |res|
              return res
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
              ensure
                result.clear
              end
            end
            succeed
          end
        else
          to_enum(:each_result)
        end
      end

      def client=(client)
        @client = client
        ::EM.next_tick do
          if @next == :start
            @next = nil
            self.next
          end
        end if @next == :start
      end

      # Fetches next result asynchronously and invokes foreach callback
      # or finishes iteration with either success or failure.
      # Clears any previous result.
      # It's tail call optimized if results are available immediately.
      def next
        return if @stop
        @next || begin
          if client
            if @next == false
              # only mark that next is expected
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
        end
        nil
      end

      private

      def handle_result(result)
        clear_last_result
        if result
          begin
            result.check
          rescue PG::Error => e
            result.clear
            client.get_last_result_defer { fail(e) }
          else
            @last_result = result
            foreach.call result, self
          end
        else
          succeed
        end
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
      # May be called in both blocking and asynchronous context.
      def stop
        return @stop if finished? || !client || @stop
        clear_last_result
        if ::EM.reactor_running?
          (@stop = client.reset_defer).
            callback { @stop = nil; succeed :stop }.
            errback  {|e| @stop = nil; fail e }
        else
          begin
            client.reset
          rescue => e
            fail(e)
          else
            succeed :stop
          end
        end
      end

      # Calls asynchronous #stop waiting for the reset to finish.
      # This method is primarily to be called outside of an iterator block.
      # May be called in both blocking and asynchronous context.
      def sync_stop
        return if finished?
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
        end
      end
    end

  end
end
