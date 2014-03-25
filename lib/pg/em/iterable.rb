require 'pg/em'
require 'pg/em/iterator'
require 'pg/em/tuple_iterator'
module PG
  module EM
    # Author:: Rafal Michalski
    #
    # @note This module is experimental
    module IterableMixin

      # @!attribute [rw] result_iterator
      #   @return [PG::EM::TupleIterator] iterator instance
      #   Iterator object set by the last call to {#query_stream} or
      #   {#query_stream_defer}
      attr_accessor :result_iterator

      # Stop iterator from the previous {#query_stream} or
      # {#query_stream_defer} command asynchronously.
      # Returns iterator or nil if there wasn't any iterator.
      # @return [PG::EM::TupleIterator|nil]
      def stop_iterator_defer
        if iter = @result_iterator
          @result_iterator = nil
          iter.stop
        end
      end

      # Stop iterator from the previous {#query_stream} or
      # {#query_stream_defer} command. Synchronize current
      # fiber waiting for stop to complete.
      # Returns :stop or nil if there wasn't any iterator.
      # @return [:stop|nil]
      # @raise [PG::Error]
      def stop_iterator
        if iter = @result_iterator
          @result_iterator = nil
          ret = iter.stop_sync
          raise ret if ret.is_a?(Exception)
          ret
        end
      end

      # @!group Iterator command methods

      # Query server asynchronously providing results in single row mode.
      # @return [PG::EM::TupleIterator] - deferrable iterator instance
      #
      # @yieldparam tuple [Hash] - each tuple of streamed result
      # @yieldparam iter [PG::EM::TupleIterator] - iterator instance
      #
      # If a block is given the {TupleIterator#each_defer} is called with
      # the block before returning iterator.
      #
      # @see PG::EM::TupleIterator#each_defer PG::EM::TupleIterator#each_defer
      #      for explanation of block parameters and usage.
      #
      # @example Tuple streaming
      #  pg.query_stream_defer('select * from foo') do |tuple, iter|
      #    puts tuple.inspect
      #    iter.next
      #  end.callback do
      #    puts 'all done'
      #  end.errback do |err|
      #    puts "error: #{err.inspect}"
      #  end
      #
      # @example Row streaming and aborting after 100 rows
      #  counter = 100
      #  pg.query_stream_defer('select * from foo').each_row_defer do |row, iter|
      #    puts row.inspect
      #    if (counter-=1) > 0
      #      iter.stop
      #    else
      #      iter.next
      #    end
      #  end.callback do
      #    puts 'all done'
      #  end.errback do |err|
      #    puts "error: #{err.inspect}"
      #  end
      def query_stream_defer(*args, &block)
        if !@result_iterator || @result_iterator.finished?
          iter = @result_iterator = TupleIterator.new
        elsif @result_iterator.client.nil?
          iter = @result_iterator
        else
          # stop previous iterator (highly experimental)
          iter = stop_iterator_defer
          @result_iterator = TupleIterator.new
          iter.completion { query_stream_defer(*args, &block) }
          return @result_iterator
        end
        iter.each_defer(&block) if block_given?
        send_proc = proc do
          send_query(*args)
          set_single_row_mode
          iter.client = self
          ::EM.next_tick { iter.next } if iter.foreach
        end
        begin
          check_async_command_aborted!
          @last_transaction_status = transaction_status
          send_query(*args)
          set_single_row_mode
          setup_emio_watcher.set_send_proc(send_proc)
          iter.client = self
        rescue PG::Error => e
          ::EM.next_tick { async_autoreconnect!(iter, e, &send_proc) }
        rescue Exception => e
          ::EM.next_tick { iter.fail(e) }
        end
        iter
      end

      # Query server providing results in single row mode.
      # @return [PG::EM::TupleIterator|:stop|nil]
      #
      # @yieldparam tuple [Hash] - each tuple of streamed result
      #
      # If a block is given the {TupleIterator#each} is called with the block.
      # In this instance the method doesn't return until the iteration
      # is completed or interrupted. When there are no more results the
      # iteration terminates returning {TupleIterator}.
      #
      # Calling {#stop_iterator} will interrupt current query, reset the
      # connection and terminate iteration. It is safe to call
      # {#stop_iterator} or {#stop_iterator_defer} from inside of the
      # iterator block. In this instance +:stop+ is returned.
      #
      # Returns +nil+ if +break+ is invoked form inside of the iterator block.
      # In this instance it's possible to call {#result_iterator}+.each+ or any
      # of its variants to retrieve the remaining results.
      #
      # Returns immediately a {TupleIterator} if block is not provided.
      #
      # May be called in both blocking and asynchronous context.
      #
      # @see PG::EM::TupleIterator#each PG::EM::TupleIterator#each
      # @see PG::EM::TupleIterator#each_row PG::EM::TupleIterator#each_row
      #
      # @example Tuple streaming
      #  pg.query_stream('select * from foo') do |tuple|
      #    puts tuple.inspect
      #  end
      #
      # @example Row streaming and aborting after 100 rows
      #  pg.query_stream('select * from foo').each_row.with_index do |row, index|
      #    puts row.inspect
      #    pg.stop_iterator if index >= 99
      #  end
      def query_stream(*args, &block)
        if ::EM.reactor_running? && !(f = Fiber.current).equal?(ROOT_FIBER)
          iter = query_stream_defer(*args)
        else
          @result_iterator.stop if @result_iterator
          send_query(*args)
          set_single_row_mode
          iter = @result_iterator = TupleIterator.new(self)
        end
        if block_given?
          iter.each(&block)
        else
          iter
        end
      end

      # @!endgroup

    end

    if PG::EM::Client.single_row_mode?
      class Client < PG::Connection
        include IterableMixin
      end

      class ConnectionPool

        def query_stream_defer(*args, &blk)
          iter = TupleIterator.new
          hold_deferred do |client|
            client.result_iterator = iter
            client.query_stream_defer(*args, &blk)
          end.errback { |err| iter.fail(err) }
          iter
        end

        def query_stream(*args, &blk)
          iter = TupleIterator.new
          #  TODO: make hold fiber + deferrable release
          hold_deferred do |client|
            client.result_iterator = iter
            client.query_stream_defer(*args)
          end.errback { |err| iter.fail(err) }
          if block_given?
            res = iter.each(&blk)
            iter.stop_sync
            res
          else
            iter
          end
        end

      end
    end

  end
end
