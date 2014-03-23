require 'pg/em/iterator'
module PG
  module EM

    # This iterator is best served in a single_row_mode.
    # Used by IterableMixin#query_stream_defer and IterableMixin#query_stream.
    class TupleIterator < Iterator
      # Iterates asynchronously using the specified block or proc over each
      # first result tuple from a call to +Client#send_query+ (or another asynchronous
      # command) followed by Client#set_single_row_mode and immediately returns
      # with an iterator.
      #
      # @return [PG::EM::TupleIterator] - self
      #
      # @yieldparam tuple [Hash] - first tuple from a single result
      # @yieldparam iter [PG::EM::Iterator] - iterator instance
      #
      # The block should invoke +iter.next+ when it has finished
      # processing +tuple+.
      #
      # @macro iterator_each_deferrable_api
      #
      # @see IterableMixin#query_stream_defer
      def each_defer(foreach = nil, after = nil, &blk)
        foreach ||= blk or raise ArgumentError, 'proc or block required for iteration' 
        each_result_defer(nil, after) do |result|
          if result.ntuples.zero?
            self.next
          else
            foreach.call result[0], self
          end
        end
      end

      # Iterates asynchronously using the specified block or proc over each
      # first result row from a call to +Client#send_query+ (or another asynchronous
      # command) followed by Client#set_single_row_mode and immediately returns
      # with an iterator.
      #
      # @return [PG::EM::TupleIterator] - self
      #
      # @yieldparam row [Array] - first row from a single result
      # @yieldparam iter [PG::EM::Iterator] - iterator instance
      #
      # The block should invoke +iter.next+ when it has finished
      # processing +row+.
      #
      # @macro iterator_each_deferrable_api
      #
      # @see IterableMixin#query_stream_defer
      def each_row_defer(foreach = nil, after = nil, &blk)
        foreach ||= blk or raise ArgumentError, 'proc or block required for iteration' 
        each_result_defer(nil, after) do |result|
          if result.ntuples.zero?
            self.next
          else
            result.each_row do |row|
              foreach.call row, self
              break
            end
          end
        end
      end

      # Iterate, using the specified block, over each tuple from
      # a call to +Client#send_query+ (or another asynchronous command).
      #
      # @macro iterator_each_api
      #
      # @yieldparam tuple [Hash] - first tuple from a single result
      #
      # @example Simple iteration
      #  iter = pg.query_stream('select value from foo')
      #  iter.each_with_index.map do |row, i|
      #    if i > 100
      #      iter.stop
      #    end
      #    row[:value]
      #  end
      #
      # @example Advanced enumerator
      #  iter = pg.query_stream('select * from foo')
      #  iter.each_with_index do |t, i|
      #    puts i, t.inspect
      #    break if i >= 4
      #  end
      #  enum = iter.each_with_index
      #  5.times do
      #    result, i = enum.next
      #    puts i, result.inspect
      #  end
      #  puts iter.take(5).inspect
      #  iter.sync_stop
      #
      # @see #each_defer
      # @see IterableMixin#query_stream
      def each(&blk)
        if block_given?
          each_result do |result|
            result.each(&blk)
          end
        else
          to_enum
        end
      end

      # Iterate, using the specified block, over each row from
      # a call to +Client#send_query+ (or another asynchronous command).
      #
      # @macro iterator_each_api
      #
      # @yieldparam row [Array] - first row from a single result
      #
      # @see #each_defer
      # @see IterableMixin#query_stream
      #
      # @example Simple iteration
      #  iter = pg.query_stream('select value from foo')
      #  iter.each_row.with_index.map do |row, i|
      #    if i > 100
      #      iter.stop
      #    end
      #    row.first
      #  end
      #
      # @example Advanced enumerator
      #  iter = pg.query_stream('select * from foo')
      #  iter.each_row.with_index do |t, i|
      #    puts i, t.inspect
      #    break if i >= 4
      #  end
      #  enum = iter.each_row.with_index
      #  5.times do
      #    result, i = enum.next
      #    puts i, result.inspect
      #  end
      #  puts iter.each_row.take(5).inspect
      #  iter.sync_stop
      def each_row(&blk)
        if block_given?
          each_result do |result|
            result.each_row(&blk)
          end
        else
          to_enum(:each_row)
        end
      end

    end

  end
end
