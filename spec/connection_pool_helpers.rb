module ConnectionPoolHelpers
  def sleep_one_tick
    f = Fiber.current
    EM.next_tick { f.resume }
    Fiber.yield
  end

  def create_connection
    client.allocate.tap do |conn|
      conn.stub(:query) do |query|
        query.should start_with 'f'
        checkpoint.check_defer
        f = Fiber.current
        conn.instance_eval { @fiber = f }
        EM.next_tick {
          conn.instance_variable_get(:@fiber).should be f
          f.resume
        }
        Fiber.yield
        if query == 'fee'
          conn.should_receive(:status).once.and_return(PG::CONNECTION_BAD)
          conn.should_receive(:finished?).once.and_return(false)
          conn.should_receive(:finish).once
          raise PG::ConnectionBad
        else
          :result
        end
      end
      conn.stub(:query_defer) do |query|
        query.should start_with 'b'
        checkpoint.check_fiber
        deferrable.new.tap do |df|
          conn.instance_eval { @defer = df }
          EM.next_tick {
            conn.instance_variable_get(:@defer).should be df
            if query == 'bzz'
              conn.should_receive(:status).once.and_return(PG::CONNECTION_BAD)
              conn.should_receive(:finished?).once.and_return(false)
              conn.should_receive(:finish).once
              df.fail pgerror
            else
              df.succeed :result
            end
          }
        end
      end
    end
  end

  def test_queries(pool, queries)
    EM.synchrony do
      EM.add_timer(2) { raise 'timeout' }
      progress = Set.new
      queries.each_with_index do |query, i|
        case query
        when 'foo'
          Fiber.new do
            pool.query(query).should be :result
            progress.delete i
          end.resume
        when 'bar'
          pool.query_defer(query).callback do |result|
            result.should be :result
            progress.delete i
          end.should_not_receive(:fail)
        when 'fee'
          Fiber.new do
            expect do
              pool.query(query)
            end.to raise_error(PG::ConnectionBad)
            progress.delete i
          end.resume
        when 'bzz'
          pool.query_defer(query).errback do |err|
            err.should be pgerror
            progress.delete i
          end.should_not_receive(:succeed)
        end
        progress << i
      end
      progress.should eq Set.new(0...queries.length)
      begin
        yield progress
        sleep_one_tick
      end until progress.empty?
      EM.stop
    end
  end
end
