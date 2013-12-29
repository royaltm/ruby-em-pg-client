module PG
  module EM

    # Deferrable with error protectors
    #
    # Author:: Rafal Michalski
    class FeaturedDeferrable < ::EM::DefaultDeferrable

      def initialize(&blk)
        completion(&blk) if block_given?
      end

      def completion(&blk)
        callback(&blk)
        errback(&blk)
      end

      def protect(fail_value = nil)
        yield
      rescue Exception => e
        ::EM.next_tick { fail e }
        fail_value
      end

      def protect_and_succeed(fail_value = nil)
        ret = yield
      rescue Exception => e
        ::EM.next_tick { fail e }
        fail_value
      else
        ::EM.next_tick { succeed ret }
        ret
      end

      # bind deferred status of this deferrable to other +df+
      def bind_status(df)
        df.callback { |*a| succeed(*a) }
        df.errback  { |*a| fail(*a) }
      end
    end

  end
end
