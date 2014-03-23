module PG
  module EM

    # Deferrable with error protectors
    #
    # Author:: Rafal Michalski
    module DeferrableFeatures
      include ::EM::Deferrable

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

    class FeaturedDeferrable
      include DeferrableFeatures

      def initialize(&blk)
        completion(&blk) if block_given?
      end
    end

  end
end
