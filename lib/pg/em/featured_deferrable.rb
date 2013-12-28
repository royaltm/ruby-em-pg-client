module PG
  module EM

    # Deferrable with error protectors
    #
    # Author:: Rafal Michalski (mailto:royaltm75@gmail.com)
    # Licence:: MIT License
    class FeaturedDeferrable < ::EM::DefaultDeferrable

      def initialize(&blk)
        if block_given?
          callback(&blk)
          errback(&blk)
        end
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
    end

  end
end
