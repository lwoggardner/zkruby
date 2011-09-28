require 'spec_helper'

describe "WatchEvent" do
    it "should implement case equality" do
        e = ZK::WatchEvent.fetch(4)

        e.should === :node_children_changed

        c = case e
            when :node_children_changed
               true
            else
               false
            end
        c.should be_true
    end

end
