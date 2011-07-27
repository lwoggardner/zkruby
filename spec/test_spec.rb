require 'spec_helper'

class TestEnum
   include Enumeration
   
   enum :one, 1
   enum :two, -2

end

describe Enumeration do
   
    it "should equate to itself" do
        TestEnum::ONE.should == TestEnum::ONE
    end

    it "should be useable in a mock" do
        m = mock("watchevent")
        m.should_receive(:test).with(TestEnum::TWO)
        m.test(TestEnum::TWO)
    end

end
