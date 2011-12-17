require 'zk/enum.rb'

class TestEnum
    include Enumeration

    enum :one, 1
    enum :two, -2
end

class TestError < StandardError
    include Enumeration
    enum :badone, 10
    enum :oops,20
end

describe Enumeration do
    it "should equate to itself" do
        TestEnum::ONE.should == TestEnum::ONE
    end

    it "should have case equals comparisons to symbols and ints" do
        t = TestEnum.fetch(1)
        t.should === :one
        t.should === 1
    end

    it "should be useable in a mock" do
        m = mock("watchevent")
        m.should_receive(:test).with(TestEnum::TWO)
        m.test(TestEnum::TWO)
    end

    context "errors" do
        it "should be raisable with get" do
            begin
                raise TestError.get(:badone), "mymessage"
            rescue TestError::BADONE
            end
        end

        it "should be raisable with constant" do
            begin
                raise TestError::OOPS
            rescue TestError::fetch(20)
            end
        end

        it "should catch the superclass" do
            begin
                raise TestError::BADONE, "a message"
            rescue TestError
            end
        end

        it "should be raisable with unknown" do
            begin
                raise TestError.lookup(:unknown)
            rescue TestError => err
                err.to_sym.should == :unknown
            end
        end
    end
end
