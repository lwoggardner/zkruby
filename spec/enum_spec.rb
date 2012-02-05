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

    describe "errors" do
        # Ruby 1.9 inadvertently dropped requirement that the argument
        # to rescue must be a class or module, but despite its usefulness
        # it is not supposed to be that way and JRuby doesn't work that way
        it "should be an exception class" do
            TestError.get(:badone).should equal (TestError::BADONE)
            TestError.get(:badone).class.should == Class
            TestError::BADONE.should < StandardError
            TestError::BADONE.new().should be_kind_of(TestError)
            TestError::OOPS.should === TestError.get(:oops).exception()
            TestError::OOPS.to_sym.should == :oops
        end

        it "should be raisable with get and rescuable by a constant" do
            begin
                raise TestError.get(:badone), "mymessage"
            rescue TestError::BADONE => ex
                ex.message.should == "mymessage"
            end
        end

        it "should be raisable with constant and rescuable with fetch" do
            begin
                raise TestError::OOPS, "a message"
            rescue TestError::fetch(20) => ex
                # do nothing
                ex.message.should == "a message"
            end
        end

        it "should catch the superclass" do
            begin
                raise TestError::BADONE, "a message"
            rescue TestError
            end
        end

        it "should be raisable with unknown symbol" do
            begin
                raise TestError.lookup(:unknown)
            rescue TestError => err
            end
        end

        it "should be raisable with unknown int" do
            begin
                raise TestError.lookup(-132)
            rescue TestError => err
            end
        end
    end
end
