require 'spec_helper'

class ZK::MockConnectionFactory

    include ZK::Protocol

    def initialize(connection)
        @connection = connection
    end

    def connect(host,port,timeout)
        session.prime_connection(@connection)
    end

    def recieve_records(io)

    end

    # Session calls
    #
    #
    #

    # including calls

end

# Here we test the behaviour of the session without actually talking to ZooKeeper
describe ZooKeeper::Session do

    let(:client) { double("ZKClient") }
    let(:connection) { double ("ZKConnection") }
    let(:connection_factory) { ZK::MockConnectionFactory.new(connection) }
    let(:session) { ZooKeeper::Session.new("nohost:123",:connection_factory => connection_factory) }

    it "invokes response callbacks"
    it "invokes error callbacks"
    it "invokes connection_lost on pending callbacks when disconnected"
    it "invokes expired error on pending callbacks when the session expires"
    it "expires the session on exception outside of a callback"
    it "does not expire the session on exceptions raised in callbacks"
end

