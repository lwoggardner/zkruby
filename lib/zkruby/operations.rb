module ZooKeeper

  class Operation
    attr_reader :xid, :watcher, :callback, :op, :op_data
    def initialize(xid, op, op_data, watch, callback)
      @xid,@op,@op_data,@watcher,@callback = xid,op,op_data,watch,callback
      op_data[:watch] = true if watch
    end

    def header
      Proto::RequestHeader.new(xid: xid, _type: op.to_i)
    end

    def request
      op.request_class && op.request_class.new(op_data)
    end

    def error(reason)
      result(reason)[0..2] # don't need the watch
    end

    def result(rc)
      error = nil
      unless (Error::NONE === rc) then
        error = Error.lookup(rc)
        error = error.exception("ZooKeeper error #{error.to_sym} for #{op.to_sym} (#{path}) ")
      end

      response_class = error ? nil : op.response_class

      [ callback, error, response_class, watch_type ]
    end

    def watch_type
      watcher ? op.watch_type : nil
    end

    def path
      op_data[:path] if op_data
    end
  end

  # NoNode error is expected for exists
  class ExistsOperation < Operation
    def result(rc)
      Error::NO_NODE === rc ? [ callback, nil, nil, :exists ] : super(rc)
    end
  end

  class MultiOperation < Operation
    # data for a multi is an array of [ type,data ] pairs
    def request
      req = op.request_class.new()

      op_data.each do |op_type,op_data|
        op = OperationType.lookup(op_type)
        req.requests << { :header => { :_type => op.to_i, :done => false, :err => 0 }, :request => op.request_class.new(op_data) }
      end

      req.requests << { :header => { :_type => -1 , :done => true, :err => -1 } }
      logger.debug ("Requesting multi op #{req}")
      req
    rescue Exception => ex
      logger.error("WTF?",ex)
      raise
    end

    def path
      nil
    end
  end

  class CloseOperation < Operation
    # In the normal case the close packet will be last and will get
    # cleared via disconnected() and :session_expired
    def result(rc)
      Error::SESSION_EXPIRED == rc ? [ callback, nil, nil, nil ] : super(rc)
    end
  end

  class OperationType
    include Enumeration

    def initialize(request_class, response_class = nil, watch_type = nil, op_class = Operation)
      @request_class, @response_class, @watch_type, @op_class = request_class, response_class, watch_type, op_class
    end

    enum :create, 1, Proto::CreateRequest, Proto::CreateResponse
    enum :delete, 2, Proto::DeleteRequest
    enum :exists, 3, Proto::ExistsRequest, Proto::ExistsResponse, :exists, ExistsOperation
    enum :get, 4, Proto::GetDataRequest, Proto::GetDataResponse, :data
    enum :set_data, 5, Proto::SetDataRequest, Proto::SetDataResponse
    enum :get_acl, 6, Proto::GetACLRequest, Proto::GetACLResponse
    enum :set_acl, 7, Proto::SetACLRequest, Proto::SetACLResponse
    enum :sync, 9, Proto::SyncRequest, Proto::SyncResponse
    enum :ping, 11, nil
    enum :get_children2, 12, Proto::GetChildren2Request, Proto::GetChildren2Response, :children
    enum :check, 13, Proto::CheckVersionRequest
    enum :multi, 14, Proto::MultiRequest, Proto::MultiResponse, nil, MultiOperation
    enum :auth, 100, Proto::AuthPacket
    enum :set_watches, 101, Proto::SetWatches
    enum :close, -11, nil, nil, nil, CloseOperation

    attr_reader :request_class, :response_class, :watch_type, :op_class

    def create(xid,op_data,watch,callback)
      op_class.new(xid,self,op_data,watch,callback)
    end

    def op_type
      to_i
    end
  end

end

