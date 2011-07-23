module ZooKeeper
 module Data
  class Identity < BinData::Record
    zk_string :scheme
    zk_string :identity
  end
  class ACL < BinData::Record
    int32be :perms
    identity :identity
  end
  class Stat < BinData::Record
    int64be :czxid
    int64be :mzxid
    int64be :ctime
    int64be :mtime
    int32be :version
    int32be :cversion
    int32be :aversion
    int64be :ephemeral_owner
    int32be :data_length
    int32be :num_children
    int64be :pzxid
  end
  class StatPersisted < BinData::Record
    int64be :czxid
    int64be :mzxid
    int64be :ctime
    int64be :mtime
    int32be :version
    int32be :cversion
    int32be :aversion
    int64be :ephemeral_owner
    int64be :pzxid
  end
  class StatPersistedV1 < BinData::Record
    int64be :czxid
    int64be :mzxid
    int64be :ctime
    int64be :mtime
    int32be :version
    int32be :cversion
    int32be :aversion
    int64be :ephemeral_owner
  end
 end
 module Proto
  class OpResultT < BinData::Record
    int32be :rc
    int32be :op
    zk_buffer :response
  end
  class ConnectRequest < BinData::Record
    int32be :protocol_version
    int64be :last_zxid_seen
    int32be :time_out
    int64be :session_id
    zk_buffer :passwd
  end
  class ConnectResponse < BinData::Record
    int32be :protocol_version
    int32be :time_out
    int64be :session_id
    zk_buffer :passwd
  end
  class SetWatches < BinData::Record
    int64be :relative_zxid
    hide :data_watches__length
    int32be :data_watches__length, :value => lambda { data_watches.length }
    array :data_watches, :type => :zk_string, :initial_length => :data_watches__length
    hide :exist_watches__length
    int32be :exist_watches__length, :value => lambda { exist_watches.length }
    array :exist_watches, :type => :zk_string, :initial_length => :exist_watches__length
    hide :child_watches__length
    int32be :child_watches__length, :value => lambda { child_watches.length }
    array :child_watches, :type => :zk_string, :initial_length => :child_watches__length
  end
  class RequestHeader < BinData::Record
    int32be :xid
    int32be :_type
  end
  class AuthPacket < BinData::Record
    int32be :_type
    zk_string :scheme
    zk_buffer :auth
  end
  class ReplyHeader < BinData::Record
    int32be :xid
    int64be :zxid
    int32be :err
  end
  class GetDataRequest < BinData::Record
    zk_string :path
    zk_boolean :watch
  end
  class SetDataRequest < BinData::Record
    zk_string :path
    zk_buffer :data
    int32be :version
  end
  class SetDataResponse < BinData::Record
    stat :stat
  end
  class CreateRequest < BinData::Record
    zk_string :path
    zk_buffer :data
    hide :acl__length
    int32be :acl__length, :value => lambda { acl.length }
    array :acl, :type => :acl, :initial_length => :acl__length
    int32be :flags
  end
  class DeleteRequest < BinData::Record
    zk_string :path
    int32be :version
  end
  class GetChildrenRequest < BinData::Record
    zk_string :path
    zk_boolean :watch
  end
  class GetChildren2Request < BinData::Record
    zk_string :path
    zk_boolean :watch
  end
  class GetMaxChildrenRequest < BinData::Record
    zk_string :path
  end
  class GetMaxChildrenResponse < BinData::Record
    int32be :maximum
  end
  class SetMaxChildrenRequest < BinData::Record
    zk_string :path
    int32be :maximum
  end
  class SyncRequest < BinData::Record
    zk_string :path
  end
  class SyncResponse < BinData::Record
    zk_string :path
  end
  class GetACLRequest < BinData::Record
    zk_string :path
  end
  class SetACLRequest < BinData::Record
    zk_string :path
    hide :acl__length
    int32be :acl__length, :value => lambda { acl.length }
    array :acl, :type => :acl, :initial_length => :acl__length
    int32be :version
  end
  class SetACLResponse < BinData::Record
    stat :stat
  end
  class WatcherEvent < BinData::Record
    int32be :_type
    int32be :state
    zk_string :path
  end
  class CreateResponse < BinData::Record
    zk_string :path
  end
  class ExistsRequest < BinData::Record
    zk_string :path
    zk_boolean :watch
  end
  class ExistsResponse < BinData::Record
    stat :stat
  end
  class GetDataResponse < BinData::Record
    zk_buffer :data
    stat :stat
  end
  class GetChildrenResponse < BinData::Record
    hide :children__length
    int32be :children__length, :value => lambda { children.length }
    array :children, :type => :zk_string, :initial_length => :children__length
  end
  class GetChildren2Response < BinData::Record
    hide :children__length
    int32be :children__length, :value => lambda { children.length }
    array :children, :type => :zk_string, :initial_length => :children__length
    stat :stat
  end
  class GetACLResponse < BinData::Record
    hide :acl__length
    int32be :acl__length, :value => lambda { acl.length }
    array :acl, :type => :acl, :initial_length => :acl__length
    stat :stat
  end
 end
end
