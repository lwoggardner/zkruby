module ZooKeeper

    module Proto

        #MultiTransactionRecord and MultiResponse in java
        #are not able to be compiled from Jute because jute doesn't support
        #discriminators
        class MultiRequest < BinData::Record
            array :requests, :read_until => lambda { element.header._type < 0 } do
                multi_header :header
                choice :request, :selection => lambda { header._type },
                    :onlyif => lambda { header._type > 0 } do
                    create_request 1
                    delete_request 2
                    set_data_request 5
                    check_version_request 13
                    end
            end

        end

        class MultiResponseItem < BinData::Record
            multi_header :header
            choice :response, :selection => lambda { header._type },
                :onlyif => lambda { has_response? } do
                error_response -1
                create_response 1
                set_data_response 5
                end

            def has_response?
                !done? && [ -1, 1, 5 ] .include?(header._type)
            end

            def done?
                # Boolean's don't actually work properly in bindata
                header.done == true
            end

        end

        class MultiResponse < BinData::Record
            array :responses, :type => :multi_response_item, :read_until => lambda { element.done? }
        end
    end
end

