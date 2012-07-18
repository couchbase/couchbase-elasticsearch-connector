package org.elasticsearch.transport.couchbase;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.transport.BoundTransportAddress;

public interface CouchbaseCAPITransport extends LifecycleComponent<CouchbaseCAPITransport> {

    BoundTransportAddress boundAddress();

}
