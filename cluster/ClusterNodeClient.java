package grakn.client.cluster;

import grakn.client.common.rpc.GraknChannel;
import grakn.client.core.CoreClient;

import javax.annotation.Nullable;
import java.nio.file.Path;

class ClusterNodeClient extends CoreClient {
    public ClusterNodeClient(String address, GraknChannel graknChannel, int parallelisation) {
        super(address, graknChannel, parallelisation);
    }

    static ClusterNodeClient create(String address, boolean tlsEnabled, @Nullable Path tlsRootCA, int parallelisation) {
        GraknChannel channel;
        if (tlsEnabled) {
            channel = tlsRootCA != null ? new GraknChannel.TLS(tlsRootCA) : new GraknChannel.TLS();

        } else {
            channel = new GraknChannel.PlainText();
        }
        return new ClusterNodeClient(address, channel, parallelisation);
    }
}
