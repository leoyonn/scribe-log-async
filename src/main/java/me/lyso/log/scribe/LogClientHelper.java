/**
 * LogClientHelper.java
 * [CopyRight]
 * @author leo [leoyonn@gmail.com]
 * @date Sep 9, 2013 2:57:45 PM
 */
package me.lyso.log.scribe;

import java.security.SecureRandom;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.lyso.servicepool.Endpoint;
import me.lyso.servicepool.EndpointChooser;
import me.lyso.servicepool.EndpointClientConfigs;
import me.lyso.servicepool.EndpointPool;
import me.lyso.thrift.ClientFactory;
import me.lyso.zookeeper.ZKClient;
import me.lyso.zookeeper.ZKFacade;

/**
 * Helper for get scribe client from zookeeper configure.
 * 
 * @author leo
 */
public class LogClientHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogClientHelper.class);
    private static final Random rand = new SecureRandom();

    /**
     * get a scribe client connect to server described in zkPath, which is "servers=host1:port1,host2:port2..."
     * 
     * @param zkPath
     * @return
     * @throws Exception
     */
    public static scribe.thrift.scribe.Iface getScribeClientFromZookeeper(String zkPath) throws Exception {
        ZKClient zkClient = ZKFacade.getClient();
        Properties prop = zkClient.getData(Properties.class, zkPath);
        String[] addrs = prop.getProperty("servers", "localhost:1463").split(",");
        final List<Endpoint> servers = new ArrayList<Endpoint>(addrs.length);
        for (String addr : addrs) {
            String[] hp = addr.split(":");
            if (hp.length != 2) {
                LOGGER.info("^#Red.read-logserver-from-zk: invalid host-port pair: {}", addr);
                continue;
            }
            int port = 1463;
            try {
                port = Integer.parseInt(hp[1]);
            } catch (NumberFormatException ex) {
                LOGGER.info("^#Red.parse-logserver: invalid port : {}", hp[1]);
                continue;
            }
            servers.add(new Endpoint(hp[0], port));
        }
        return getScribeClient(servers);
    }

    /**
     * Get a scribe client connect to #host:#port
     *
     * @param host
     * @param port
     * @return
     * @throws Exception
     */
    public static scribe.thrift.scribe.Iface getScribeClient(final String host, final int port) throws Exception {
        return getScribeClient(Arrays.asList(new Endpoint(host, port)));
    }

    /**
     * Get a scribe client connect to randomly choosed host:port from #servers
     *
     * @param servers
     * @return
     */
    public static scribe.thrift.scribe.Iface getScribeClient(final List<Endpoint> servers) {
        EndpointClientConfigs<Class<?>, Endpoint> emptyConfigs = EndpointClientConfigs.getEmptyConfigs();
        return ClientFactory.createClient(scribe.thrift.scribe.Iface.class, 5000,
                new EndpointChooser<Endpoint>() {
                    @Override
                    public Endpoint choose(EndpointPool<Endpoint> pool, Collection<Endpoint> invalidEndpoints) {
                        servers.removeAll(invalidEndpoints);
                        return servers.isEmpty() ? null : servers.get(rand.nextInt(servers.size()));
                    }
                }, emptyConfigs
        );
    }
}
