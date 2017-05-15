package com.nucypher.kafka.zk;

import com.nucypher.kafka.errors.CommonException;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Curator testing server with SASL as resource
 * 
 * @author szotov
 *
 */
public final class ZooKeeperSASLResource extends ExternalResource { 
    private TestingServer testingServer; 
    private CuratorFramework curatorFramework;
    private CuratorFramework adminCuratorFramework;
    private RetryPolicy retryPolicy; 
 
    public ZooKeeperSASLResource() { 
        this(new RetryOneTime(1000)); 
    } 
 
    public ZooKeeperSASLResource(RetryPolicy curatorRetryPolicy) { 
        this.retryPolicy = curatorRetryPolicy; 
    } 
 
    @Override 
    protected void before() throws Exception { 
    	String jaas = getClass().getResource("/jaas_test.conf").getPath();
    	System.setProperty("java.security.auth.login.config", jaas);
        System.setProperty("zookeeper.DigestAuthenticationProvider.superDigest",
                "zkAdmin:AjgEq8UnRSdnG/GV6d2ONoelKZc=");
    	
    	Map<String,Object> customProperties = new HashMap<>();
    	customProperties.put("authProvider.1", 
    			"org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
		InstanceSpec spec = new InstanceSpec(null, -1, -1, -1, true, 1, -1, -1, customProperties);
        testingServer = new TestingServer(spec, true); 
        curatorFramework = CuratorFrameworkFactory.newClient(
        		testingServer.getConnectString(), retryPolicy); 
        curatorFramework.start();
        adminCuratorFramework = CuratorFrameworkFactory.builder()
                .authorization("digest", "zkAdmin:123".getBytes())
                .connectString(testingServer.getConnectString())
                .retryPolicy(retryPolicy)
                .build();
        adminCuratorFramework.start();
        //curatorFramework.blockUntilConnected();
        //blockUntilConnected not worked for simple zooKeeper connection
        curatorFramework.checkExists().forPath("/");
        System.out.println("ZooKeeper server was started");
    } 
 
    @Override 
    protected void after() { 
        try { 
            curatorFramework.close();
            testingServer.stop(); 
        } catch (IOException e) { 
            throw new CommonException(e);
        }
        System.out.println("ZooKeeper server was stopped");
    } 
 
    /**
     * @return {@link TestingServer}
     */
    public TestingServer getZooKeeperTestingServer() { 
        return testingServer; 
    } 
 
    /**
     * @return {@link CuratorFramework}
     */
    public CuratorFramework getApacheCuratorFramework() { 
        return curatorFramework;
    }

    /**
     * @return {@link CuratorFramework} with admin privileges
     */
    public CuratorFramework getAdminApacheCuratorFramework() {
        return adminCuratorFramework;
    }
}