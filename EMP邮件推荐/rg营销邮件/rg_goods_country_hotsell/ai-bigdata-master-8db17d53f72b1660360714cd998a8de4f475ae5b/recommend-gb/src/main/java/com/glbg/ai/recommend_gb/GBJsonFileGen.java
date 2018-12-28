package com.glbg.ai.recommend_gb;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.json.JSONObject;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;


import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class GBJsonFileGen {

    /**
     * 日志
     */
    public static final Log LOG = LogFactory.getLog(GBJsonFileGen.class);

    private static final int IP_INDEX = 0;

    private static final int PORT_INDEX = 1;

    private static Properties getRedisConfig() {
        Properties props = new Properties();

        InputStream is = null;
        try {
            is = GBJsonFileGen.class.getClassLoader().getResourceAsStream("redis-cluster.properties");
            props.load(is);
        } catch (IOException e) {
            LOG.error(e.toString(), e);
        } finally {
            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    LOG.error(e.toString(), e);
                }
            }
        }
        return props;
    }

    private static List<String> getHostsAndPort(Properties props) {
        List<String> lists = new ArrayList<String>();
        for (Map.Entry<Object, Object> one : props.entrySet()) {
            lists.add((String) one.getValue());
        }
        LOG.info("Redis cluster info : " + lists);
        return lists;
    }

    private static JedisCluster initJedisCluster(Properties props) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();

        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        for (String one : getHostsAndPort(props)) {
            String[] splitHostAndPort = one.split(":");
            HostAndPort hostAndPort = new HostAndPort(splitHostAndPort[IP_INDEX],
                    Integer.valueOf(splitHostAndPort[PORT_INDEX]));

            nodes.add(hostAndPort);
        }
        return new JedisCluster(nodes, poolConfig);// JedisCluster中默认分装好了连接池
    }

    public static void main(String[] args) throws Exception {

        Properties prop = getRedisConfig();
        JedisCluster jedisCluster = initJedisCluster(prop);

        List<String> pipLangList = Arrays.asList("GB_de", "GB_en", "GB_ep", "GB_fr",
                "GB_it", "GB_po", "GB_pt-br", "GB_ru", "GB_tr", "GBAU_en", "GBBR_pt-br",
                "GBCZ_cs", "GBDE_de", "GBES_ep", "GBFR_fr", "GBGR_el", "GBHU_hu", "GBIN_en",
                "GBIT_it", "GBJP_ja", "GBMA_fr", "GBMX_ep-mx", "GBNL_nl", "GBPL_pl", "GBPT_po",
                "GBRO_ro", "GBRU_ru", "GBSK_sk", "GBTR_tr", "GBUK_en", "GBUS_en");

        String prefix = "goods_static_backup_result";

        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);

        for (String key : pipLangList) {
            String redisKey = prefix + "_" + key;

            List<String> values = jedisCluster.srandmember(redisKey, 30);
            JSONArray jsonArray = new JSONArray(values);

            JSONObject json = new JSONObject();
            json.put("bucketid", "1");
            json.put("msg", "ok");
            json.put("planid", "1");
            json.put("result", jsonArray);
            json.put("status", "1");
            json.put("versionid", "1");

            String pathKey = key.replace("_", "");
            String fileName = pathKey + ".json";
            Path filenamePath = new Path("/user/zhanrui/jsonfile/" + fileName);

            if (fs.exists(filenamePath)) {
                fs.delete(filenamePath, true);
            }

            FSDataOutputStream out = fs.create(filenamePath);
            out.writeUTF(json.toString());
            out.close();

        }


    }


}
