package com.redis.basicredisleaderboarddemojava.createDB;


import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;


@Component
public class StartupHousekeeper implements ApplicationListener<ContextRefreshedEvent> {

    @Value("${REDIS_URL}")
    private String properties_uri;


    @Value("${data_ready_redis_key}")
    private String data_ready_redis_key;

    @Value("${redis_leaderboard}")
    private String redis_leaderboard;


    @Override
    public void onApplicationEvent(final ContextRefreshedEvent event) {
        Jedis jedis = new Jedis(properties_uri);
        boolean isDataReady = Boolean.parseBoolean(jedis.get(data_ready_redis_key));
        if (!isDataReady){
            try {
                JSONArray companyJsonArray = new JSONArray(readFile("src/main/resources/data.json"));
                JSONObject companyJson;
                String symbol;
                for (int i=0; i<companyJsonArray.length(); i++) {
                    companyJson = new JSONObject(companyJsonArray.getString(i));
                    symbol = companyJson.get("symbol").toString().toLowerCase();
                    jedis.zadd(redis_leaderboard, Double.parseDouble(companyJson.get("marketCap").toString()), symbol);
                    jedis.hset(symbol, "company", companyJson.get("company").toString());
                    jedis.hset(symbol, "country", companyJson.get("country").toString());
                }
                jedis.set(data_ready_redis_key, "true");
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    private static String readFile(String filename) {
        String result = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
            result = sb.toString();
        } catch(Exception e) {
            e.printStackTrace();
        }
        return result;
    }

}