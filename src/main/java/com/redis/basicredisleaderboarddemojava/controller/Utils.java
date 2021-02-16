package com.redis.basicredisleaderboarddemojava.controller;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class Utils {
    public static String resetData(boolean isDataReady, Jedis jedis, String data_ready_redis_key,
                                   String redis_leaderboard) {
        boolean is_ok = true;
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
            } catch (Exception e) {
                is_ok = false;
            }
        }
        return String.format("{\"succes\":%s}", is_ok);
    }

    protected static String getRedisDataZrangeWithScores(int start, int end, Jedis jedis, String redis_leaderboard) {
        Set<Tuple> zrangeWithScores =  jedis.zrangeWithScores(redis_leaderboard, start, end);
        return resultList(zrangeWithScores, jedis,
                new AtomicInteger((zrangeWithScores.size() + 1) / (1 - start)),
                false);
    }

    protected static String getRedisDataZrevrangeWithScores(int start, int end, Jedis jedis, String redis_leaderboard) {
        return resultList(jedis.zrevrangeWithScores(redis_leaderboard, start, end), jedis,
                new AtomicInteger(start),
                true);
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

    private static String resultList(Set<Tuple> jedisRedis, Jedis jedis, AtomicInteger index, boolean isIncrease) {
        List<JSONObject> resultList = new ArrayList<>();
        jedisRedis.forEach((k) -> {

            Map<String, String> company = jedis.hgetAll(k.getElement());
            JSONObject json = addDataToResult(company, ((Double) k.getScore()).longValue(), k.getElement());
            try {
                json.put("rank",  isIncrease ? index.incrementAndGet() : index.decrementAndGet());
            } catch (JSONException e) {
                e.printStackTrace();
            }
            resultList.add(json);
        });
        return resultList.toString();
    }

    protected static JSONObject addDataToResult(Map<String, String> company, Long marketCap, String symbol){
        JSONObject json = new JSONObject();
        try {
            json.put("marketCap", (marketCap));
            json.put("symbol", symbol);
            json.put("country", company.get("country"));
            json.put("company", company.get("company"));
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return json;
    }

}
