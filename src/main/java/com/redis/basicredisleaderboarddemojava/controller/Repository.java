package com.redis.basicredisleaderboarddemojava.controller;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletResponse;

@RestController
@Service
public class Repository {
    @Value("${REDIS_URL}")
    private String properties_uri;

    @Value("${redis_leaderboard}")
    private String redis_leaderboard;

    Jedis jedis;
    private void getConnection() {
        if (jedis == null) {
            String REDIS_URL = System.getenv("REDIS_URL");

            if (REDIS_URL == null) {
                REDIS_URL = properties_uri;
            }
            jedis = new Jedis(REDIS_URL);
        }
    }

    @RequestMapping(value = "/api/list/top10", produces = { "text/html; charset=utf-8" })
    @ResponseBody
    public String getTop10(HttpServletResponse response
    ) {
        return getRedisDataZrevrangeWithScores(0, 9);
    }

    @RequestMapping(value = "/api/list/all", produces = { "text/html; charset=utf-8" })
    @ResponseBody
    public String getAll(HttpServletResponse response
    ) {
        return getRedisDataZrevrangeWithScores(0, -1);
    }

    @RequestMapping(value = "/api/list/bottom10", produces = { "text/html; charset=utf-8" })
    @ResponseBody
    public String get10(HttpServletResponse response
    ) {
        return getRedisDataZrangeWithScores(0, 9);
    }

    @RequestMapping("/api/list/inRank")
    @ResponseBody
    public String getinRank(HttpServletResponse response, @RequestParam(name = "start") int start,
                         @RequestParam(name = "end") int end
    ) {

        return getRedisDataZrevrangeWithScores(start, end);
    }

    @RequestMapping("/api/list/getBySymbol")
    @ResponseBody
    public String getBySymbol(HttpServletResponse response, @RequestParam(name = "symbols") List<String> symbols

    ) {
        jedis = new Jedis(properties_uri);
        List<JSONObject> list = new ArrayList<>();

        for (String symbol : symbols) {
            Long s  =jedis.zscore(redis_leaderboard, symbol).longValue();
            JSONObject json = new JSONObject();
            Map<String, String> company = jedis.hgetAll(symbol);
            try {
                json.put("marketCap", s);
                json.put("symbol",symbol);
                json.put("country", company.get("country"));
                json.put("company", company.get("company"));

            } catch (JSONException e) {
                e.printStackTrace();
            }
            list.add(json);
        }

        return list.toString();
    }
//
//    @RequestMapping(value = "/api/list/bottom10", produces = { "text/html; charset=utf-8" })
//    @ResponseBody
//    @RequestParam(required = false, defaultValue = "0") int start,
//    @RequestParam(required = false, defaultValue = "10") int limit)
//



    protected String getRedisDataZrevrangeWithScores(int start, int end) {
        jedis = new Jedis(properties_uri);
        List<JSONObject> topList = new ArrayList<>();
        AtomicInteger index = new AtomicInteger(0);
        jedis.zrevrangeWithScores(redis_leaderboard, start, end).forEach((k) -> {
            JSONObject json = new JSONObject();
            Map<String, String> company = jedis.hgetAll(k.getElement());
            try {
                json.put("marketCap", ((Double) k.getScore()).longValue());
                json.put("symbol", k.getElement());
                json.put("rank", index.incrementAndGet());
                json.put("country", company.get("country"));
                json.put("company", company.get("company"));

            } catch (JSONException e) {
                e.printStackTrace();
            }
            topList.add(json);
        });
        return topList.toString();
    }

    protected String getRedisDataZrangeWithScores(int start, int end) {
        jedis = new Jedis(properties_uri);
        List<JSONObject> topList = new ArrayList<>();
        AtomicInteger index = new AtomicInteger(0);
        jedis.zrangeWithScores(redis_leaderboard, start, end).forEach((k) -> {
            JSONObject json = new JSONObject();
            Map<String, String> company = jedis.hgetAll(k.getElement());
            try {
                json.put("marketCap", ((Double) k.getScore()).longValue());
                json.put("symbol", k.getElement());
                json.put("rank", index.incrementAndGet());
                json.put("country", company.get("country"));
                json.put("company", company.get("company"));

            } catch (JSONException e) {
                e.printStackTrace();
            }
            topList.add(json);
        });
        return topList.toString();
    }

}
