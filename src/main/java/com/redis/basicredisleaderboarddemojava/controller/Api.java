package com.redis.basicredisleaderboarddemojava.controller;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;

import java.util.*;
import javax.servlet.http.HttpServletResponse;

import static com.redis.basicredisleaderboarddemojava.controller.Utils.*;


@RestController
@Service
@Component
public class Api implements ApplicationListener<ContextRefreshedEvent> {
    @Value("${REDIS_URL}")
    private String properties_uri;

    @Value("${redis_leaderboard}")
    private String redis_leaderboard;

    @Value("${data_ready_redis_key}")
    private String data_ready_redis_key;

    Jedis jedis;


    @RequestMapping(value = "/api/list/top10", produces = { "text/html; charset=utf-8" })
    @ResponseBody
    public String getTop10(HttpServletResponse response
    ) {
        return getRedisDataZrevrangeWithScores(0, 9, jedis, redis_leaderboard);
    }

    @RequestMapping(value = "/api/list/all", produces = { "text/html; charset=utf-8" })
    @ResponseBody
    public String getAll(HttpServletResponse response
    ) {
        return getRedisDataZrevrangeWithScores(0, -1, jedis, redis_leaderboard);
    }

    @RequestMapping(value = "/api/list/bottom10", produces = { "text/html; charset=utf-8" })
    @ResponseBody
    public String get10(HttpServletResponse response
    ) {
        return getRedisDataZrangeWithScores(0, 9, jedis, redis_leaderboard);
    }

    @RequestMapping("/api/list/inRank")
    @ResponseBody
    public String getinRank(HttpServletResponse response, @RequestParam(name = "start") int start,
                         @RequestParam(name = "end") int end
    ) {
        return getRedisDataZrevrangeWithScores(start, end, jedis, redis_leaderboard);
    }

    @RequestMapping("/api/list/getBySymbol")
    @ResponseBody
    public String getBySymbol(HttpServletResponse response, @RequestParam(name = "symbols") List<String> symbols

    ) {
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


    @RequestMapping("/api/rank/update")
    @ResponseBody
    public String updateAmount(HttpServletResponse response, @RequestParam(name = "symbol") String symbol,
                               @RequestParam(name = "amount") Long amount

    ) {
        try {
            jedis.zincrby(redis_leaderboard, amount.doubleValue(), symbol);
            return "{success: true}";
        }
        catch (Exception e) {
            return "{success: false}";
        }
    }

    @RequestMapping("/api/rank/reset")
    @ResponseBody
    public String reset(HttpServletResponse response

    ) {
        return resetData(false, jedis, data_ready_redis_key, redis_leaderboard);
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        jedis = new Jedis(properties_uri);
        resetData(Boolean.parseBoolean(jedis.get(data_ready_redis_key)), jedis, data_ready_redis_key, redis_leaderboard);
    }


}
