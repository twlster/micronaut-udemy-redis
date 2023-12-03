package edu.udemy.micronautredis.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.LocalTime;

@Controller("/time")
@AllArgsConstructor
@ExecuteOn(TaskExecutors.IO)
public class ReteLimitTimeoutEndpoint {

    private static final int QUOTA_PER_MINUTE = 10;
    private static final Logger LOG = LoggerFactory.getLogger(ReteLimitTimeoutEndpoint.class);

    private StatefulRedisConnection<String, String> redisConnection;

    @Get()
    public String time(){
        final String key = "MICRONAUTREDIS::TIME";
        return getTime(key, LocalTime.now());
    }

    @Get("/utc")
    public String utc(){
        final String key = "MICRONAUTREDIS::UTC";
        return getTime(key, LocalTime.now(Clock.systemUTC()));
    }

    private String getTime(String key, LocalTime now) {
        final String value = redisConnection.sync().get(key);
        final int currentQuota = value != null ? Integer.parseInt(value) : 0;

        if (currentQuota >= QUOTA_PER_MINUTE) {
            final String err = String.format("Rate limit reached %s %s/%s", key, currentQuota, QUOTA_PER_MINUTE);
            LOG.info(err);
            return err;
        }

        LOG.info("The current quota {} in {}/{}", key, currentQuota, QUOTA_PER_MINUTE);

        increaseCurrentQuota(key);

        return now.toString();
    }

    private void increaseCurrentQuota(String key){

        final RedisCommands<String, String> commands = redisConnection.sync();

        commands.multi();
        commands.incrby(key, 1);
        commands.expire(key, 60 - LocalTime.now().getSecond());
        commands.exec();
    }

}
