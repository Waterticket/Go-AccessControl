package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v9"
)

var (
	rdb                *redis.Client
	ctx                      = context.Background()
	__LastProcessedIdx int64 = 0
	__CurrentIdx       int64 = 0
)

func newRedisDB() {
	if rdb == nil {
		rdb = redis.NewClient(&redis.Options{
			Addr:     config.Redis.Addr,
			Password: config.Redis.Password,
			DB:       config.Redis.DB,
		})
	}

	// test connection
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal(err)
	}

	// return pong if server is online
	log.Println(pong)

	setFirstValue()
}

func setFirstValue() {
	if rdb.Exists(ctx, "accesscontrol:current_idx").Val() == 0 {
		err := rdb.Set(ctx, "accesscontrol:current_idx", 0, 0).Err()
		if err != nil {
			log.Fatal(err)
		}
	}

	if rdb.Exists(ctx, "accesscontrol:last_processed_idx").Val() == 0 {
		err := rdb.Set(ctx, "accesscontrol:last_processed_idx", 0, 0).Err()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func getNextIdx() int64 {
	// get next index
	idx, err := rdb.Incr(ctx, "accesscontrol:current_idx").Result()
	if err != nil {
		log.Fatal(err)
	}

	return idx
}

func getCurrentIdx() int64 {
	// get current index
	idx, err := rdb.Get(ctx, "accesscontrol:current_idx").Int64()
	if err != nil {
		if err != redis.Nil {
			log.Fatal(err)
		} else {
			return __CurrentIdx
		}
	}

	__CurrentIdx = idx

	return idx
}

func getCurrentIdxCache() int64 {
	return __CurrentIdx
}

func getLastProcessedIdx() int64 {
	// get last processed index
	idx, err := rdb.Get(ctx, "accesscontrol:last_processed_idx").Int64()
	if err != nil {
		if err != redis.Nil {
			log.Fatal(err)
		} else {
			return __LastProcessedIdx
		}
	}

	if idx > __LastProcessedIdx {
		__LastProcessedIdx = idx
	}

	return idx
}

func getLastProcessedIdxCache() int64 {
	return __LastProcessedIdx
}

func setLastProcessedIdx(idx int64) {
	if idx > __LastProcessedIdx {
		__LastProcessedIdx = idx
	}
	err := rdb.Set(ctx, "accesscontrol:last_processed_idx", idx, 0).Err()
	if err != nil {
		log.Fatal(err)
	}
}

func popPendingQueue() string {
	// pop pending queue
	token, err := rdb.RPop(ctx, "accesscontrol:pending_queue").Result()
	if err != nil {
		if err != redis.Nil {
			log.Fatal(err)
		}
	}

	return token
}

func pushPendingQueue(token string) {
	// push pending queue
	err := rdb.LPush(ctx, "accesscontrol:pending_queue", token).Err()
	if err != nil {
		log.Fatal(err)
	}
}

func zaddBucket(token string) {
	// add to bucket
	var timestamp = time.Now().UnixMilli()
	err := rdb.ZAdd(ctx, "accesscontrol:bucket", redis.Z{
		Score:  float64(timestamp + config.Connection.BucketTTLMilliseconds),
		Member: token,
	}).Err()
	if err != nil {
		log.Fatal(err)
	}
}

func zscoreBucket(token string) int64 {
	// get score from bucket
	score, err := rdb.ZScore(ctx, "accesscontrol:bucket", token).Result()
	if err != nil {
		if err != redis.Nil {
			log.Fatal(err)
		}
	}

	return int64(score)
}

func zremBucket(token string) {
	// remove from bucket
	err := rdb.ZRem(ctx, "accesscontrol:bucket", token).Err()
	if err != nil {
		log.Fatal(err)
	}
}

func zrangebyscoreBucket() []string {
	// get range of bucket
	tokens, err := rdb.ZRangeByScore(ctx, "accesscontrol:bucket", &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", time.Now().UnixMilli()),
	}).Result()
	if err != nil {
		if err != redis.Nil {
			log.Fatal(err)
		}
	}

	return tokens
}

func zremrangebyscoreBucket() int64 {
	// remove from bucket
	count, err := rdb.ZRemRangeByScore(ctx, "accesscontrol:bucket", "-inf", fmt.Sprintf("%d", time.Now().UnixMilli())).Result()
	if err != nil {
		if err != redis.Nil {
			log.Fatal(err)
		}
	}

	return count
}

func delBucket() {
	// delete bucket
	err := rdb.Del(ctx, "accesscontrol:bucket").Err()
	if err != nil {
		log.Fatal(err)
	}
}

func delPendingQueue() {
	// delete pending queue
	err := rdb.Del(ctx, "accesscontrol:pending_queue").Err()
	if err != nil {
		log.Fatal(err)
	}
}

func getCountOfBucket() int64 {
	// get count of bucket
	count, err := rdb.ZCard(ctx, "accesscontrol:bucket").Result()
	if err != nil {
		if err != redis.Nil {
			log.Fatal(err)
		}
	}

	return count
}

func getCountOfPendingQueue() int64 {
	// get count of pending queue
	count, err := rdb.LLen(ctx, "accesscontrol:pending_queue").Result()
	if err != nil {
		if err != redis.Nil {
			log.Fatal(err)
		}
	}

	return count
}

func setHeartbeat(token string) {
	// set heartbeat
	err := rdb.Set(ctx, "accesscontrol:heartbeat:"+token, time.Now().UnixMilli(), time.Duration(config.Client.HeartbeatRedisTTLMilliseconds)*time.Millisecond).Err()
	if err != nil {
		log.Fatal(err)
	}
}

func getHeartbeat(token string) int64 {
	// get heartbeat
	heartbeat, err := rdb.Get(ctx, "accesscontrol:heartbeat:"+token).Int64()
	if err != nil {
		if err != redis.Nil {
			log.Fatal(err)
		} else {
			return -1
		}
	}

	return heartbeat
}

func delHeartbeat(token string) {
	// delete heartbeat
	err := rdb.Del(ctx, "accesscontrol:heartbeat:"+token).Err()
	if err != nil {
		log.Fatal(err)
	}
}
