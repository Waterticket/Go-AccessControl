package main

import (
	"log"
	"time"
)

func abnormalPackageGC() {
	for {
		// Garbage Collection

		lastProcessedIdx := getLastProcessedIdx()
		buckets := zrangebyscoreBucket()
		for _, bucket := range buckets {
			idx := getIdxFromToken(s2b(bucket))
			if idx > lastProcessedIdx {
				lastProcessedIdx = idx
			}

			log.Println("[gc][remove] reqIdx=", idx)
		}
		lastProcessedIdxInRedis := getLastProcessedIdx()
		//if lastProcessedIdx > lastProcessedIdxInRedis {
		setLastProcessedIdx(lastProcessedIdx)
		//}
		log.Println("[gc][lastProcessedIdx] lastProcessedIdx=", lastProcessedIdx, "lastProcessedIdxInRedis=", lastProcessedIdxInRedis)

		removedCount := zremrangebyscoreBucket()
		log.Println("[gc] RemovedCount=", removedCount)

		countOfBucket := getCountOfBucket()
		log.Println("[gc] CountOfBucket=", countOfBucket)

		var cnt = countOfBucket
		var deletedIdx int64 = 0
		for (config.Connection.AccessSize)*2 > cnt {
			nextReq := popPendingQueue()
			if len(nextReq) > 0 {
				heartbeat := getHeartbeat(nextReq)
				deletedIdx = getIdxFromToken(s2b(nextReq))
				log.Println("[gc][pop] nextReq=", nextReq, "reqIdx=", deletedIdx)
				if heartbeat < 0 {
					//deletedIdx = getIdxFromToken(s2b(nextReq))
					log.Println("[gc][pop][delete] nextReq=", nextReq, "reqIdx=", deletedIdx)
					go func() {
						// Heartbeat delete for prevent Memory Leak
						delHeartbeat(nextReq)
						setLastProcessedIdx(deletedIdx) // 무효키가 많으면 유용하나, 그 외에는 그닥?
					}()
					continue
				}

				zaddBucket(nextReq)
				log.Println("[gc][pop][add] nextReq=", nextReq, "reqIdx=", deletedIdx)
				cnt++
			} else {
				break
			}
		}

		if deletedIdx > 0 {
			setLastProcessedIdx(deletedIdx)
		}

		log.Println("[gc] AddedCount: ", cnt-countOfBucket)

		time.Sleep(time.Millisecond * 100)
	}
}
