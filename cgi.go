package main

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/valyala/fasthttp"
	"log"
	"math/rand"
	"time"
)

func cgiQuery(urlPath string, token []byte, ctx *fasthttp.RequestCtx) {
	switch urlPath {
	case "/pq-cgi/query":
		var lastProcessedIdx = getLastProcessedIdxCache() //getLastProcessedIdx()
		var currentIdx = getCurrentIdxCache()             //getCurrentIdx()
		ctx.SetContentType("application/json")

		if len(token) > 0 {
			var reqIdx = getIdxFromToken(token)
			var rank int64 = 0
			if reqIdx > lastProcessedIdx {
				rank = reqIdx - lastProcessedIdx
			}

			var behind = currentIdx - reqIdx
			var inBucket = -1
			var tokenStr = b2s(token)

			//if rank < (config.Connection.AccessSize)*10 { // may in bucket
			if true {
				score := zscoreBucket(tokenStr)
				if score > 0 {
					inBucket = 1
				} else {
					inBucket = 0
				}
			}

			log.Println("[query] reqIdx=", reqIdx, "lastProcessedIdx=", lastProcessedIdx, "currentIdx=", currentIdx, "rank=", rank, "behind=", behind, "inBucket=", inBucket)

			ctx.WriteString(fmt.Sprintf(`{"rank":%d,"behind":%d,"inbucket":%d,"idx":%d}`, rank, behind, inBucket, reqIdx))
			go func() {
				// heartbeat to redis
				setHeartbeat(tokenStr)
				log.Println("[heartbeat] reqIdx=", reqIdx, "token=", tokenStr)
			}()
		} else {
			ctx.WriteString(fmt.Sprintf(`{"lastProcessedIdx":%d,"currentIdx":%d}`, lastProcessedIdx, currentIdx))
		}
		return

	case "/pq-cgi/status":
		var countOfBucket = getCountOfBucket()
		var countOfPendingQueue = getCountOfPendingQueue()
		var lastProcessedIdx = getLastProcessedIdx()
		var currentIdx = getCurrentIdx()
		ctx.WriteString(fmt.Sprintf("countOfBucket=%d, countOfPendingQueue=%d, lastProcessedIdx=%d, currentIdx=%d", countOfBucket, countOfPendingQueue, lastProcessedIdx, currentIdx))
		return

	case "/pq-cgi/forceclear":
		var currentIdx = getCurrentIdx()
		setLastProcessedIdx(currentIdx)
		delBucket()
		delPendingQueue()

		ctx.WriteString("DONE")
		return

	case "/pq-cgi/generatetoken":
		var reqIdx = getNextIdx()
		buf := make([]byte, 22)
		ts := uint32(time.Now().Unix())
		binary.BigEndian.PutUint32(buf[0:], rand.Uint32())          // random
		binary.BigEndian.PutUint32(buf[4:], ts)                     // timestamp
		binary.BigEndian.PutUint16(buf[8:], config.Server.Sequence) // server number
		binary.BigEndian.PutUint32(buf[10:], serverRequest)         // server request
		binary.BigEndian.PutUint64(buf[14:], uint64(reqIdx))        // request index
		var token = base64.RawURLEncoding.EncodeToString(buf)
		serverRequest++
		ctx.WriteString(token)
		return

	default:
		ctx.SetStatusCode(404)
		ctx.WriteString("404 not found")
		return
	}
}
