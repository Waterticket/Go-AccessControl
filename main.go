package main

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/valyala/fasthttp"
	proxy "github.com/yeqown/fasthttp-reverse-proxy/v2"
)

var (
	pool          proxy.Pool
	err           error
	serverRequest uint32 = 0
	WaitingHTML   []byte
)

// ProxyPoolHandler ...
func ProxyPoolHandler(ctx *fasthttp.RequestCtx) {
	requestURI := string(ctx.RequestURI())
	log.Println("requestURI=", requestURI)

	urlPath := strings.Split(requestURI, "?")[0]
	log.Println("urlPath=", urlPath)

	//if urlPath has dot like file
	if strings.Contains(urlPath, ".") {
		httpProxy(ctx)
		return
	}

	if strings.HasPrefix(requestURI, "/pq-cgi/") {
		switch requestURI {
		case "/pq-cgi/query":
			var token = ctx.Request.Header.Peek("X-ACS-Token")
			if len(token) == 0 {
				token = ctx.Request.Header.Cookie("ACS-Token")
			}

			var lastProcessedIdx = getLastProcessedIdx()
			var currentIdx = getCurrentIdx()

			if len(token) > 0 {
				var req_idx = getIdxFromToken(token)
				var rank int64 = 0
				if req_idx > lastProcessedIdx {
					rank = req_idx - lastProcessedIdx
				}

				var behind = currentIdx - req_idx
				var inbucket = 0

				if rank < (config.Connection.AccessSize)*4 { // may in bucket
					score := zscoreBucket(string(token))
					if score > 0 {
						inbucket = 1
						//zaddBucket(string(token)) // renew token
					}
				}

				ctx.WriteString(fmt.Sprintf(`{"rank":%d,"behind":%d,"inbucket":%d}`, rank, behind, inbucket))
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

		default:
			ctx.SetStatusCode(404)
			ctx.WriteString("404 not found")
			return
		}
	} else {
		var token = ctx.Request.Header.Peek("X-ACS-Token")

		if len(token) == 0 {
			token = ctx.Request.Header.Cookie("ACS-Token")
		}

		if len(token) == 0 {
			// no token
			var reqIdx = getNextIdx()
			var lastProcessedIdx = getLastProcessedIdx()
			if reqIdx < lastProcessedIdx+(config.Connection.AccessSize*2) {
				// 바로 처리 가능
				httpProxy(ctx)
				lastProcessedIdx = getLastProcessedIdx()
				if reqIdx > lastProcessedIdx {
					setLastProcessedIdx(reqIdx)
				}

				var nextReq = popPendingQueue()
				if len(nextReq) > 0 {
					// 다음 요청이 있으면 처리
					zaddBucket(nextReq)
				}
			} else {
				// 처리 불가능 -> 대기
				buf := make([]byte, 22)
				ts := uint32(time.Now().Unix())
				binary.BigEndian.PutUint32(buf[0:], rand.Uint32())        // random
				binary.BigEndian.PutUint32(buf[4:], ts)                   // timestamp
				binary.BigEndian.PutUint16(buf[8:], config.Server.Number) // server number
				binary.BigEndian.PutUint32(buf[10:], serverRequest)       // server request
				binary.BigEndian.PutUint64(buf[14:], uint64(reqIdx))      // request index
				serverRequest++

				var token = base64.RawURLEncoding.EncodeToString(buf)
				pushPendingQueue(token)

				var rank int64 = 0
				if reqIdx > lastProcessedIdx {
					rank = reqIdx - lastProcessedIdx
				}

				// if request is post
				if ctx.IsPost() && string(ctx.Request.Header.Peek("Content-Type")) == "application/json" && string(ctx.Request.Header.Peek("X-ACS-Request")) == "true" {
					ctx.SetStatusCode(253)
					ctx.WriteString(fmt.Sprintf(`{"token":"%s",lastProcessedIdx":%d,"rank":%d,"Idx":%d}`, token, lastProcessedIdx, rank, reqIdx))
					return
				} else {
					// write html
					ctx.SetStatusCode(200)
					ctx.SetContentType("text/html; charset=utf-8")

					//set cookie
					tokenCookie := fasthttp.Cookie{}
					tokenCookie.SetKey("ACS-Token")
					tokenCookie.SetValue(token)
					tokenCookie.SetMaxAge(3600000)
					//tokenCookie.SetDomain(string(ctx.Host()))
					//tokenCookie.SetPath("/")
					//tokenCookie.SetSecure(true)
					ctx.Response.Header.SetCookie(&tokenCookie)

					rankCookie := fasthttp.Cookie{}
					rankCookie.SetKey("ACS-Rank")
					rankCookie.SetValue(fmt.Sprintf("%d", rank))
					rankCookie.SetMaxAge(60)
					//rankCookie.SetDomain(string(ctx.Host()))
					//rankCookie.SetPath("/")
					//rankCookie.SetSecure(true)
					ctx.Response.Header.SetCookie(&rankCookie)
					ctx.Write(WaitingHTML)
					return
				}
			}
		} else {
			// if token
			var score = zscoreBucket(string(token))
			if score > 0 {
				// 바로 처리 가능

				// delete cookie
				tokenCookie := fasthttp.Cookie{}
				tokenCookie.SetKey("ACS-Token")
				tokenCookie.SetValue("")
				tokenCookie.SetMaxAge(-1)
				//tokenCookie.SetDomain(string(ctx.Host()))
				//tokenCookie.SetPath("/")
				//tokenCookie.SetSecure(true)
				ctx.Response.Header.SetCookie(&tokenCookie)

				httpProxy(ctx)
				var reqIdx = getIdxFromToken(token)
				lastProcessedIdx := getLastProcessedIdx()
				if reqIdx > lastProcessedIdx {
					setLastProcessedIdx(reqIdx)
				}
				zremBucket(string(token))

				nextReq := popPendingQueue()
				if len(nextReq) > 0 {
					// 다음 요청이 있으면 처리
					zaddBucket(nextReq)
				}
			} else {
				// delete cookie
				tokenCookie := fasthttp.Cookie{}
				tokenCookie.SetKey("ACS-Token")
				tokenCookie.SetValue("")
				tokenCookie.SetMaxAge(-1)
				//tokenCookie.SetDomain(string(ctx.Host()))
				//tokenCookie.SetPath("/")
				//tokenCookie.SetSecure(true)
				ctx.Response.Header.SetCookie(&tokenCookie)

				if ctx.IsPost() {
					ctx.SetStatusCode(401)
					ctx.WriteString("401 Unauthorized")
				} else {
					ctx.Redirect(string(ctx.RequestURI()), 302)
				}

				return
			}
		}
	}
}

func getIdxFromToken(token []byte) int64 {
	var buf, err = base64.RawURLEncoding.DecodeString(string(token))
	if err != nil {
		return 0
	}

	return int64(binary.BigEndian.Uint64(buf[14:]))
}

func httpProxy(ctx *fasthttp.RequestCtx) {
	proxyServer, err := pool.Get("localhost:9090")
	if err != nil {
		log.Println("ProxyPoolHandler got an error: ", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}
	defer pool.Put(proxyServer)

	proxyServer.ServeHTTP(ctx)
}

func factory(hostAddr string) (*proxy.ReverseProxy, error) {
	p := proxy.NewReverseProxy(hostAddr)
	return p, nil
}

func main() {
	newRedisDB()
	WaitingHTML, _ = os.ReadFile(config.Server.WaitingHTMLFile)

	go func() {
		for {
			serverRequest = 0

			if config.Server.Master {
				lastProcessedIdx := getLastProcessedIdx()
				buckets := zrangebyscoreBucket()
				for _, bucket := range buckets {
					idx := getIdxFromToken([]byte(bucket))
					if idx > lastProcessedIdx {
						lastProcessedIdx = idx
					}
				}
				lastProcessedIdxInRedis := getLastProcessedIdx()
				if lastProcessedIdx > lastProcessedIdxInRedis {
					setLastProcessedIdx(lastProcessedIdx)
				}

				removedCount := zremrangebyscoreBucket()
				log.Println("Removed count: ", removedCount)

				countOfBucket := getCountOfBucket()

				var cnt = countOfBucket
				for (config.Connection.AccessSize)*2 > cnt {
					nextReq := popPendingQueue()
					if len(nextReq) > 0 {
						// 다음 요청이 있으면 처리
						zaddBucket(nextReq)
						cnt++
					} else {
						break
					}
				}
				log.Println("Added count: ", (cnt - countOfBucket))
			}
			time.Sleep(time.Second * 1)
		}
	}()

	initialCap, maxCap := 100, 1000
	pool, err = proxy.NewChanPool(initialCap, maxCap, factory)
	if err := fasthttp.ListenAndServe(":8083", ProxyPoolHandler); err != nil {
		panic(err)
	}
}
