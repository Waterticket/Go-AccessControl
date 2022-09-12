package main

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"time"
	"unsafe"

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
	requestURI := b2s(ctx.RequestURI())
	//log.Println("requestURI=", requestURI)

	urlPath := strings.Split(requestURI, "?")[0]
	//log.Println("urlPath=", urlPath)

	//if urlPath has dot like file
	if strings.Contains(urlPath, ".") {
		httpProxy(ctx)
		return
	}

	var token = ctx.Request.Header.Peek("X-ACS-Token")
	if len(token) == 0 {
		token = ctx.Request.Header.Cookie("ACS-Token")
	}

	if strings.HasPrefix(requestURI, "/pq-cgi/") {
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
	} else {
		if len(token) == 0 {
			// no token
			var reqIdx = getNextIdx()
			var lastProcessedIdx = getLastProcessedIdx()
			if reqIdx < lastProcessedIdx+(config.Connection.AccessSize*2) {
				// 바로 처리 가능
				httpProxy(ctx)
				log.Println("[direct][notoken] reqIdx=", reqIdx, "lastProcessedIdx=", lastProcessedIdx)
				go func() {
					lastProcessedIdx = getLastProcessedIdx()
					if reqIdx > lastProcessedIdx {
						setLastProcessedIdx(reqIdx)
					}

					var nextReq = popPendingQueue()
					if len(nextReq) > 0 {
						// 다음 요청이 있으면 처리
						zaddBucket(nextReq)
						log.Println("[next][notoken_direct] reqIdx=", reqIdx, "lastProcessedIdx=", lastProcessedIdx, "nextReq=", nextReq)
					}
				}()
			} else {
				// 처리 불가능 -> 대기
				buf := make([]byte, 22)
				ts := uint32(time.Now().Unix())
				binary.BigEndian.PutUint32(buf[0:], rand.Uint32())          // random
				binary.BigEndian.PutUint32(buf[4:], ts)                     // timestamp
				binary.BigEndian.PutUint16(buf[8:], config.Server.Sequence) // server number
				binary.BigEndian.PutUint32(buf[10:], serverRequest)         // server request
				binary.BigEndian.PutUint64(buf[14:], uint64(reqIdx))        // request index
				serverRequest++

				var token = base64.RawURLEncoding.EncodeToString(buf)
				pushPendingQueue(token)
				setHeartbeat(token)

				var rank int64 = 0
				if reqIdx > lastProcessedIdx {
					rank = reqIdx - lastProcessedIdx
				}

				log.Println("[wait][notoken] reqIdx=", reqIdx, "lastProcessedIdx=", lastProcessedIdx, "rank=", rank, "token=", token)

				// if request is post
				if ctx.IsPost() && string(ctx.Request.Header.Peek("X-ACS-Request")) == "true" {
					ctx.SetStatusCode(253)
					ctx.SetContentType("application/json")
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
			tokenStr := b2s(token)
			var score = zscoreBucket(tokenStr)
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
				var reqIdx = getIdxFromToken(token) // for debug
				log.Println("[direct][token] reqIdx=", reqIdx, "token=", tokenStr, "score=", score)
				go func() {
					//var reqIdx = getIdxFromToken(token)
					lastProcessedIdx := getLastProcessedIdx()
					if reqIdx > lastProcessedIdx {
						setLastProcessedIdx(reqIdx)
					}
					zremBucket(tokenStr)

					nextReq := popPendingQueue()
					if len(nextReq) > 0 {
						// 다음 요청이 있으면 처리
						zaddBucket(nextReq)
						log.Println("[next][token_direct] reqIdx=", reqIdx, "lastProcessedIdx=", lastProcessedIdx, "nextReq=", nextReq)
					}

					delHeartbeat(tokenStr)
				}()
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

				var reqIdx = getIdxFromToken(token) // for debug
				log.Println("[invaild][token] reqIdx=", reqIdx, " token=", tokenStr)

				if ctx.IsPost() {
					ctx.SetStatusCode(401)
					ctx.WriteString("401 Unauthorized")
				} else {
					ctx.Redirect(b2s(ctx.RequestURI()), 302)
				}

				return
			}
		}
	}
}

func getIdxFromToken(token []byte) int64 {
	//decoded := make([]byte, base64.StdEncoding.DecodedLen(len(token)))
	//var _, err = base64.RawURLEncoding.Decode(decoded, token)
	decoded, err := base64.RawURLEncoding.DecodeString(b2s(token))
	if err != nil {
		return -1
	}
	if len(decoded) != 22 {
		return -1
	}

	return int64(binary.BigEndian.Uint64(decoded[14:22]))
}

func httpProxy(ctx *fasthttp.RequestCtx) {
	proxyServer, err := pool.Get(config.Proxy.Addr)
	if err != nil {
		log.Println("ProxyPoolHandler got an error: ", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}
	defer pool.Put(proxyServer)

	proxyServer.ServeHTTP(ctx)
}

func factory(hostAddr string) (*proxy.ReverseProxy, error) {
	p := proxy.NewReverseProxy(hostAddr, proxy.WithTimeout(time.Duration(config.Proxy.TimeoutMilliseconds)*time.Millisecond))
	return p, nil
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func s2b(s string) (b []byte) {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh.Data = sh.Data
	bh.Cap = sh.Len
	bh.Len = sh.Len
	return b
}

func main() {
	newRedisDB()
	WaitingHTML, _ = os.ReadFile(config.Server.WaitingHTMLFile)

	logFile, err := os.Create("log.txt")
	if err != nil {
		fmt.Println(err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	go func() {
		for {
			serverRequest = 0
			getCurrentIdx() // CurrentIdx 초기화
			getLastProcessedIdx()
			time.Sleep(time.Second * 1)
		}
	}()

	if config.Server.Master {
		go func() {
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
								setLastProcessedIdx(deletedIdx)
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
		}()
	}

	initialCap, maxCap := config.Proxy.PoolConnectionCapacityMin, config.Proxy.PoolConnectionCapacityMax
	pool, err = proxy.NewChanPool(initialCap, maxCap, factory)
	if err := fasthttp.ListenAndServe(config.Server.Addr, ProxyPoolHandler); err != nil {
		panic(err)
	}
}
