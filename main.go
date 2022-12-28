package main

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/valyala/fasthttp"
	proxy "github.com/yeqown/fasthttp-reverse-proxy/v2"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
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
		cgiQuery(urlPath, token, ctx)
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
					if !config.Server.UseInLocalhost {
						tokenCookie.SetDomain(string(ctx.Host()))
						tokenCookie.SetPath("/")
						tokenCookie.SetSecure(true)
					}
					ctx.Response.Header.SetCookie(&tokenCookie)

					rankCookie := fasthttp.Cookie{}
					rankCookie.SetKey("ACS-Rank")
					rankCookie.SetValue(fmt.Sprintf("%d", rank))
					rankCookie.SetMaxAge(60)
					if !config.Server.UseInLocalhost {
						rankCookie.SetDomain(string(ctx.Host()))
						rankCookie.SetPath("/")
						rankCookie.SetSecure(true)
					}
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
				if !config.Server.UseInLocalhost {
					tokenCookie.SetDomain(string(ctx.Host()))
					tokenCookie.SetPath("/")
					tokenCookie.SetSecure(true)
				}
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
				if !config.Server.UseInLocalhost {
					tokenCookie.SetDomain(string(ctx.Host()))
					tokenCookie.SetPath("/")
					tokenCookie.SetSecure(true)
				}
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
		go abnormalPackageGC()
	}

	initialCap, maxCap := config.Proxy.PoolConnectionCapacityMin, config.Proxy.PoolConnectionCapacityMax
	pool, err = proxy.NewChanPool(initialCap, maxCap, factory)
	if err := fasthttp.ListenAndServe(config.Server.Addr, ProxyPoolHandler); err != nil {
		panic(err)
	}
}
