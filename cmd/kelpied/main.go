// Command kelpied 是 Kelpie 的常驻守护进程：内嵌 eD2k 引擎并提供
// HTTP 管理 API + 静态页面，单二进制运行在路由器上。
//
// 用法:
//   kelpied -data /overlay/kelpie/data \
//           -out  /overlay/kelpie/out \
//           -www  /overlay/kelpie/www \
//           -listen :8082 \
//           -port 4662 \
//           -version v0.2.0
//
// HTTP API:
//   GET  /api/status                     全量状态 JSON
//   POST /api/tasks  {"link":"ed2k://..","name":"a.iso"}   添加并立即开始
//   POST /api/tasks/{id}/stop            停止（保留断点，可 resume）
//   POST /api/tasks/{id}/resume          继续（重新 run，Durable State 续传）
//   POST /api/tasks/{id}/remove          删除任务 + Durable State（不删文件）
//   DELETE /api/tasks/{id}?file=1        同上，file=1 时同时删除文件
//   GET  /                               静态页面（www 目录）
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/XiaoYouChR/Kelpie/internal/engine"
	"github.com/XiaoYouChR/Kelpie/internal/gateway"
)

var version = "dev"

// ---------- 任务表 ----------

type taskState string

const (
	stateRunning taskState = "running"
	stateStopped taskState = "stopped"
	stateDone    taskState = "done"
	stateFailed  taskState = "failed"
	statePending taskState = "pending"
)

type task struct {
	ID       engine.RunID     `json:"id"`
	Link     string           `json:"link"`
	Name     string           `json:"name"`
	File     string           `json:"file"`
	Hash     string           `json:"hash,omitempty"`
	Size     int64            `json:"size,omitempty"`
	Received int64            `json:"received,omitempty"`
	Pct      int              `json:"pct,omitempty"`
	Rate     int64            `json:"rate,omitempty"`
	Peers    int              `json:"peers,omitempty"`
	Active   int              `json:"active,omitempty"`
	State    taskState        `json:"state"`
	Error    string           `json:"error,omitempty"`
}

// ---------- 引擎事件收集器 ----------

// collector 实现 engine.Events，把引擎回调汇总到 daemon 状态。
type collector struct {
	mu       sync.Mutex
	tasks    map[engine.RunID]*task
	next     engine.RunID
	network  engine.Network
	engineOn bool
	stopped  map[engine.RunID]bool // 主动 stop 的 run：ended 时候判别"完成"与"被停"
}

func newCollector() *collector {
	return &collector{tasks: map[engine.RunID]*task{}, stopped: map[engine.RunID]bool{}}
}

func (c *collector) SetProgress(id engine.RunID, p engine.Progress) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.tasks[id]
	if !ok {
		return
	}
	t.Hash = p.Hash.String()
	t.Size = p.Size
	t.Received = p.Received
	t.Rate = p.DownloadRate
	t.Peers = p.Peers
	t.Active = p.ActivePeers
	if p.Size > 0 {
		t.Pct = int(p.Received * 100 / p.Size)
	}
	if t.State == statePending {
		t.State = stateRunning
	}
}

func (c *collector) SendEnded(id engine.RunID, err *engine.Error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.tasks[id]
	if !ok {
		return
	}
	if err == nil && !c.stopped[id] {
		t.State = stateDone
		t.Error = ""
	} else if err == nil {
		t.State = stateStopped
		t.Error = ""
	} else {
		t.State = stateFailed
		t.Error = err.Message
	}
	t.Rate = 0
	t.Active = 0
}

// updateProgress 直接从 stdout 消息更新任务进度（hash 以字符串形式传来）
func (c *collector) updateProgress(id engine.RunID, hash string, size, received, rate int64, peers, active int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.tasks[id]
	if !ok {
		return
	}
	t.Hash = hash
	t.Size = size
	t.Received = received
	t.Rate = rate
	t.Peers = peers
	t.Active = active
	if size > 0 {
		t.Pct = int(received * 100 / size)
	}
	if t.State == statePending {
		t.State = stateRunning
	}
}

func (c *collector) SetNetwork(n engine.Network) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.network = n
}

// ---------- 引擎 stdio 桥接 ----------

// bridge 把 daemon 的命令写入引擎 stdin，并异步读取引擎 stdout。
type bridge struct {
	in   io.WriteCloser
	mu   sync.Mutex // 保护 in 的并发写
	done chan struct{}
}

func newBridge(in io.WriteCloser) *bridge {
	return &bridge{in: in, done: make(chan struct{})}
}

func (b *bridge) send(v any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = b.in.Write(append(data, '\n'))
	return err
}

// ---------- 主线程 ----------

type daemon struct {
	col    *collector
	br     *bridge
	cfg    config
	stopFn func() // 关闭引擎 stdin，触发引擎保存状态并退出
}

type config struct {
	dataFolder string
	outDir     string
	wwwDir     string
	listen     string
	port       int
	dlLimit    int64
	ulLimit    int64
	proxy      string // socks5:// 或 socks5h://，空则直连（ADR-0006）
}

type statusJSON struct {
	Engine  bool           `json:"engine"`
	Updated int64          `json:"updated"`
	Network map[string]any `json:"network"`
	Tasks   []*task        `json:"tasks"`
}

func (d *daemon) status() statusJSON {
	d.col.mu.Lock()
	defer d.col.mu.Unlock()
	list := make([]*task, 0, len(d.col.tasks))
	for _, t := range d.col.tasks {
		cp := *t
		if cp.State == stateRunning {
			// 页面用：running 时保留实时进度
		}
		list = append(list, &cp)
	}
	n := d.col.network
	return statusJSON{
		Engine:  d.col.engineOn,
		Updated: time.Now().Unix(),
		Network: map[string]any{
			"server":       n.IsServerConnected,
			"highId":       n.IsHighID,
			"kadFirewalled": n.IsKadFirewalled,
			"kadNodes":     n.KadNodes,
			"carrierNat":   n.IsBehindCarrierNat,
		},
		Tasks: list,
	}
}

// add 登记任务并立即发 run
func (d *daemon) add(link, name string) (*task, error) {
	if name == "" {
		name = nameFromLink(link)
		if name == "" {
			return nil, fmt.Errorf("无法从链接解析文件名，请显式指定 name")
		}
	}
	d.col.mu.Lock()
	d.col.next++
	id := d.col.next
	t := &task{
		ID:    id,
		Link:  link,
		Name:  name,
		File:  filepath.Join(d.cfg.outDir, name),
		State: statePending,
	}
	d.col.tasks[id] = t
	d.col.mu.Unlock()

	if err := d.br.send(map[string]any{
		"type": "run",
		"run":  id,
		"mode": "download",
		"link": link,
		"file": t.File,
	}); err != nil {
		return nil, fmt.Errorf("发 run 失败: %w", err)
	}
	log.Printf("任务 #%d 已启动: %s", id, name)
	return t, nil
}

func (d *daemon) stop(id engine.RunID) error {
	if err := d.br.send(map[string]any{"type": "stop", "run": id}); err != nil {
		return err
	}
	d.col.mu.Lock()
	if t, ok := d.col.tasks[id]; ok && t.State == stateRunning {
		t.State = stateStopped
	}
	d.col.stopped[id] = true
	d.col.mu.Unlock()
	return nil
}

func (d *daemon) resume(id engine.RunID) error {
	d.col.mu.Lock()
	t, ok := d.col.tasks[id]
	if !ok {
		d.col.mu.Unlock()
		return fmt.Errorf("任务 #%d 不存在", id)
	}
	if t.State == stateRunning {
		d.col.mu.Unlock()
		return fmt.Errorf("任务 #%d 正在下载，无需 resume", id)
	}
	// 必须换新 RunID：outbox 对已 ended 的 run 永久忽略后续事件（isEnded 不清除），
	// 复用旧 id 会让 resume 后的 progress/ended 全被吞，任务永远显示 pending
	d.col.next++
	newID := d.col.next
	delete(d.col.tasks, id)
	delete(d.col.stopped, id)
	t.ID = newID
	t.State = statePending
	d.col.tasks[newID] = t
	d.col.mu.Unlock()
	if err := d.br.send(map[string]any{
		"type": "run",
		"run":  newID,
		"mode": "download",
		"link": t.Link,
		"file": t.File,
	}); err != nil {
		return err
	}
	log.Printf("任务 #%d 已以新 ID #%d 恢复: %s", id, newID, t.Name)
	return nil
}

// remove 删除任务的 Durable State（引擎侧），可选同时删除文件
func (d *daemon) remove(id engine.RunID, delFile bool) error {
	d.col.mu.Lock()
	t, ok := d.col.tasks[id]
	if ok {
		delete(d.col.tasks, id)
	}
	d.col.mu.Unlock()
	if !ok {
		return fmt.Errorf("任务 #%d 不存在", id)
	}
	// hash 优先取引擎上报的；还没收到 progress 时从 ed2k 链接解析，
	// 否则引擎侧会留下孤儿 run 继续下载
	hash := t.Hash
	if hash == "" {
		if parts := strings.Split(t.Link, "|"); len(parts) >= 6 {
			hash = parts[4]
		}
	}
	if hash != "" {
		_ = d.br.send(map[string]any{"type": "remove", "hash": hash})
	}
	if delFile && t.File != "" {
		if err := os.Remove(t.File); err != nil && !os.IsNotExist(err) {
			log.Printf("删除文件失败 %s: %v", t.File, err)
		}
	}
	return nil
}

// ---------- HTTP ----------

func (d *daemon) routes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, d.status())
	})

	mux.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Link string `json:"link"`
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			// 兼容表单提交
			body.Link = r.FormValue("link")
			body.Name = r.FormValue("name")
		}
		if body.Link == "" {
			writeJSON(w, map[string]any{"ok": false, "error": "link 为空"})
			return
		}
		t, err := d.add(body.Link, body.Name)
		if err != nil {
			writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"ok": true, "id": t.ID, "name": t.Name})
	})

	mux.HandleFunc("/api/tasks/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
		parts := strings.Split(rest, "/")
		id64, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			writeJSON(w, map[string]any{"ok": false, "error": "id 非法"})
			return
		}
		id := engine.RunID(id64)
		action := ""
		if len(parts) > 1 {
			action = parts[1]
		}

		switch {
		case r.Method == http.MethodDelete:
			err = d.remove(id, r.URL.Query().Get("file") == "1")
		case action == "stop":
			err = d.stop(id)
		case action == "resume":
			err = d.resume(id)
		case action == "remove":
			err = d.remove(id, r.URL.Query().Get("file") == "1")
		default:
			writeJSON(w, map[string]any{"ok": false, "error": "未知动作"})
			return
		}
		if err != nil {
			writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"ok": true, "id": id64})
	})

	// 静态页面
	mux.Handle("/", http.FileServer(http.Dir(d.cfg.wwwDir)))
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}

// ---------- 链接解析 ----------

// nameFromLink 从 ed2k://|file|NAME|SIZE|HASH|/ 提取 NAME
func nameFromLink(link string) string {
	parts := strings.Split(link, "|")
	if len(parts) >= 4 && parts[0] == "ed2k:" && strings.HasPrefix(parts[1], "//") || strings.HasPrefix(link, "ed2k://|file|") {
		if len(parts) >= 3 {
			return parts[2]
		}
	}
	// 宽松解析：按 | 切，第 3 段是文件名
	if i := strings.Index(link, "|file|"); i >= 0 {
		rest := link[i+len("|file|"):]
		if j := strings.Index(rest, "|"); j > 0 {
			return rest[:j]
		}
	}
	return ""
}

// ---------- 启动 ----------

func main() {
	var cfg config
	flag.StringVar(&cfg.dataFolder, "data", "/overlay/kelpie/data", "Durable State 目录")
	flag.StringVar(&cfg.outDir, "out", "/overlay/kelpie/out", "下载输出目录")
	flag.StringVar(&cfg.wwwDir, "www", "/overlay/kelpie/www", "静态页面目录")
	flag.StringVar(&cfg.listen, "listen", ":8082", "HTTP 监听地址")
	flag.IntVar(&cfg.port, "port", 4662, "eD2k TCP/UDP 端口")
	flag.Int64Var(&cfg.dlLimit, "dl", 0, "下载限速 B/s，0 不限")
	flag.Int64Var(&cfg.ulLimit, "ul", 0, "上传限速 B/s，0 不限")
	flag.StringVar(&cfg.proxy, "proxy", "", "socks5 代理 URL（如 socks5://127.0.0.1:1070），引擎全部流量走此代理，绕开路由器透明代理的不确定性")
	flag.Parse()

	for _, dir := range []string{cfg.dataFolder, cfg.outDir, cfg.wwwDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatalf("建目录失败 %s: %v", dir, err)
		}
	}

	// 引擎 stdin/stdout 桥接：engineIn 由我们写入命令，engineOut 由 gateway 写事件
	// gateway.Run 会一直读 in 直到 EOF，并在 out 上输出事件。
	engineInR, engineInW := io.Pipe()   // gateway 读（我们写）
	engineOutR, engineOutW := io.Pipe() // gateway 写（我们读）

	col := newCollector()
	br := newBridge(engineInW)

	// 启动 gateway（阻塞，放 goroutine）
	gwDone := make(chan error, 1)
	go func() {
		gwDone <- gateway.Run(engineInR, engineOutW, version, func(c engine.Config, ev engine.Events) (gateway.Engine, error) {
			c.LocalAddrs = probeLocalAddrs()
			e, err := engine.Start(c, ev)
			if err != nil {
				return nil, err
			}
			return e, nil
		})
		// gateway 退出（含引擎启动失败）后必须关两端：
		// engineOutW 不关 => 主流程 ReadString 永久阻塞（读死锁）
		// engineInR 不关 => 之后任何 br.send 永久阻塞（写死锁，曾致僵尸进程）
		engineOutW.Close()
		engineInR.Close()
	}()

	// 端口探测：被占则快速失败（否则引擎 START_FAILED 后主流程死锁）
	if ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.port)); err != nil {
		log.Fatalf("eD2k 端口 %d 被占用（旧实例还活着？）: %v", cfg.port, err)
	} else {
		ln.Close()
	}

	// 发 hello
	hello := map[string]any{
		"type":       "hello",
		"protocol":   1,
		"dataFolder": cfg.dataFolder,
		"settings": map[string]any{
			"port":        cfg.port,
			"enableKad":   true,
			"enableUpnp":  false,
			"serverLists": []string{filepath.Join(cfg.dataFolder, "server.met")},
			"nodeLists":   []string{filepath.Join(cfg.dataFolder, "nodes.dat")},
			"traceFile":   "",
			"proxy":       cfg.proxy,
		},
		"rateLimits": map[string]any{
			"download": cfg.dlLimit,
			"upload":   cfg.ulLimit,
		},
	}
	if err := br.send(hello); err != nil {
		log.Fatalf("发 hello 失败: %v", err)
	}

	// 读引擎 stdout，等待 ready
	reader := bufio.NewReader(engineOutR)
	deadline := time.Now().Add(30 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("读引擎输出: %v", err)
			break
		}
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		switch m["type"] {
		case "ready":
			ready = true
			col.mu.Lock()
			col.engineOn = true
			col.mu.Unlock()
			log.Printf("引擎就绪 version=%v", m["version"])
		case "failed":
			log.Printf("引擎启动失败: %v", m["error"])
		}
		if ready {
			break
		}
	}
	if !ready {
		// 引擎启动失败（如端口冲突）后 gateway.Run 已返回，
		// 管道不会再有输出，继续等会死锁 —— 必须立即退出
		log.Fatal("引擎 30 秒内未就绪，退出（详见上方日志）")
	}

	// 持续读引擎事件：gateway 把引擎事件序列化到 stdout（不直接调 collector，
	// collector 是作为 outbox 的 holder 传给引擎的，事件实际走 stdout 管道）。
	// 这里解析 progress / network / ended 并更新任务表。
	go func() {
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					log.Printf("读引擎事件结束: %v", err)
				}
				col.mu.Lock()
				col.engineOn = false
				// 引擎没了，未终结任务不会再有事件 —— 标 failed 防永久 running
				for _, t := range col.tasks {
					if t.State == stateRunning || t.State == statePending {
						t.State = stateFailed
						t.Error = "引擎已退出"
						t.Rate = 0
						t.Active = 0
					}
				}
				col.mu.Unlock()
				return
			}
			var m struct {
				Type string `json:"type"`
				Run  uint64 `json:"run"`
				// progress 字段
				Hash         string `json:"hash"`
				Size         int64  `json:"size"`
				Received     int64  `json:"received"`
				DownloadRate int64  `json:"downloadRate"`
				Peers        int    `json:"peers"`
				ActivePeers  int    `json:"activePeers"`
				// network 字段
				IsServerConnected  bool   `json:"isServerConnected"`
				IsHighID           bool   `json:"isHighId"`
				IsKadFirewalled    bool   `json:"isKadFirewalled"`
				KadNodes           int    `json:"kadNodes"`
				IsBehindCarrierNat bool   `json:"isBehindCarrierNat"`
				ProxyIssue         string `json:"proxyIssue"`
				// ended 字段
				Error *struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if json.Unmarshal([]byte(line), &m) != nil {
				continue
			}
			switch m.Type {
			case "progress":
				col.updateProgress(engine.RunID(m.Run), m.Hash, m.Size, m.Received, m.DownloadRate, m.Peers, m.ActivePeers)
			case "network":
				col.SetNetwork(engine.Network{
					IsServerConnected:  m.IsServerConnected,
					IsHighID:           m.IsHighID,
					IsKadFirewalled:    m.IsKadFirewalled,
					KadNodes:           m.KadNodes,
					IsBehindCarrierNat: m.IsBehindCarrierNat,
					ProxyIssue:         m.ProxyIssue,
				})
			case "ended":
				var e *engine.Error
				if m.Error != nil {
					e = &engine.Error{Code: engine.CodeStartFailed, Message: m.Error.Message}
				}
				col.SendEnded(engine.RunID(m.Run), e)
			}
		}
	}()

	d := &daemon{col: col, br: br, cfg: cfg, stopFn: func() { _ = engineInW.Close() }}

	log.Printf("kelpied 监听 %s (data=%s out=%s www=%s)",
		cfg.listen, cfg.dataFolder, cfg.outDir, cfg.wwwDir)
	srv := &http.Server{Addr: cfg.listen, Handler: d.routes()}
	if err := srv.ListenAndServe(); err != nil {
		log.Printf("HTTP 退出: %v", err)
	}
	// HTTP 退出后，关 stdin 让引擎保存状态
	d.stopFn()
	<-gwDone
}

// probeLocalAddrs 直接采用 cmd/kelpie/main.go 中的实现（已验证）。
func probeLocalAddrs() []netip.Addr {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Printf("kelpied: interface addresses: %v", err)
	}
	var local []netip.Addr
	for _, a := range addrs {
		if prefix, ok := a.(*net.IPNet); ok {
			if ip, ok := netip.AddrFromSlice(prefix.IP); ok {
				local = append(local, ip.Unmap())
			}
		}
	}
	return local
}
