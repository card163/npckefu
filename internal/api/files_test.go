package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/png"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"npckefu/internal/files"
	"npckefu/internal/im"
	"npckefu/internal/store"
)

func genPNG(t *testing.T, w, h int) []byte {
	var b bytes.Buffer
	if err := png.Encode(&b, image.NewGray(image.Rect(0, 0, w, h))); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func upload(t *testing.T, base string, hdr map[string]string, field, name string, data []byte) (int, map[string]any) {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, _ := mw.CreateFormFile(field, name)
	fw.Write(data)
	mw.Close()
	req, _ := http.NewRequest("POST", base+"/v1/upload", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func accHdr(e *env, a store.Account) map[string]string {
	return map[string]string{"X-App-Key": e.tenant.AppKey, "X-Accid": a.Accid, "X-Token": a.Token}
}

func TestImageUpload(t *testing.T) {
	fsDir := t.TempDir()
	for _, backend := range []string{"db", "fs"} {
		t.Run(backend, func(t *testing.T) {
			e := newEnvWith(t, func(s *im.Service) {
				if backend == "fs" {
					s.Files = files.FS{Dir: fsDir}
				}
			})
			var agent store.Account
			do(t, "POST", e.srv.URL+"/v1/accounts", e.h, store.Account{Accid: "img-agent", Name: "图", Role: "agent"}, &agent)
			v, v2 := e.visitor(t), e.visitor(t)

			// 访客上传
			img := genPNG(t, 40, 30)
			code, up := upload(t, e.srv.URL, accHdr(e, v), "file", "a.png", img)
			if code != 201 || up["width"] != float64(40) || up["height"] != float64(30) || up["mime"] != "image/png" {
				t.Fatalf("upload: %d %v", code, up)
			}
			id, url := up["file_id"].(string), up["url"].(string)

			// 读取：无需认证（持有链接），内容一致，安全响应头齐全，支持 304
			resp, _ := http.Get(e.srv.URL + url)
			got, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 || !bytes.Equal(got, img) || resp.Header.Get("Content-Type") != "image/png" ||
				resp.Header.Get("X-Content-Type-Options") != "nosniff" || !strings.Contains(resp.Header.Get("Content-Security-Policy"), "sandbox") {
				t.Fatalf("serve: %d %v", resp.StatusCode, resp.Header)
			}
			req, _ := http.NewRequest("GET", e.srv.URL+url, nil)
			req.Header.Set("If-None-Match", resp.Header.Get("ETag"))
			if r, _ := http.DefaultClient.Do(req); r.StatusCode != 304 {
				t.Fatalf("etag: %d", r.StatusCode)
			}
			if r, _ := http.Get(e.srv.URL + "/files/" + strings.Repeat("0", 32)); r.StatusCode != 404 {
				t.Fatalf("missing file: %d", r.StatusCode)
			}
			if r, _ := http.Get(e.srv.URL + "/files/..%2f..%2fetc%2fpasswd"); r.StatusCode != 404 {
				t.Fatalf("traversal: %d", r.StatusCode)
			}
			if backend == "fs" {
				if _, err := os.Stat(filepath.Join(fsDir, id[:2], id)); err != nil {
					t.Fatalf("fs file missing: %v", err)
				}
			}

			// 会话：访客转人工并被分配给坐席
			vc := dial(t, e.srv.URL, e.tenant.AppKey, v)
			defer vc.Close()
			vc.WriteJSON(map[string]any{"op": "start"})
			sess := wait(t, vc, func(x im.Event) bool { return x.Op == "session" }).Session
			vc.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "h", "body": "转人工"})
			ac := dial(t, e.srv.URL, e.tenant.AppKey, agent)
			defer ac.Close()
			wait(t, ac, func(x im.Event) bool { return x.Op == "session" && x.Session.Status == "active" })

			// 发送图片消息：服务端把正文重写为规范 JSON，坐席收到 type=image
			vc.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "i1", "type": "image", "body": `{"file_id":"` + id + `","url":"http://evil.example/x.png"}`})
			ack := wait(t, vc, func(x im.Event) bool { return x.Op == "ack" && x.ClientMsgID == "i1" })
			var canon map[string]any
			json.Unmarshal([]byte(ack.Message.Body), &canon)
			if canon["url"] != url || canon["width"] != float64(40) || strings.Contains(ack.Message.Body, "evil") {
				t.Fatalf("canonical body: %s", ack.Message.Body)
			}
			got2 := wait(t, ac, func(x im.Event) bool { return x.Op == "msg" && x.Message.Type == "image" })
			if got2.Message.Body != ack.Message.Body {
				t.Fatalf("agent got %s", got2.Message.Body)
			}
			// 同一文件在同一会话重发（幂等 client_msg_id）不报错
			vc.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "i1", "type": "image", "body": `{"file_id":"` + id + `"}`})
			wait(t, vc, func(x im.Event) bool { return x.Op == "ack" && x.ClientMsgID == "i1" })

			// 滥用：外链、坏 JSON、他人文件、其他会话复用
			for i, body := range []string{`{"url":"http://evil.example/x.png"}`, `not json`, `{"file_id":"` + strings.Repeat("a", 32) + `"}`, `{"file_id":"../../etc/passwd"}`} {
				cid := "bad" + string(rune('0'+i))
				vc.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": cid, "type": "image", "body": body})
				if er := wait(t, vc, func(x im.Event) bool { return x.Op == "error" && x.ClientMsgID == cid }); !strings.Contains(er.Error, "图片") {
					t.Fatalf("bad body %q => %q", body, er.Error)
				}
			}
			_, up2 := upload(t, e.srv.URL, accHdr(e, v2), "file", "b.png", img) // v2 上传的文件
			ac.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "steal", "type": "image", "body": `{"file_id":"` + up2["file_id"].(string) + `"}`})
			wait(t, ac, func(x im.Event) bool { return x.Op == "error" && x.ClientMsgID == "steal" }) // 坐席不能用别人上传的文件
			vc.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "steal2", "type": "image", "body": `{"file_id":"` + up2["file_id"].(string) + `"}`})
			wait(t, vc, func(x im.Event) bool { return x.Op == "error" && x.ClientMsgID == "steal2" })

			// 坐席上传并发送给访客
			_, aup := upload(t, e.srv.URL, accHdr(e, agent), "file", "c.png", genPNG(t, 8, 8))
			ac.WriteJSON(map[string]any{"op": "send", "session_id": sess.ID, "client_msg_id": "ai", "type": "image", "body": `{"file_id":"` + aup["file_id"].(string) + `"}`})
			wait(t, vc, func(x im.Event) bool {
				return x.Op == "msg" && x.Message.Type == "image" && x.Message.From == "img-agent"
			})

			// 租户服务端上传 + 下发
			code, bup := upload(t, e.srv.URL, e.h, "file", "n.png", genPNG(t, 4, 4))
			if code != 201 {
				t.Fatalf("tenant upload: %d", code)
			}
			if c := do(t, "POST", e.srv.URL+"/v1/messages", e.h, map[string]any{"session_id": sess.ID, "type": "image", "body": `{"file_id":"` + bup["file_id"].(string) + `"}`}, nil); c != 200 {
				t.Fatalf("tenant image notice: %d", c)
			}
			wait(t, vc, func(x im.Event) bool {
				return x.Op == "msg" && x.Message.Type == "image" && x.Message.From == "business"
			})
			if c := do(t, "POST", e.srv.URL+"/v1/messages", e.h, map[string]any{"session_id": sess.ID, "type": "image", "body": `{"file_id":"` + id + `"}`}, nil); c != 400 {
				t.Fatalf("tenant using visitor's file should be 400, got %d", c)
			}

			// 未被引用的上传会被清理；已发送的不会
			_, orphan := upload(t, e.srv.URL, accHdr(e, v2), "file", "o.png", img)
			time.Sleep(50 * time.Millisecond) // created_at 需严格早于"现在"
			if n, err := e.svc.GCAttachments(context.Background(), 0); err != nil || n < 1 {
				t.Fatalf("gc: %d %v", n, err)
			}
			if r, _ := http.Get(e.srv.URL + "/files/" + orphan["file_id"].(string)); r.StatusCode != 404 {
				t.Fatalf("orphan should be gone: %d", r.StatusCode)
			}
			if r, _ := http.Get(e.srv.URL + url); r.StatusCode != 200 {
				t.Fatalf("sent image must survive gc: %d", r.StatusCode)
			}
			if backend == "fs" {
				if _, err := os.Stat(filepath.Join(fsDir, orphan["file_id"].(string)[:2], orphan["file_id"].(string))); !os.IsNotExist(err) {
					t.Fatalf("orphan file still on disk: %v", err)
				}
			}
		})
	}
}

func TestImageUploadRejects(t *testing.T) {
	e := newEnv(t)
	v := e.visitor(t)
	h := accHdr(e, v)

	jpegHeaderGarbage := append([]byte{0xFF, 0xD8, 0xFF, 0xE0}, bytes.Repeat([]byte{0x41}, 200)...)
	cases := []struct {
		name  string
		hdr   map[string]string
		field string
		data  []byte
		want  int
	}{
		{"no credentials", map[string]string{"X-App-Key": e.tenant.AppKey}, "file", genPNG(t, 2, 2), 401},
		{"wrong token", map[string]string{"X-App-Key": e.tenant.AppKey, "X-Accid": v.Accid, "X-Token": "nope"}, "file", genPNG(t, 2, 2), 401},
		{"wrong secret", map[string]string{"X-App-Key": e.tenant.AppKey, "X-App-Secret": "nope"}, "file", genPNG(t, 2, 2), 401},
		{"no file field", h, "other", genPNG(t, 2, 2), 400},
		{"text file named png", h, "file", []byte("hello, definitely not an image"), 400},
		{"svg", h, "file", []byte(`<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>`), 400},
		{"html", h, "file", []byte(`<html><script>alert(1)</script></html>`), 400},
		{"jpeg header + garbage", h, "file", jpegHeaderGarbage, 400},
		{"too wide (12000px)", h, "file", genPNG(t, 12000, 1), 400},
		{"too many pixels", h, "file", genPNG(t, 9000, 9000), 400},
		{"over 5MB", h, "file", bytes.Repeat([]byte{0}, files.MaxBytes+10), 413},
		{"empty", h, "file", []byte{}, 400},
	}
	for _, c := range cases {
		if code, out := upload(t, e.srv.URL, c.hdr, c.field, "x.png", c.data); code != c.want {
			t.Errorf("%s: got %d (%v), want %d", c.name, code, out, c.want)
		}
	}
	// 以上失败的上传不应留下任何附件记录（对该访客）
	if n, _ := e.st.CountRecentUploads(context.Background(), e.tenant.ID, v.Accid, time.Hour); n != 0 {
		t.Fatalf("rejected uploads left %d records", n)
	}
	// 功能关闭时 404
	e2 := newEnvWith(t, func(s *im.Service) { s.Files = nil })
	v2 := e2.visitor(t)
	if code, _ := upload(t, e2.srv.URL, accHdr(e2, v2), "file", "a.png", genPNG(t, 2, 2)); code != 404 {
		t.Fatalf("disabled should be 404, got %d", code)
	}
}

func TestImageUploadRateLimit(t *testing.T) {
	e := newEnv(t)
	v := e.visitor(t)
	png1 := genPNG(t, 1, 1)
	for i := 0; i < 30; i++ {
		if code, _ := upload(t, e.srv.URL, accHdr(e, v), "file", "a.png", png1); code != 201 {
			t.Fatalf("upload %d: %d", i, code)
		}
	}
	if code, _ := upload(t, e.srv.URL, accHdr(e, v), "file", "a.png", png1); code != 429 {
		t.Fatalf("31st should be 429, got %d", code)
	}
	// 另一个访客不受影响
	if code, _ := upload(t, e.srv.URL, accHdr(e, e.visitor(t)), "file", "a.png", png1); code != 201 {
		t.Fatalf("other visitor: %d", code)
	}
}
