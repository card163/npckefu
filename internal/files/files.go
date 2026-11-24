// Package files 提供图片附件的存储抽象与上传校验。
//
// 存储实现：DB（MySQL，多实例开箱即用，适合中小规模）与 FS（本地目录；多实例时需要共享卷）。
// 需要对象存储（S3/OSS/COS）时实现 Storage 接口即可，上层不用改。
package files

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"

	"golang.org/x/image/webp"

	"npckefu/internal/store"
)

const (
	MaxBytes     = 5 << 20 // 单张图片上限
	MaxDimension = 10000   // 单边像素上限
	MaxPixels    = 50_000_000
)

var ErrInvalid = errors.New("invalid image")

var idRe = regexp.MustCompile(`^[0-9a-f]{32}$`)

// ValidID 文件 ID 必须是 32 位小写十六进制（同时杜绝路径穿越）。
func ValidID(id string) bool { return idRe.MatchString(id) }

type Storage interface {
	Put(ctx context.Context, id string, data []byte) error
	Get(ctx context.Context, id string) (io.ReadCloser, error) // 不存在返回 store.ErrNotFound
	Delete(ctx context.Context, id string) error
}

// Inspect 校验并识别图片：按**内容**判断类型（不信任客户端声明的文件名/Content-Type），
// 只允许 JPEG/PNG/GIF/WebP（明确排除 SVG 等可含脚本的格式），并限制尺寸以防"解压炸弹"。
// 只读取图片头部（DecodeConfig），不会解码整张图。
func Inspect(data []byte) (mime string, w, h int, err error) {
	if len(data) == 0 || len(data) > MaxBytes {
		return "", 0, 0, fmt.Errorf("%w: size must be 1B..%dMB", ErrInvalid, MaxBytes>>20)
	}
	head := data
	if len(head) > 512 {
		head = head[:512]
	}
	mime = http.DetectContentType(head)
	var cfg image.Config
	switch mime {
	case "image/jpeg", "image/png", "image/gif":
		cfg, _, err = image.DecodeConfig(bytes.NewReader(data))
	case "image/webp":
		cfg, err = webp.DecodeConfig(bytes.NewReader(data))
	default:
		return "", 0, 0, fmt.Errorf("%w: only jpeg/png/gif/webp allowed", ErrInvalid)
	}
	if err != nil {
		return "", 0, 0, fmt.Errorf("%w: corrupt image", ErrInvalid)
	}
	if cfg.Width < 1 || cfg.Height < 1 || cfg.Width > MaxDimension || cfg.Height > MaxDimension || cfg.Width*cfg.Height > MaxPixels {
		return "", 0, 0, fmt.Errorf("%w: dimensions out of range (max %dpx per side)", ErrInvalid, MaxDimension)
	}
	return mime, cfg.Width, cfg.Height, nil
}

// ---- DB 存储 ----

type DB struct{ St *store.Store }

func (d DB) Put(ctx context.Context, id string, data []byte) error {
	return d.St.PutBlob(ctx, id, data)
}
func (d DB) Delete(ctx context.Context, id string) error { return d.St.DeleteBlob(ctx, id) }
func (d DB) Get(ctx context.Context, id string) (io.ReadCloser, error) {
	b, err := d.St.GetBlob(ctx, id)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

// ---- 本地目录存储 ----

type FS struct{ Dir string }

func (f FS) path(id string) (string, error) {
	if !ValidID(id) {
		return "", errors.New("bad file id")
	}
	return filepath.Join(f.Dir, id[:2], id), nil
}

func (f FS) Put(_ context.Context, id string, data []byte) error {
	p, err := f.path(id)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return err
	}
	_, werr := tmp.Write(data)
	cerr := tmp.Close()
	if werr != nil || cerr != nil {
		os.Remove(tmp.Name())
		return errors.Join(werr, cerr)
	}
	if err := os.Rename(tmp.Name(), p); err != nil { // 原子落盘，读者不会看到半截文件
		os.Remove(tmp.Name())
		return err
	}
	return nil
}

func (f FS) Get(_ context.Context, id string) (io.ReadCloser, error) {
	p, err := f.path(id)
	if err != nil {
		return nil, store.ErrNotFound
	}
	fh, err := os.Open(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, store.ErrNotFound
	}
	return fh, err
}

func (f FS) Delete(_ context.Context, id string) error {
	p, err := f.path(id)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
