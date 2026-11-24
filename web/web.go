// Package web 内嵌访客聊天窗口与坐席工作台（原生 HTML/JS，无需构建）。
package web

import "embed"

//go:embed *.html
var FS embed.FS
