// Package web holds the dashboard's static UI, embedded into the binary so the
// whole system ships as one Go binary + Redis (no separate frontend to serve).
//
// go:embed can only reference files in this same directory — that's exactly why
// the UI lives here in web/ with its own embed.go, rather than being reached from
// internal/dashboard via "../web".
package web

import "embed"

//go:embed index.html app.js styles.css
var FS embed.FS
