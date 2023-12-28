// Modified version of cli.go from github.com/apex/log
package core

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/apex/log"
	"github.com/fatih/color"
	colorable "github.com/mattn/go-colorable"
)

// Default handler outputting to stderr.
var Log = NewLogger(os.Stderr)

var DebugEnvrion = strings.Split(os.Getenv("DEBUG"), ",")

var bold = color.New(color.Bold)
var grey = color.New(color.FgHiBlack)

// Strings mapping.
var Strings = [...]string{
	log.DebugLevel: "DEBUG",
	log.InfoLevel:  "INFO",
	log.WarnLevel:  "WARNING",
	log.ErrorLevel: "ERROR",
	log.FatalLevel: "FATAL",
}

// Colors mapping.
var Colors = [...]*color.Color{
	log.DebugLevel: color.New(color.FgWhite),
	log.InfoLevel:  color.New(color.FgBlue),
	log.WarnLevel:  color.New(color.FgYellow),
	log.ErrorLevel: color.New(color.FgRed),
	log.FatalLevel: color.New(color.FgRed),
}

type MultiHandler struct {
	handlers []log.Handler
}

func NewMultiHandler(handlers ...log.Handler) *MultiHandler {
	allHandlers := make([]log.Handler, 0, len(handlers))
	for _, h := range handlers {
		if mh, ok := h.(*MultiHandler); ok {
			allHandlers = append(allHandlers, mh.handlers...)
		} else {
			allHandlers = append(allHandlers, h)
		}
	}

	return &MultiHandler{handlers: allHandlers}
}

func (h *MultiHandler) HandleLog(e *log.Entry) error {
	for _, h := range h.handlers {
		if err := h.HandleLog(e); err != nil {
			return err
		}
	}

	return nil
}

// Handler implementation.
type Handler struct {
	mu     sync.Mutex
	Writer io.Writer
}

// New handler.
func NewLogger(w io.Writer) *Handler {
	if f, ok := w.(*os.File); ok {
		return &Handler{
			Writer: colorable.NewColorable(f),
		}
	}

	return &Handler{
		Writer: w,
	}
}

func IsDebugEnabled(modName string, id string) bool {
	for _, name := range DebugEnvrion {
		if name == "*" {
			return true
		}

		s := strings.Split(name, ":")
		switch len(s) {
		case 1:
			// Module name only
			if s[0] == modName {
				return true
			}
		case 2:
			// Module and id
			if s[0] == modName && (s[1] == "*" || id == "*" || s[1] == id) {
				return true
			}
		}
	}
	return false
}

// HandleLog implements log.Handler.
func (h *Handler) HandleLog(e *log.Entry) error {
	if e.Level == log.DebugLevel {
		modName, ok := e.Fields.Get("modName").(string)
		if !ok {
			return nil
		}
		id, ok := e.Fields.Get("id").(string)
		if !ok {
			id = "*"
		}

		if !IsDebugEnabled(modName, id) {
			return nil
		}
	}

	color := Colors[e.Level]
	level := Strings[e.Level]
	name := e.Fields.Get("name")
	t := time.Now()

	h.mu.Lock()
	defer h.mu.Unlock()

	// if _, ok := h.Writer.(*colorable.Writer); ok {
		grey.Fprintf(h.Writer, "[%s] ", t.Format("2006-01-02 01:02:03"))
		color.Fprintf(h.Writer, bold.Sprintf("%*s: ", 1, level))
	// } else {
		// fmt.Fprintf(h.Writer, "[%s] ", t.Format("2006-01-02 01:02:03"))
		// fmt.Fprintf(h.Writer, fmt.Sprintf("%*s: ", 1, level))
	// }

	fmt.Fprintf(h.Writer, "%s: %s", name, e.Message)
	fmt.Fprintln(h.Writer)

	return nil
}
