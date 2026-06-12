package filter

import (
	"log/slog"
	"path"
	"strings"
)

type Filter struct {
	rules []filterRuleItem
}

func New(src []string) *Filter {
	items := make([]filterRuleItem, 0, len(src))

	for _, srcItem := range src {
		srcItem = strings.TrimSpace(srcItem)
		if srcItem == "" {
			continue
		}
		method, pattern := "", ""
		parts := strings.SplitN(srcItem, ":", 2)
		switch len(parts) {
		case 1:
			pattern = parts[0]
		case 2:
			method = parts[0]
			pattern = parts[1]
		default:
			slog.Error("invalid filter rule", "rule", srcItem)
			continue
		}
		if pattern == "" {
			slog.Error("empty filter rule", "rule", srcItem)
			continue
		}
		items = append(items, filterRuleItem{
			Method:  strings.ToUpper(method),
			Pattern: strings.ToLower("/" + strings.Trim(pattern, "/")),
		})
	}

	// print parsed rules
	if len(items) > 0 {
		slog.Info("Log-Kafka: Applied filter rules:")
		for _, r := range items {
			slog.Info("  " + r.String())
		}
	} else {
		slog.Info("Log-Kafka: No filter rules applied")
	}

	return &Filter{
		rules: items,
	}
}

func (r *Filter) Check(method, pathStr string) bool {
	if len(r.rules) == 0 {
		return true
	}

	pathStr = strings.ToLower("/" + strings.Trim(pathStr, "/"))
	method = strings.ToUpper(method)

	for _, rule := range r.rules {
		if rule.Method != "" && rule.Method != method {
			continue
		}

		if matched, _ := path.Match(rule.Pattern, pathStr); matched {
			return true
		}
	}

	return false
}

type filterRuleItem struct {
	Method  string // пустой = любой метод
	Pattern string
}

func (r *filterRuleItem) String() string {
	if r.Method != "" {
		return "{" + r.Method + " " + r.Pattern + "}"
	}
	return r.Pattern
}
