package rewriter

import (
	"fmt"
	"regexp"
	"strings"
)

const Separator = "__"

// Rewrite rewrites database-qualified table references in a SELECT query,
// prefixing each database name with tenantID + separator.
//
// Example: "SELECT * FROM foo.bar" with tenant "t1" becomes "SELECT * FROM t1__foo.bar"
func Rewrite(sql string, tenantID string) (string, error) {
	if err := validateSelect(sql); err != nil {
		return "", err
	}

	segments := tokenize(sql)
	var result strings.Builder
	for _, seg := range segments {
		if seg.isString {
			result.WriteString(seg.text)
		} else {
			result.WriteString(rewriteTableRefs(seg.text, tenantID))
		}
	}
	return result.String(), nil
}

// RewriteDatabase prefixes a database name with the tenant ID.
func RewriteDatabase(database string, tenantID string) string {
	if database == "" {
		database = "default"
	}
	return tenantID + Separator + database
}

func validateSelect(sql string) error {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)

	if strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "WITH") ||
		strings.HasPrefix(upper, "EXPLAIN") ||
		strings.HasPrefix(upper, "SHOW") ||
		strings.HasPrefix(upper, "DESCRIBE") ||
		strings.HasPrefix(upper, "DESC") ||
		strings.HasPrefix(upper, "EXISTS") {
		return nil
	}
	return fmt.Errorf("only SELECT queries are allowed, got: %.40s", trimmed)
}

// segment represents either a string literal or non-string SQL text.
type segment struct {
	text     string
	isString bool
}

// tokenize splits SQL into string literal segments and non-string segments.
// This ensures we never rewrite content inside string literals.
func tokenize(sql string) []segment {
	var segments []segment
	var current strings.Builder
	inString := false
	var stringChar byte
	i := 0

	for i < len(sql) {
		ch := sql[i]

		if inString {
			current.WriteByte(ch)
			if ch == stringChar {
				// Check for escaped quote (doubled quote)
				if i+1 < len(sql) && sql[i+1] == stringChar {
					current.WriteByte(sql[i+1])
					i += 2
					continue
				}
				segments = append(segments, segment{text: current.String(), isString: true})
				current.Reset()
				inString = false
				i++
				continue
			}
			i++
			continue
		}

		// Check for string literal start
		if ch == '\'' {
			if current.Len() > 0 {
				segments = append(segments, segment{text: current.String(), isString: false})
				current.Reset()
			}
			current.WriteByte(ch)
			inString = true
			stringChar = ch
			i++
			continue
		}

		// Check for line comments
		if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			end := strings.Index(sql[i:], "\n")
			if end == -1 {
				current.WriteString(sql[i:])
				i = len(sql)
			} else {
				current.WriteString(sql[i : i+end+1])
				i = i + end + 1
			}
			continue
		}

		// Check for block comments
		if ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			end := strings.Index(sql[i+2:], "*/")
			if end == -1 {
				current.WriteString(sql[i:])
				i = len(sql)
			} else {
				current.WriteString(sql[i : i+2+end+2])
				i = i + 2 + end + 2
			}
			continue
		}

		current.WriteByte(ch)
		i++
	}

	if current.Len() > 0 {
		segments = append(segments, segment{text: current.String(), isString: inString})
	}

	return segments
}

// ident matches an unquoted identifier or a backtick-quoted identifier.
var ident = `(?:` + "`" + `[^` + "`" + `]+` + "`" + `|\w+)`

// dbTableRe matches FROM/JOIN followed by database.table reference.
// Captures: group 1 = prefix (FROM/JOIN + whitespace), group 2 = database, group 3 = dot+table
var dbTableRe = regexp.MustCompile(
	`(?i)(\b(?:FROM|JOIN)\s+)(` + ident + `)\.(` + ident + `)`,
)

// commaTableRe matches comma-separated table references that follow
// a FROM clause: ", database.table"
var commaTableRe = regexp.MustCompile(
	`(,\s*)(` + ident + `)\.(` + ident + `)`,
)

func rewriteTableRefs(sql string, tenantID string) string {
	prefix := tenantID + Separator

	// Rewrite FROM/JOIN db.table
	sql = dbTableRe.ReplaceAllStringFunc(sql, func(match string) string {
		groups := dbTableRe.FindStringSubmatch(match)
		// groups[1] = "FROM " or "JOIN ", groups[2] = db, groups[3] = table
		db := stripBackticks(groups[2])
		table := groups[3]

		// Don't rewrite already-prefixed databases
		if strings.HasPrefix(db, prefix) {
			return match
		}

		return groups[1] + prefix + db + "." + table
	})

	return sql
}

func stripBackticks(s string) string {
	if len(s) >= 2 && s[0] == '`' && s[len(s)-1] == '`' {
		return s[1 : len(s)-1]
	}
	return s
}
