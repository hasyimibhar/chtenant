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

	rewriteMatch := func(re *regexp.Regexp, match string) string {
		groups := re.FindStringSubmatch(match)
		db := stripBackticks(groups[2])
		table := groups[3]

		if strings.HasPrefix(db, prefix) {
			return match
		}

		return groups[1] + prefix + db + "." + table
	}

	// Pass 1: Rewrite all FROM/JOIN db.table references.
	sql = dbTableRe.ReplaceAllStringFunc(sql, func(m string) string {
		return rewriteMatch(dbTableRe, m)
	})

	// Pass 2: Rewrite ", db.table" comma references within FROM clauses.
	// We find each FROM keyword (at depth 0 relative to parentheses that
	// started after FROM) and rewrite comma-refs until we hit a clause-ending
	// keyword or a JOIN.
	sql = rewriteCommaRefs(sql, prefix, func(m string) string {
		return rewriteMatch(commaTableRe, m)
	})

	return sql
}

// fromKeywordRe finds the FROM keyword (word boundary).
var fromKeywordRe = regexp.MustCompile(`(?i)\bFROM\b`)

// clauseEndRe matches keywords that end a FROM table list.
var clauseEndRe = regexp.MustCompile(
	`(?i)\b(?:WHERE|ORDER|GROUP|HAVING|LIMIT|UNION|JOIN|LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\b`,
)

// rewriteCommaRefs finds FROM clauses and rewrites ", db.table" within them.
func rewriteCommaRefs(sql string, prefix string, rewrite func(string) string) string {
	var result strings.Builder
	pos := 0

	for {
		// Find the next FROM keyword.
		loc := fromKeywordRe.FindStringIndex(sql[pos:])
		if loc == nil {
			result.WriteString(sql[pos:])
			break
		}
		fromStart := pos + loc[1] // right after "FROM"
		result.WriteString(sql[pos:fromStart])

		// Find the end of this FROM's table list: scan forward, tracking
		// parentheses depth. The FROM list ends at depth 0 when we hit a
		// clause-ending keyword, a closing paren, or a semicolon.
		end := findFromClauseEnd(sql, fromStart)
		fromBody := sql[fromStart:end]

		// Rewrite comma-refs within this FROM body.
		fromBody = commaTableRe.ReplaceAllStringFunc(fromBody, rewrite)

		result.WriteString(fromBody)
		pos = end
	}

	return result.String()
}

// findFromClauseEnd returns the index where the FROM table list ends.
func findFromClauseEnd(sql string, start int) int {
	depth := 0
	i := start

	for i < len(sql) {
		ch := sql[i]

		switch ch {
		case '(':
			depth++
			i++
		case ')':
			if depth == 0 {
				return i
			}
			depth--
			i++
		default:
			// At depth 0, check for clause-ending keywords.
			if depth == 0 {
				rest := sql[i:]
				if loc := clauseEndRe.FindStringIndex(rest); loc != nil && loc[0] == 0 {
					return i
				}
			}
			i++
		}
	}

	return len(sql)
}

func stripBackticks(s string) string {
	if len(s) >= 2 && s[0] == '`' && s[len(s)-1] == '`' {
		return s[1 : len(s)-1]
	}
	return s
}
