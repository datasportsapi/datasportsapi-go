package datasportsapi

// PageOption configures a …Pages iterator.
type PageOption func(*pageOptions)

type pageOptions struct{ maxPages int }

// MaxPages stops a …Pages iterator after n pages (n <= 0: no limit).
func MaxPages(n int) PageOption {
	return func(o *pageOptions) { o.maxPages = n }
}

func pageLimit(opts []PageOption) int {
	var o pageOptions
	for _, f := range opts {
		if f != nil {
			f(&o)
		}
	}
	return o.maxPages
}

// pageMeta is what a pager reads from an answer's meta.
type pageMeta struct {
	hasNextPage *bool
	page        *int64
	nextCursor  *string
}

type nextPage struct {
	cursor string
	page   int64
}

// next decides the following request (sdk/notes.md → Paging): a non-empty
// nextCursor on an operation that takes a cursor wins; else hasNextPage moves to
// page (meta.page ?? the page asked for ?? 0) + 1; else the walk ends.
func (m pageMeta) next(asked *int64, takesCursor bool) (nextPage, bool) {
	if takesCursor && m.nextCursor != nil && *m.nextCursor != "" {
		return nextPage{cursor: *m.nextCursor}, true
	}
	if m.hasNextPage == nil || !*m.hasNextPage {
		return nextPage{}, false
	}
	var cur int64
	switch {
	case m.page != nil:
		cur = *m.page
	case asked != nil:
		cur = *asked
	}
	return nextPage{page: cur + 1}, true
}
