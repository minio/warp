package iceberg

import (
	"fmt"
)

type TreeConfig struct {
	NamespaceWidth   int
	NamespaceDepth   int
	TablesPerNS      int
	ViewsPerNS       int
	ColumnsPerTable  int
	ColumnsPerView   int
	PropertiesPerNS  int
	PropertiesPerTbl int
	PropertiesPerVw  int
	BaseLocation     string
	CatalogName      string
}

type Tree struct {
	cfg TreeConfig
}

func NewTree(cfg TreeConfig) *Tree {
	return &Tree{cfg: cfg}
}

func (t *Tree) TotalNamespaces() int {
	if t.cfg.NamespaceWidth == 1 {
		return t.cfg.NamespaceDepth
	}
	n := 1
	for i := 0; i < t.cfg.NamespaceDepth; i++ {
		n *= t.cfg.NamespaceWidth
	}
	return (n - 1) / (t.cfg.NamespaceWidth - 1)
}

func (t *Tree) LeafNamespaces() int {
	n := 1
	for i := 0; i < t.cfg.NamespaceDepth-1; i++ {
		n *= t.cfg.NamespaceWidth
	}
	return n
}

func (t *Tree) TotalTables() int {
	return t.LeafNamespaces() * t.cfg.TablesPerNS
}

func (t *Tree) TotalViews() int {
	return t.LeafNamespaces() * t.cfg.ViewsPerNS
}

func (t *Tree) DepthOf(ordinal int) int {
	if t.cfg.NamespaceWidth == 1 {
		return ordinal
	}
	depth := 0
	size := 1
	total := 0
	for total+size <= ordinal {
		total += size
		size *= t.cfg.NamespaceWidth
		depth++
	}
	return depth
}

func (t *Tree) PathToRoot(ordinal int) []string {
	// Build hierarchical path from root to this node
	var path []int
	current := ordinal
	for current >= 0 {
		path = append([]int{current}, path...)
		current = t.parentOf(current)
	}

	result := make([]string, len(path))
	for i, ord := range path {
		result[i] = t.namespaceName(ord)
	}
	return result
}

func (t *Tree) parentOf(ordinal int) int {
	if ordinal == 0 {
		return -1
	}
	if t.cfg.NamespaceWidth == 1 {
		return ordinal - 1
	}
	return (ordinal - 1) / t.cfg.NamespaceWidth
}

func (t *Tree) ChildrenOf(ordinal int) []int {
	if t.DepthOf(ordinal) >= t.cfg.NamespaceDepth-1 {
		return nil
	}

	if t.cfg.NamespaceWidth == 1 {
		return []int{ordinal + 1}
	}

	first := ordinal*t.cfg.NamespaceWidth + 1
	children := make([]int, t.cfg.NamespaceWidth)
	for i := 0; i < t.cfg.NamespaceWidth; i++ {
		children[i] = first + i
	}
	return children
}

func (t *Tree) IsLeaf(ordinal int) bool {
	return t.DepthOf(ordinal) == t.cfg.NamespaceDepth-1
}

func (t *Tree) LeafOrdinals() []int {
	total := t.TotalNamespaces()
	leafCount := t.LeafNamespaces()
	start := total - leafCount

	ordinals := make([]int, leafCount)
	for i := 0; i < leafCount; i++ {
		ordinals[i] = start + i
	}
	return ordinals
}

func (t *Tree) namespaceName(ordinal int) string {
	// MinIO S3 Tables doesn't allow underscores in namespace names
	return fmt.Sprintf("ns%d", ordinal)
}

func (t *Tree) TableName(index int) string {
	return fmt.Sprintf("t%d", index)
}

func (t *Tree) ViewName(index int) string {
	return fmt.Sprintf("v%d", index)
}

func (t *Tree) TableLocation(namespace []string, tableName string) string {
	path := t.cfg.BaseLocation + "/" + t.cfg.CatalogName
	for _, ns := range namespace {
		path += "/" + ns
	}
	return path + "/" + tableName
}

func (t *Tree) Config() TreeConfig {
	return t.cfg
}

type NamespaceInfo struct {
	Ordinal   int
	Path      []string
	IsLeaf    bool
	TableIdxs []int
	ViewIdxs  []int
}

func (t *Tree) AllNamespaces() []NamespaceInfo {
	total := t.TotalNamespaces()
	result := make([]NamespaceInfo, total)

	tableIdx := 0
	viewIdx := 0

	for i := 0; i < total; i++ {
		info := NamespaceInfo{
			Ordinal: i,
			Path:    t.PathToRoot(i),
			IsLeaf:  t.IsLeaf(i),
		}

		if info.IsLeaf {
			info.TableIdxs = make([]int, t.cfg.TablesPerNS)
			for j := 0; j < t.cfg.TablesPerNS; j++ {
				info.TableIdxs[j] = tableIdx
				tableIdx++
			}
			info.ViewIdxs = make([]int, t.cfg.ViewsPerNS)
			for j := 0; j < t.cfg.ViewsPerNS; j++ {
				info.ViewIdxs[j] = viewIdx
				viewIdx++
			}
		}

		result[i] = info
	}

	return result
}

type TableInfo struct {
	Index     int
	Name      string
	Namespace []string
	Location  string
}

func (t *Tree) AllTables() []TableInfo {
	var tables []TableInfo
	tableIdx := 0

	for _, ns := range t.AllNamespaces() {
		if !ns.IsLeaf {
			continue
		}
		for i := 0; i < t.cfg.TablesPerNS; i++ {
			name := t.TableName(tableIdx)
			tables = append(tables, TableInfo{
				Index:     tableIdx,
				Name:      name,
				Namespace: ns.Path,
				Location:  t.TableLocation(ns.Path, name),
			})
			tableIdx++
		}
	}

	return tables
}

type ViewInfo struct {
	Index     int
	Name      string
	Namespace []string
	Location  string
}

func (t *Tree) AllViews() []ViewInfo {
	var views []ViewInfo
	viewIdx := 0

	for _, ns := range t.AllNamespaces() {
		if !ns.IsLeaf {
			continue
		}
		for i := 0; i < t.cfg.ViewsPerNS; i++ {
			name := t.ViewName(viewIdx)
			views = append(views, ViewInfo{
				Index:     viewIdx,
				Name:      name,
				Namespace: ns.Path,
				Location:  t.TableLocation(ns.Path, name),
			})
			viewIdx++
		}
	}

	return views
}
