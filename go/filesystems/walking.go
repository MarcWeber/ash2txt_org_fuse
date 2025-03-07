package filesystems

import (
	"fmt"
	"strings"
	"sync"
)

func keys(m interface{}) []string {
	switch v := m.(type) {
	case map[string]*LazyFolder:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		return keys
	case map[string]*LazyFile:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		return keys
	default:
		return nil
	}
}

func WalkPath(folder *LazyFolder, path string) (interface{}, error) {
	ps := strings.Split(strings.Trim(path, "/"), "/")
	if len(ps) == 1 && ps[0] == "" {
		ps = []string{}
	}
	current := folder
	for _, p := range ps {
		folders, files, err := current.FoldersAndFiles()
		if err != nil {
			return nil, err
		}
		if next, ok := folders[p]; ok {
			current = next
		} else if file, ok := files[p]; ok {
			return file, nil
		} else {
			return nil, fmt.Errorf("%s not found", p)
		}
	}
	return current, nil
}

func WalkPathFindFolder(folder *LazyFolder, path string) (*LazyFolder, error) {
	node, err := WalkPath(folder, path)
	if err != nil {
		return nil, err
	}
	if f, ok := node.(*LazyFolder); ok {
		return f, nil
	}
	return nil, fmt.Errorf("%s is not a folder", path)
}

func formatSizeBytes(b int64) string { return fmt.Sprintf("%d bytes", b) }
func formatSizeMiB(b int64) string  { return fmt.Sprintf("%.2f MiB", float64(b)/1024/1024) }

func specialFolder(files map[string]*LazyFile) string {
	tifCount := 0
	for name := range files {
		if strings.HasSuffix(name, ".tif") {
			tifCount++
		}
		if name == ".zarray" {
			return "zarr archive"
		}
	}
	if tifCount > 20 {
		return "tiff archive"
	}
	return ""
}

func ListAndSizeApproximateFastParallel(folder *LazyFolder, indent string) (int64, error) {
	folders, files, err := folder.FoldersAndFiles()
	if err != nil {
		return 0, err
	}
	sizes := make(chan int64, len(files)+len(folders))
	var wg sync.WaitGroup

	for _, file := range files {
		wg.Add(1)
		go func(f *LazyFile) {
			defer wg.Done()
			size, _ := f.SizeBytesApproximate()
			sizes <- size
		}(file)
	}
	for _, subFolder := range folders {
		wg.Add(1)
		go func(f *LazyFolder) {
			defer wg.Done()
			size, _ := ListAndSizeApproximateFastParallel(f, indent+"    ")
			sizes <- size
		}(subFolder)
	}

	go func() {
		wg.Wait()
		close(sizes)
	}()

	total := int64(0)
	for size := range sizes {
		total += size
	}
	fmt.Printf("folder %s%s %s\n", indent, folder.path, formatSizeMiB(total))
	return total, nil
}

func Prefetch(folder *LazyFolder) error {
	folders, files, err := folder.FoldersAndFiles()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for _, file := range files {
		wg.Add(1)
		go func(f *LazyFile) {
			defer wg.Done()
			f.EnsureFetched()
		}(file)
	}
	for _, subFolder := range folders {
		wg.Add(1)
		go func(f *LazyFolder) {
			defer wg.Done()
			Prefetch(f)
		}(subFolder)
	}
	wg.Wait()
	return nil
}

func ListImportant(folder *LazyFolder, indent string) error {
	folders, files, err := folder.FoldersAndFiles()
	if err != nil {
		return err
	}
	fmt.Printf("%s%s\n", indent, folder.path)
	special := specialFolder(files)
	if special == "" {
		for _, subFolder := range folders {
			ListImportant(subFolder, indent+"    ")
		}
		for name, file := range files {
			size, _ := file.SizeBytesApproximate()
			fmt.Printf("%s    %s %s %s\n", indent, name, formatSizeBytes(size), formatSizeMiB(size))
		}
	} else {
		fmt.Printf("%s    probably %s\n", indent, special)
	}
	return nil
}
