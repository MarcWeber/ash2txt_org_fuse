cat > main.go << 'EOF'
package main

import (
    "flag"
    "fmt"
    "os"
    "go_fs_project/filesystems"
)

func main() {
    fetchLimit := flag.Int("fetch-limit", 40, "Maximum number of concurrent fetch operations")
    flag.Parse()

    if len(flag.Args()) < 3 {
        fmt.Println("Usage: ./go_fs_project [--fetch-limit N] <cacheDir> <rootURL> <command> [path]")
        fmt.Println("Commands: list, list_recursive, du_approximate, prefetch-meta, prefetch-files")
        os.Exit(1)
    }

    cacheDir := flag.Arg(0)
    rootURL := flag.Arg(1)
    command := flag.Arg(2)
    path := ""
    if len(flag.Args()) > 3 {
        path = flag.Arg(3)
    }

    // Initialize global semaphore with the specified limit
    filesystems.GlobalSemaphore = make(chan struct{}, *fetchLimit)
    fmt.Printf("Global fetch limit set to %d\n", *fetchLimit)

    rootFolder := filesystems.NewLazyFolder("", cacheDir, rootURL)

    switch command {
    case "list":
        result, err := rootFolder.List(path)
        if err != nil {
            fmt.Printf("Error listing %s: %v\n", path, err)
            os.Exit(1)
        }
        fmt.Println(result)
    case "list_recursive":
        if err := rootFolder.ListRecursive(path); err != nil {
            fmt.Printf("Error listing recursively %s: %v\n", path, err)
            os.Exit(1)
        }
    case "du_approximate":
        if err := rootFolder.DuApproximate(path); err != nil {
            fmt.Printf("Error calculating approximate size for %s: %v\n", path, err)
            os.Exit(1)
        }
    case "prefetch-meta":
        if err := rootFolder.PrefetchMeta(path); err != nil {
            fmt.Printf("Error prefetching metadata for %s: %v\n", path, err)
            os.Exit(1)
        }
        fmt.Printf("Metadata prefetched for %s\n", path)
    case "prefetch-files":
        if err := rootFolder.PrefetchFiles(path); err != nil {
            fmt.Printf("Error prefetching files for %s: %v\n", path, err)
            os.Exit(1)
        }
        fmt.Printf("Files and metadata prefetched for %s\n", path)
    default:
        fmt.Printf("Unknown command: %s\n", command)
        fmt.Println("Commands: list, list_recursive, du_approximate, prefetch-meta, prefetch-files")
        os.Exit(1)
    }

    rootFolder.Stop()
}
EOF

cat > filesystems/ash2txtorg.go << 'EOF'
package filesystems

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "path/filepath"
    "strconv"
    "strings"
    "sync"
    "time"

    "github.com/PuerkitoBio/goquery"
)

type CachedFileData struct {
    Size           *int64 `json:"size"`
    SizeApproximate int64  `json:"size_approximate"`
}

type CachedFolderData struct {
    Files   map[string]CachedFileData `json:"files"`
    Folders []string                  `json:"folders"`
}

type FetchResultFile struct {
    Size string
    Date string
}

type FetchResultFolder struct {
    Folders []string
    Files   map[string]FetchResultFile
}

type LazyFile struct {
    name   string
    parent *LazyFolder
}

type LazyFolder struct {
    path       string
    cacheDir   string
    rootURL    string
    client     *http.Client
    semaphore  chan struct{}
    fetchOnce  map[string]chan struct{}
    fetchMutex sync.Mutex
    cache      *CachedFolderData
    cacheMutex sync.RWMutex
    wg         *sync.WaitGroup
    fs         *FS
    flusher    *Flusher
}

type Flusher struct {
    folders    map[*LazyFolder]time.Time
    mutex      sync.RWMutex
    stopChan   chan struct{}
    flushChan  chan *LazyFolder
    wg         sync.WaitGroup
}

// Global cache for folder data
var (
    folderCache      = make(map[string]*CachedFolderData)
    folderCacheMutex sync.RWMutex
)

// Global fetch concurrency tracking and limiting
var (
    activeFetches    int
    activeFetchesMux sync.Mutex
    GlobalSemaphore  chan struct{} // Initialized in main.go, exported
)

var globalFlusher *Flusher

func init() {
    globalFlusher = NewFlusher()
    go globalFlusher.Run()
}

func NewFlusher() *Flusher {
    f := &Flusher{
        folders:   make(map[*LazyFolder]time.Time),
        stopChan:  make(chan struct{}),
        flushChan: make(chan *LazyFolder, 100),
    }
    return f
}

func (f *Flusher) Run() {
    ticker := time.NewTicker(15 * time.Second)
    defer ticker.Stop()
    f.wg.Add(1)
    defer f.wg.Done()
    for {
        select {
        case <-ticker.C:
            f.flushExpired()
        case folder := <-f.flushChan:
            if folder.allSizesSet() {
                f.flushFolder(folder)
            }
        case <-f.stopChan:
            f.flushAll()
            return
        }
    }
}

func (f *Flusher) Subscribe(folder *LazyFolder) {
    f.mutex.Lock()
    f.folders[folder] = time.Now()
    f.mutex.Unlock()
    f.flushChan <- folder
}

func (f *Flusher) flushExpired() {
    var toFlush []*LazyFolder
    f.mutex.RLock()
    now := time.Now()
    for folder, lastChange := range f.folders {
        if now.Sub(lastChange) >= 15*time.Second {
            toFlush = append(toFlush, folder)
        }
    }
    f.mutex.RUnlock()

    for _, folder := range toFlush {
        f.flushFolder(folder)
    }
}

func (f *Flusher) flushFolder(folder *LazyFolder) {
    if err := folder.saveCache(); err != nil {
        fmt.Printf("Failed to flush cache for %s: %v\n", folder.path, err)
    }
    f.mutex.Lock()
    delete(f.folders, folder)
    f.mutex.Unlock()
}

func (f *Flusher) flushAll() {
    var folders []*LazyFolder
    f.mutex.RLock()
    for folder := range f.folders {
        folders = append(folders, folder)
    }
    f.mutex.RUnlock()

    for _, folder := range folders {
        f.flushFolder(folder)
    }
}

func (f *Flusher) Stop() {
    close(f.stopChan)
    f.wg.Wait()
}

func ExactSizeBytesFromStr(size string) *int64 {
    if strings.HasSuffix(size, " B") {
        if n, err := strconv.ParseInt(size[:len(size)-2], 10, 64); err == nil {
            return &n
        }
    }
    return nil
}

func ApproximateSizeBytesFromStr(size string) int64 {
    parts := strings.Split(size, " ")
    if len(parts) != 2 {
        return 999999
    }
    n, _ := strconv.ParseFloat(parts[0], 64)
    switch parts[1] {
    case "B":
        return int64(n)
    case "KiB":
        return int64(n * 1024)
    case "MiB":
        return int64(n * 1024 * 1024)
    case "GiB":
        return int64(n * 1024 * 1024 * 1024)
    default:
        return 999999
    }
}

func ParseDirectoryHTML(html string) (*FetchResultFolder, error) {
    doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
    if err != nil {
        return nil, err
    }

    folders := []string{}
    files := make(map[string]FetchResultFile)

    doc.Find("#list tbody tr").Each(func(i int, s *goquery.Selection) {
        cols := s.Find("td")
        if cols.Length() == 3 {
            nameNode := cols.Eq(0).Find("a")
            nameTitle := cols.Eq(0).Text()
            href, _ := nameNode.Attr("href")
            name, _ := url.QueryUnescape(href)
            parts := strings.Split(name, "/")
            isDir := strings.HasSuffix(name, "/")
            var entry string
            if isDir {
                entry = parts[len(parts)-2]
            } else {
                entry = parts[len(parts)-1]
            }
            if nameTitle == "Parent directory/" {
                return
            }
            size := cols.Eq(1).Text()
            date := cols.Eq(2).Text()
            if isDir {
                folders = append(folders, entry)
            } else {
                files[entry] = FetchResultFile{Size: size, Date: date}
            }
        }
    })
    return &FetchResultFolder{Folders: folders, Files: files}, nil
}

func (f *LazyFile) SizeBytesApproximate() (int64, error) {
    return f.parent.FileSizeBytesApproximate(f.name)
}

func (f *LazyFile) SizeBytesExact() (int64, error) {
    return f.parent.FileSizeBytesExact(f.name)
}

func (f *LazyFile) EnsureFetched() error {
    return f.parent.FileEnsureFetched(f.name)
}

func (f *LazyFile) CachePath() (string, error) {
    return f.parent.FileCachePath(f.name)
}

func NewLazyFolder(path, cacheDir, rootURL string) *LazyFolder {
    var wg sync.WaitGroup
    f := &LazyFolder{
        path:      path,
        cacheDir:  cacheDir,
        rootURL:   rootURL,
        client:    &http.Client{Timeout: 0},
        semaphore: make(chan struct{}, 80),
        fetchOnce: make(map[string]chan struct{}),
        wg:        &wg,
        fs:        nil,
        flusher:   globalFlusher,
    }
    return f
}

func (f *LazyFolder) buildURL(parts ...string) string {
    var cleaned []string
    for _, p := range parts {
        cleaned = append(cleaned, strings.Trim(p, "/"))
    }
    return strings.Join(cleaned, "/")
}

func (f *LazyFolder) fetchText(url string) (string, error) {
    activeFetchesMux.Lock()
    activeFetches++
    fmt.Printf("Starting fetchText: %s (active fetches: %d)\n", url, activeFetches)
    activeFetchesMux.Unlock()

    GlobalSemaphore <- struct{}{}
    f.semaphore <- struct{}{}
    defer func() {
        <-f.semaphore
        <-GlobalSemaphore
        activeFetchesMux.Lock()
        activeFetches--
        fmt.Printf("Finished fetchText: %s (active fetches: %d)\n", url, activeFetches)
        activeFetchesMux.Unlock()
    }()

    resp, err := f.client.Get(url)
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()
    body, err := io.ReadAll(resp.Body)
    return string(body), err
}

func (f *LazyFolder) fetchBytes(url, dest string) error {
    activeFetchesMux.Lock()
    activeFetches++
    fmt.Printf("Starting fetchBytes: %s to %s (active fetches: %d)\n", url, dest, activeFetches)
    activeFetchesMux.Unlock()

    GlobalSemaphore <- struct{}{}
    f.semaphore <- struct{}{}
    defer func() {
        <-f.semaphore
        <-GlobalSemaphore
        activeFetchesMux.Lock()
        activeFetches--
        fmt.Printf("Finished fetchBytes: %s to %s (active fetches: %d)\n", url, dest, activeFetches)
        activeFetchesMux.Unlock()
    }()

    resp, err := f.client.Get(url)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    tmp := dest + ".tmp"
    out, err := os.Create(tmp)
    if err != nil {
        return err
    }
    defer out.Close()
    _, err = io.Copy(out, resp.Body)
    if err != nil {
        os.Remove(tmp)
        return err
    }
    return os.Rename(tmp, dest)
}

func (f *LazyFolder) fetchHeaders(url string) (int64, error) {
    activeFetchesMux.Lock()
    activeFetches++
    fmt.Printf("Starting fetchHeaders: %s (active fetches: %d)\n", url, activeFetches)
    activeFetchesMux.Unlock()

    GlobalSemaphore <- struct{}{}
    f.semaphore <- struct{}{}
    defer func() {
        <-f.semaphore
        <-GlobalSemaphore
        activeFetchesMux.Lock()
        activeFetches--
        fmt.Printf("Finished fetchHeaders: %s (active fetches: %d)\n", url, activeFetches)
        activeFetchesMux.Unlock()
    }()

    resp, err := f.client.Head(url)
    if err != nil {
        return 0, err
    }
    defer resp.Body.Close()
    length, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
    return length, nil
}

func (f *LazyFolder) folderFetch(folder string) (*CachedFolderData, error) {
    folderCacheMutex.RLock()
    if cached, ok := folderCache[folder]; ok {
        folderCacheMutex.RUnlock()
        return cached, nil
    }
    folderCacheMutex.RUnlock()

    cachePath := filepath.Join(f.cacheDir, folder)
    os.MkdirAll(cachePath, 0755)
    cacheFile := filepath.Join(cachePath, ".directory_contents_cached_v2.json")
    if _, err := os.Stat(cacheFile); err == nil {
        data, err := os.ReadFile(cacheFile)
        if err == nil {
            var cached CachedFolderData
            if err := json.Unmarshal(data, &cached); err == nil {
                folderCacheMutex.Lock()
                folderCache[folder] = &cached
                folderCacheMutex.Unlock()
                return &cached, nil
            }
        }
    }

    url := f.buildURL(f.rootURL, folder)
    html, err := f.fetchText(url)
    if err != nil {
        return nil, err
    }
    parsed, err := ParseDirectoryHTML(html)
    if err != nil {
        return nil, err
    }
    cached := &CachedFolderData{
        Files:   make(map[string]CachedFileData),
        Folders: parsed.Folders,
    }
    for k, v := range parsed.Files {
        cached.Files[k] = CachedFileData{
            Size:           ExactSizeBytesFromStr(v.Size),
            SizeApproximate: ApproximateSizeBytesFromStr(v.Size),
        }
    }
    data, _ := json.Marshal(cached)
    os.WriteFile(cacheFile+".tmp", data, 0644)
    os.Rename(cacheFile+".tmp", cacheFile)

    folderCacheMutex.Lock()
    folderCache[folder] = cached
    folderCacheMutex.Unlock()
    return cached, nil
}

func (f *LazyFolder) Cached() (*CachedFolderData, error) {
    f.cacheMutex.RLock()
    if f.cache != nil {
        defer f.cacheMutex.RUnlock()
        return f.cache, nil
    }
    f.cacheMutex.RUnlock()

    f.cacheMutex.Lock()
    defer f.cacheMutex.Unlock()
    if f.cache != nil {
        return f.cache, nil
    }

    f.wg.Add(1)
    go func() {
        defer f.wg.Done()
        data, err := f.folderFetch(f.path)
        if err != nil {
            fmt.Printf("Error fetching %s: %v\n", f.path, err)
            return
        }
        f.cacheMutex.Lock()
        f.cache = data
        f.cacheMutex.Unlock()
        f.flusher.Subscribe(f)
    }()
    data, err := f.folderFetch(f.path)
    if err != nil {
        return nil, err
    }
    f.cache = data
    f.flusher.Subscribe(f)
    return data, nil
}

func (f *LazyFolder) saveCache() error {
    f.cacheMutex.RLock()
    defer f.cacheMutex.RUnlock()
    if f.cache == nil {
        return nil
    }
    cachePath := filepath.Join(f.cacheDir, f.path)
    os.MkdirAll(cachePath, 0755)
    cacheFile := filepath.Join(cachePath, ".directory_contents_cached_v2.json")
    data, err := json.Marshal(f.cache)
    if err != nil {
        return err
    }
    tmpFile := cacheFile + ".tmp"
    if err := os.WriteFile(tmpFile, data, 0644); err != nil {
        return err
    }
    return os.Rename(tmpFile, cacheFile)
}

func (f *LazyFolder) allSizesSet() bool {
    f.cacheMutex.RLock()
    defer f.cacheMutex.RUnlock()
    if f.cache == nil {
        return false
    }
    for _, file := range f.cache.Files {
        if file.Size == nil {
            return false
        }
    }
    return true
}

func (f *LazyFolder) Stop() {
    f.flusher.Stop()
}

func (f *LazyFolder) FoldersAndFiles() (map[string]*LazyFolder, map[string]*LazyFile, error) {
    c, err := f.Cached()
    if err != nil {
        return nil, nil, err
    }
    folders := make(map[string]*LazyFolder)
    files := make(map[string]*LazyFile)
    for _, k := range c.Folders {
        subFolder := NewLazyFolder(filepath.Join(f.path, k), f.cacheDir, f.rootURL)
        subFolder.fs = f.fs
        folders[k] = subFolder
    }
    for k := range c.Files {
        files[k] = &LazyFile{name: k, parent: f}
    }
    return folders, files, nil
}

func (f *LazyFolder) FileSizeBytesApproximate(name string) (int64, error) {
    c, err := f.Cached()
    if err != nil {
        return 0, err
    }
    file, ok := c.Files[name]
    if !ok {
        return 0, fmt.Errorf("file %s not found", name)
    }
    if file.Size != nil {
        return *file.Size, nil
    }
    return file.SizeApproximate, nil
}

func (f *LazyFolder) FileSizeBytesExact(name string) (int64, error) {
    c, err := f.Cached()
    if err != nil {
        return 0, err
    }
    file, ok := c.Files[name]
    if !ok {
        return 0, fmt.Errorf("file %s not found", name)
    }
    if file.Size != nil {
        return *file.Size, nil
    }
    size, err := f.fetchHeaders(f.buildURL(f.rootURL, f.path, name))
    if err != nil {
        return 0, err
    }
    f.cacheMutex.Lock()
    c.Files[name] = CachedFileData{Size: &size, SizeApproximate: size}
    f.cacheMutex.Unlock()
    f.flusher.Subscribe(f)
    f.wg.Add(1)
    go func() {
        defer f.wg.Done()
        _, err := f.fetchHeaders(f.buildURL(f.rootURL, f.path, name))
        if err == nil {
            f.flusher.Subscribe(f)
        }
    }()
    return size, nil
}

func (f *LazyFolder) FileEnsureFetched(name string) error {
    filePath := filepath.Join(f.cacheDir, f.path, name)
    f.fetchMutex.Lock()
    if ch, ok := f.fetchOnce[filePath]; ok {
        f.fetchMutex.Unlock()
        <-ch
        return nil
    }
    ch := make(chan struct{})
    f.fetchOnce[filePath] = ch
    f.fetchMutex.Unlock()

    defer func() {
        f.fetchMutex.Lock()
        close(ch)
        delete(f.fetchOnce, filePath)
        f.fetchMutex.Unlock()
    }()

    if _, err := os.Stat(filePath); os.IsNotExist(err) {
        return f.fetchBytes(f.buildURL(f.rootURL, f.path, name), filePath)
    }
    return nil
}

func (f *LazyFolder) FileCachePath(name string) (string, error) {
    err := f.FileEnsureFetched(name)
    if err != nil {
        return "", err
    }
    return filepath.Join(f.cacheDir, f.path, name), nil
}

func (f *LazyFolder) List(path string) (string, error) {
    folder, err := WalkPathFindFolder(f, path)
    if err != nil {
        return "", err
    }
    folders, files, err := folder.FoldersAndFiles()
    if err != nil {
        return "", err
    }
    var b strings.Builder
    b.WriteString(fmt.Sprintf("INFO %s:\n", folder.path))
    b.WriteString(fmt.Sprintf("folders: %v\n", keys(folders)))
    b.WriteString(fmt.Sprintf("files: %v\n", keys(files)))
    return b.String(), nil
}

func (f *LazyFolder) ListRecursive(path string) error {
    folder, err := WalkPathFindFolder(f, path)
    if err != nil {
        return err
    }
    return ListImportant(folder, "")
}

func (f *LazyFolder) PrefetchMeta(path string) error {
    folder, err := WalkPathFindFolder(f, path)
    if err != nil {
        return err
    }
    folders, _, err := folder.FoldersAndFiles()
    if err != nil {
        return err
    }
    for _, subFolder := range folders {
        if err := subFolder.PrefetchMeta("");
            err != nil {
            return err
        }
    }
    return nil
}

func (f *LazyFolder) PrefetchFiles(path string) error {
    folder, err := WalkPathFindFolder(f, path)
    if err != nil {
        return err
    }
    folders, files, err := folder.FoldersAndFiles()
    if err != nil {
        return err
    }
    for _, file := range files {
        if err := file.EnsureFetched(); err != nil {
            return err
        }
    }
    for _, subFolder := range folders {
        if err := subFolder.PrefetchFiles("");
            err != nil {
            return err
        }
    }
    return nil
}

func (f *LazyFolder) DuApproximate(path string) error {
    folder, err := WalkPathFindFolder(f, path)
    if err != nil {
        return err
    }
    _, err = ListAndSizeApproximateFastParallel(folder, "")
    return err
}
EOF
