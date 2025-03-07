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
    case "mount":
        if len(os.Args) != 6 {
            fmt.Println("mount requires PATH and MOUNT_POINT")
            os.Exit(1)
        }
        path := os.Args[4]
        mountpoint := os.Args[5]
        subFolder, err := filesystems.WalkPathFindFolder(rootFolder, path)
        if err != nil {
            fmt.Printf("Error: %v\n", err)
            os.Exit(1)
        }
        filesystems.Mount(subFolder, mountpoint)
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
