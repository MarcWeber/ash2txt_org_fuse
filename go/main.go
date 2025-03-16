package main

import (
	"flag"
	"fmt"
	"os"

	"go_fs_project/filesystems"
)

func main() {
	fetchLimit := flag.Int("fetch-limit", 40, "Maximum number of concurrent fetch operations")
	fuseVersion := flag.String("fuse-version", "fuse", "FUSE version to use: 'fuse' or 'fuse3'")
	flag.Parse()

	if len(flag.Args()) < 3 {
		fmt.Println("Usage: ./go_fs_project [--fetch-limit N] [--fuse-version fuse|fuse3] <cacheDir> <rootURL> <command> [path/mountpoint]")
		fmt.Println("Commands: list, list_recursive, du_approximate, prefetch-meta, prefetch-files, mount")
		os.Exit(1)
	}

	cacheDir := flag.Arg(0)
	rootURL := flag.Arg(1)
	command := flag.Arg(2)
	path := ""
	if len(flag.Args()) > 3 {
		path = flag.Arg(3)
	}

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
	case "mount":
		if path == "" {
			fmt.Println("Mount command requires a mountpoint")
			os.Exit(1)
		}
		switch *fuseVersion {
		case "fuse":
			if err := filesystems.MountFUSE(path, cacheDir, rootURL, *fetchLimit); err != nil {
				fmt.Printf("Error mounting FUSE at %s: %v\n", path, err)
				os.Exit(1)
			}
		case "fuse3":
			if err := filesystems.MountFUSE3(path, cacheDir, rootURL, *fetchLimit); err != nil {
				fmt.Printf("Error mounting FUSE3 at %s: %v\n", path, err)
				os.Exit(1)
			}
		default:
			fmt.Printf("Unknown fuse-version: %s. Use 'fuse' or 'fuse3'\n", *fuseVersion)
			os.Exit(1)
		}
	default:
		fmt.Printf("Unknown command: %s\n", command)
		fmt.Println("Commands: list, list_recursive, du_approximate, prefetch-meta, prefetch-files, mount")
		os.Exit(1)
	}

	rootFolder.Stop()
}
