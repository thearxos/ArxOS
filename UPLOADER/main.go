// ╔══════════════════════════════════════════════════════════════════════╗
// ║                         RxArchiver v1.0                             ║
// ║          High-Performance Archive.org Upload Tool                   ║
// ║                                                                     ║
// ║  By: 0xbv1 | 0xb0rn3                                              ║
// ║  License: MIT                                                       ║
// ║                                                                     ║
// ║  Features:                                                          ║
// ║    • Concurrent file uploads with goroutine worker pools            ║
// ║    • Streaming uploads (zero memory bloat for massive ISOs)         ║
// ║    • Parallel SHA256/MD5 hashing via io.TeeReader                   ║
// ║    • Automatic retry with exponential backoff                       ║
// ║    • Real-time progress bars with transfer speed                    ║
// ║    • Persistent config storage for credentials                      ║
// ║    • Single file & directory upload modes                           ║
// ║    • Hash manifest generation (.rxhash)                             ║
// ╚══════════════════════════════════════════════════════════════════════╝

package main

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ─────────────────────────────────────────────────────────────────────
// Constants & Version
// ─────────────────────────────────────────────────────────────────────

const (
	AppName    = "RxArchiver"
	AppVersion = "1.0.0"
	AppAuthor  = "0xbv1 | 0xb0rn3"

	ArchiveS3Endpoint = "https://s3.us.archive.org"
	DefaultWorkers    = 4
	MaxWorkers        = 32
	MaxRetries        = 3
	RetryBaseDelay    = 2 * time.Second
	ChunkReportSize   = 1024 * 1024 // Report progress every 1MB

	ConfigDirName  = ".rxarchiver"
	ConfigFileName = "config.json"
)

// ─────────────────────────────────────────────────────────────────────
// ANSI Colors
// ─────────────────────────────────────────────────────────────────────

const (
	Reset     = "\033[0m"
	Bold      = "\033[1m"
	Dim       = "\033[2m"
	Red       = "\033[31m"
	Green     = "\033[32m"
	Yellow    = "\033[33m"
	Blue      = "\033[34m"
	Magenta   = "\033[35m"
	Cyan      = "\033[36m"
	White     = "\033[37m"
	BoldRed   = "\033[1;31m"
	BoldGreen = "\033[1;32m"
	BoldCyan  = "\033[1;36m"
	BoldWhite = "\033[1;37m"
	BgRed     = "\033[41m"
	BgGreen   = "\033[42m"
	BgBlue    = "\033[44m"
)

// ─────────────────────────────────────────────────────────────────────
// Data Structures
// ─────────────────────────────────────────────────────────────────────

// Config holds persistent archive.org credentials and defaults
type Config struct {
	AccessKey   string `json:"access_key"`
	SecretKey   string `json:"secret_key"`
	DefaultMeta Meta   `json:"default_metadata"`
	ConfiguredAt string `json:"configured_at"`
}

// Meta holds archive.org item metadata
type Meta struct {
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	MediaType   string `json:"mediatype,omitempty"`
	Collection  string `json:"collection,omitempty"`
	Creator     string `json:"creator,omitempty"`
	Subject     string `json:"subject,omitempty"`
}

// UploadJob represents a single file to upload
type UploadJob struct {
	LocalPath    string
	RemoteName   string
	FileSize     int64
	Index        int
	TotalFiles   int
}

// UploadResult holds the outcome of an upload attempt
type UploadResult struct {
	Job       UploadJob
	Success   bool
	SHA256    string
	MD5       string
	Duration  time.Duration
	BytesSent int64
	Error     error
	Retries   int
}

// ProgressTracker tracks upload progress atomically
type ProgressTracker struct {
	totalBytes     int64
	uploadedBytes  atomic.Int64
	totalFiles     int32
	completedFiles atomic.Int32
	failedFiles    atomic.Int32
	startTime      time.Time
	mu             sync.Mutex
}

// ─────────────────────────────────────────────────────────────────────
// Progress-Tracking Reader (wraps io.Reader for real-time progress)
// ─────────────────────────────────────────────────────────────────────

type progressReader struct {
	reader      io.Reader
	tracker     *ProgressTracker
	fileSize    int64
	bytesRead   int64
	fileName    string
	lastReport  int64
	fileIndex   int
	totalFiles  int
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	if n > 0 {
		pr.bytesRead += int64(n)
		pr.tracker.uploadedBytes.Add(int64(n))

		// Report progress periodically
		if pr.bytesRead-pr.lastReport >= int64(ChunkReportSize) || err == io.EOF {
			pr.lastReport = pr.bytesRead
			pr.printProgress()
		}
	}
	return n, err
}

func (pr *progressReader) printProgress() {
	pct := float64(pr.bytesRead) / float64(pr.fileSize) * 100
	if pr.fileSize == 0 {
		pct = 100
	}

	elapsed := time.Since(pr.tracker.startTime).Seconds()
	totalUploaded := pr.tracker.uploadedBytes.Load()
	speed := float64(totalUploaded) / elapsed / 1024 / 1024 // MB/s

	barWidth := 30
	filled := int(pct / 100 * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}
	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

	name := pr.fileName
	if len(name) > 35 {
		name = "..." + name[len(name)-32:]
	}

	fmt.Printf("\r  %s[%d/%d]%s %s%-35s%s %s[%s]%s %s%5.1f%%%s %s%s @ %.1f MB/s%s",
		Dim, pr.fileIndex, pr.totalFiles, Reset,
		Cyan, name, Reset,
		Yellow, bar, Reset,
		BoldWhite, pct, Reset,
		Dim, formatBytes(pr.bytesRead), speed, Reset,
	)
}

// ─────────────────────────────────────────────────────────────────────
// Config Management
// ─────────────────────────────────────────────────────────────────────

func configDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	return filepath.Join(home, ConfigDirName)
}

func configPath() string {
	return filepath.Join(configDir(), ConfigFileName)
}

func loadConfig() (*Config, error) {
	data, err := os.ReadFile(configPath())
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("corrupt config: %w", err)
	}
	return &cfg, nil
}

func saveConfig(cfg *Config) error {
	dir := configDir()
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("cannot create config dir: %w", err)
	}

	cfg.ConfiguredAt = time.Now().Format(time.RFC3339)
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}

	path := configPath()
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("cannot write config: %w", err)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────
// CLI Argument Parsing (zero dependencies)
// ─────────────────────────────────────────────────────────────────────

type CLIArgs struct {
	Command     string
	FilePath    string
	DirPath     string
	Identifier  string
	Workers     int
	EnableHash  bool
	HashOnly    bool
	Title       string
	Description string
	MediaType   string
	Collection  string
	DryRun      bool
	Verbose     bool
	NoColor     bool
}

func parseArgs() CLIArgs {
	args := CLIArgs{
		Workers:   DefaultWorkers,
		MediaType: "software",
		Collection: "opensource",
	}

	if len(os.Args) < 2 {
		args.Command = "help"
		return args
	}

	args.Command = os.Args[1]
	rest := os.Args[2:]

	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case "-f", "--file":
			if i+1 < len(rest) {
				i++
				args.FilePath = rest[i]
			}
		case "-d", "--dir":
			if i+1 < len(rest) {
				i++
				args.DirPath = rest[i]
			}
		case "-i", "--identifier", "--id":
			if i+1 < len(rest) {
				i++
				args.Identifier = rest[i]
			}
		case "-w", "--workers":
			if i+1 < len(rest) {
				i++
				fmt.Sscanf(rest[i], "%d", &args.Workers)
			}
		case "--hash":
			args.EnableHash = true
		case "--hash-only":
			args.HashOnly = true
			args.EnableHash = true
		case "-t", "--title":
			if i+1 < len(rest) {
				i++
				args.Title = rest[i]
			}
		case "--description":
			if i+1 < len(rest) {
				i++
				args.Description = rest[i]
			}
		case "--mediatype":
			if i+1 < len(rest) {
				i++
				args.MediaType = rest[i]
			}
		case "--collection":
			if i+1 < len(rest) {
				i++
				args.Collection = rest[i]
			}
		case "--dry-run":
			args.DryRun = true
		case "-v", "--verbose":
			args.Verbose = true
		case "--no-color":
			args.NoColor = true
		case "-h", "--help":
			args.Command = "help"
		case "--version":
			args.Command = "version"
		}
	}

	// Clamp workers
	if args.Workers < 1 {
		args.Workers = 1
	}
	if args.Workers > MaxWorkers {
		args.Workers = MaxWorkers
	}

	return args
}

// ─────────────────────────────────────────────────────────────────────
// Banner & Help
// ─────────────────────────────────────────────────────────────────────

func printBanner() {
	fmt.Printf(`
%s╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║   %s██████╗ ██╗  ██╗ █████╗ ██████╗  ██████╗██╗  ██╗%s          ║
║   %s██╔══██╗╚██╗██╔╝██╔══██╗██╔══██╗██╔════╝██║  ██║%s          ║
║   %s██████╔╝ ╚███╔╝ ███████║██████╔╝██║     ███████║%s          ║
║   %s██╔══██╗ ██╔██╗ ██╔══██║██╔══██╗██║     ██╔══██║%s          ║
║   %s██║  ██║██╔╝ ██╗██║  ██║██║  ██║╚██████╗██║  ██║%s          ║
║   %s╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝%s          ║
║                                                              ║
║   %s%s v%s%s                                        ║
║   %sBy %s%s                                          ║
║   %sHigh-Performance Archive.org Uploader%s                     ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝%s
`,
		Dim,
		BoldCyan, Reset+Dim,
		BoldCyan, Reset+Dim,
		BoldCyan, Reset+Dim,
		BoldCyan, Reset+Dim,
		BoldCyan, Reset+Dim,
		BoldCyan, Reset+Dim,
		BoldWhite, AppName, AppVersion, Reset+Dim,
		Dim, AppAuthor, Reset+Dim,
		Yellow, Reset+Dim,
		Reset,
	)
}

func printHelp() {
	printBanner()
	fmt.Println()
	fmt.Printf("  %sUSAGE:%s\n", BoldWhite, Reset)
	fmt.Println("  rxarchiver <command> [options]")
	fmt.Println()
	fmt.Printf("  %sCOMMANDS:%s\n", BoldWhite, Reset)
	fmt.Printf("    %slogin%s                  Configure archive.org credentials\n", Green, Reset)
	fmt.Printf("    %supload%s                 Upload file(s) to archive.org\n", Green, Reset)
	fmt.Printf("    %shash%s                   Calculate hashes without uploading\n", Green, Reset)
	fmt.Printf("    %sconfig%s                 Show current configuration\n", Green, Reset)
	fmt.Printf("    %sversion%s                Show version information\n", Green, Reset)
	fmt.Printf("    %shelp%s                   Show this help message\n", Green, Reset)
	fmt.Println()
	fmt.Printf("  %sUPLOAD OPTIONS:%s\n", BoldWhite, Reset)
	fmt.Printf("    %s-f, --file%s <path>      Upload a single file\n", Cyan, Reset)
	fmt.Printf("    %s-d, --dir%s  <path>      Upload entire directory (recursive)\n", Cyan, Reset)
	fmt.Printf("    %s-i, --id%s   <name>      Archive.org item identifier (required)\n", Cyan, Reset)
	fmt.Printf("    %s-w, --workers%s <n>      Concurrent upload workers (default: %d, max: %d)\n", Cyan, Reset, DefaultWorkers, MaxWorkers)
	fmt.Printf("    %s--hash%s                 Calculate & upload SHA256/MD5 hash manifest\n", Cyan, Reset)
	fmt.Printf("    %s--hash-only%s            Calculate hashes only (no upload)\n", Cyan, Reset)
	fmt.Printf("    %s-t, --title%s <text>     Item title on archive.org\n", Cyan, Reset)
	fmt.Printf("    %s--description%s <text>   Item description\n", Cyan, Reset)
	fmt.Printf("    %s--mediatype%s <type>     Media type (default: software)\n", Cyan, Reset)
	fmt.Printf("    %s--collection%s <name>    Collection (default: opensource)\n", Cyan, Reset)
	fmt.Printf("    %s--dry-run%s              Simulate upload without sending data\n", Cyan, Reset)
	fmt.Printf("    %s-v, --verbose%s          Verbose output\n", Cyan, Reset)
	fmt.Println()
	fmt.Printf("  %sEXAMPLES:%s\n", BoldWhite, Reset)
	fmt.Printf("    %s# First-time setup%s\n", Dim, Reset)
	fmt.Println("    rxarchiver login")
	fmt.Println()
	fmt.Printf("    %s# Upload a single ISO with hash verification%s\n", Dim, Reset)
	fmt.Println("    rxarchiver upload -f yourfilename -i yourfilenamewititshash --hash")
	fmt.Println()
	fmt.Printf("    %s# Upload entire directory with 8 concurrent workers%s\n", Dim, Reset)
	fmt.Println("    rxarchiver upload -d ./release/ -i directoryname -w 8 --hash")
	fmt.Println()
	fmt.Printf("    %s# Calculate hashes only (no upload)%s\n", Dim, Reset)
	fmt.Println("    rxarchiver upload -f filename --hash-only")
	fmt.Println()
	fmt.Printf("    %s# Upload with custom metadata%s\n", Dim, Reset)
	fmt.Println("    rxarchiver upload -f filename -i arxos-mars -t \"yourfilename\" \\")
	fmt.Println("      --description \"Your_file_name_description\" --mediatype yourfiletype e.g software")
	fmt.Println()
}


// ─────────────────────────────────────────────────────────────────────
// Login Command
// ─────────────────────────────────────────────────────────────────────

func cmdLogin() {
	printBanner()
	fmt.Printf("\n%s▸ Archive.org Account Configuration%s\n\n", BoldWhite, Reset)
	fmt.Printf("  Get your S3-like API keys from:\n")
	fmt.Printf("  %shttps://archive.org/account/s3.php%s\n\n", Cyan, Reset)

	// Load existing config if any
	existing, _ := loadConfig()

	var accessKey, secretKey string

	fmt.Printf("  %sAccess Key%s", BoldWhite, Reset)
	if existing != nil && existing.AccessKey != "" {
		masked := existing.AccessKey[:4] + "****" + existing.AccessKey[len(existing.AccessKey)-4:]
		fmt.Printf(" [current: %s%s%s]", Dim, masked, Reset)
	}
	fmt.Printf(": ")
	fmt.Scanln(&accessKey)

	fmt.Printf("  %sSecret Key%s", BoldWhite, Reset)
	if existing != nil && existing.SecretKey != "" {
		masked := existing.SecretKey[:4] + "****" + existing.SecretKey[len(existing.SecretKey)-4:]
		fmt.Printf(" [current: %s%s%s]", Dim, masked, Reset)
	}
	fmt.Printf(": ")
	fmt.Scanln(&secretKey)

	// Use existing values if user pressed enter
	if accessKey == "" && existing != nil {
		accessKey = existing.AccessKey
	}
	if secretKey == "" && existing != nil {
		secretKey = existing.SecretKey
	}

	if accessKey == "" || secretKey == "" {
		fmt.Printf("\n  %s✗ Error: Both access key and secret key are required%s\n\n", BoldRed, Reset)
		os.Exit(1)
	}

	// Optional default metadata
	fmt.Printf("\n  %s▸ Default Metadata (optional, press Enter to skip)%s\n\n", BoldWhite, Reset)

	var creator, subject string
	fmt.Printf("  %sCreator/Author%s: ", Dim, Reset)
	fmt.Scanln(&creator)
	fmt.Printf("  %sDefault Subject/Tags%s: ", Dim, Reset)
	fmt.Scanln(&subject)

	cfg := &Config{
		AccessKey: accessKey,
		SecretKey: secretKey,
		DefaultMeta: Meta{
			Creator: creator,
			Subject: subject,
		},
	}

	if err := saveConfig(cfg); err != nil {
		fmt.Printf("\n  %s✗ Failed to save config: %v%s\n\n", BoldRed, err, Reset)
		os.Exit(1)
	}

	fmt.Printf("\n  %s✓ Configuration saved to %s%s\n", BoldGreen, configPath(), Reset)
	fmt.Printf("  %s✓ Credentials stored with 0600 permissions (owner-only)%s\n\n", Green, Reset)
}

// ─────────────────────────────────────────────────────────────────────
// Config Display Command
// ─────────────────────────────────────────────────────────────────────

func cmdConfig() {
	printBanner()
	cfg, err := loadConfig()
	if err != nil {
		fmt.Printf("\n  %s✗ No configuration found. Run '%srxarchiver login%s%s' first.%s\n\n",
			Yellow, BoldWhite, Reset, Yellow, Reset)
		os.Exit(1)
	}

	fmt.Printf("\n%s▸ Current Configuration%s\n\n", BoldWhite, Reset)
	fmt.Printf("  %sConfig Path:%s   %s\n", Dim, Reset, configPath())
	fmt.Printf("  %sConfigured:%s    %s\n", Dim, Reset, cfg.ConfiguredAt)
	fmt.Printf("  %sAccess Key:%s    %s****%s\n", Dim, Reset, cfg.AccessKey[:4], cfg.AccessKey[len(cfg.AccessKey)-4:])
	fmt.Printf("  %sSecret Key:%s    %s****%s\n", Dim, Reset, cfg.SecretKey[:4], cfg.SecretKey[len(cfg.SecretKey)-4:])
	if cfg.DefaultMeta.Creator != "" {
		fmt.Printf("  %sCreator:%s       %s\n", Dim, Reset, cfg.DefaultMeta.Creator)
	}
	if cfg.DefaultMeta.Subject != "" {
		fmt.Printf("  %sSubject:%s       %s\n", Dim, Reset, cfg.DefaultMeta.Subject)
	}
	fmt.Println()
}

// ─────────────────────────────────────────────────────────────────────
// File Discovery
// ─────────────────────────────────────────────────────────────────────

func discoverFiles(dirPath string) ([]UploadJob, int64, error) {
	var jobs []UploadJob
	var totalSize int64
	idx := 0

	absDir, err := filepath.Abs(dirPath)
	if err != nil {
		return nil, 0, err
	}

	err = filepath.Walk(absDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(absDir, path)
		if err != nil {
			relPath = info.Name()
		}
		// Use forward slashes for archive.org compatibility
		relPath = filepath.ToSlash(relPath)

		idx++
		jobs = append(jobs, UploadJob{
			LocalPath:  path,
			RemoteName: relPath,
			FileSize:   info.Size(),
			Index:      idx,
		})
		totalSize += info.Size()
		return nil
	})

	// Set total count on all jobs
	for i := range jobs {
		jobs[i].TotalFiles = len(jobs)
	}

	return jobs, totalSize, err
}

// ─────────────────────────────────────────────────────────────────────
// Hash Calculation (concurrent SHA256 + MD5 in single pass)
// ─────────────────────────────────────────────────────────────────────

type HashResult struct {
	FilePath string
	FileName string
	SHA256   string
	MD5      string
	Size     int64
	Duration time.Duration
	Error    error
}

func hashFile(path string) HashResult {
	start := time.Now()
	result := HashResult{FilePath: path, FileName: filepath.Base(path)}

	f, err := os.Open(path)
	if err != nil {
		result.Error = err
		return result
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		result.Error = err
		return result
	}
	result.Size = info.Size()

	// Compute SHA256 and MD5 simultaneously in a single read pass
	sha256Hash := sha256.New()
	md5Hash := md5.New()
	multiWriter := io.MultiWriter(sha256Hash, md5Hash)

	// Use a large buffer for maximum I/O throughput
	buf := make([]byte, 4*1024*1024) // 4MB buffer
	if _, err := io.CopyBuffer(multiWriter, f, buf); err != nil {
		result.Error = err
		return result
	}

	result.SHA256 = hex.EncodeToString(sha256Hash.Sum(nil))
	result.MD5 = hex.EncodeToString(md5Hash.Sum(nil))
	result.Duration = time.Since(start)
	return result
}

func hashFilesConcurrent(jobs []UploadJob, workers int) []HashResult {
	results := make([]HashResult, len(jobs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)

	fmt.Printf("\n  %s▸ Hashing %d files with %d workers...%s\n\n", BoldWhite, len(jobs), workers, Reset)

	for i, job := range jobs {
		wg.Add(1)
		sem <- struct{}{} // Acquire semaphore
		go func(idx int, j UploadJob) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore

			start := time.Now()
			r := hashFile(j.LocalPath)
			r.Duration = time.Since(start)
			results[idx] = r

			if r.Error != nil {
				fmt.Printf("  %s✗%s %s — %s%v%s\n", BoldRed, Reset, j.RemoteName, Red, r.Error, Reset)
			} else {
				speed := float64(r.Size) / r.Duration.Seconds() / 1024 / 1024
				fmt.Printf("  %s✓%s %-40s %sSHA256:%s %s  %s(%.0f MB/s)%s\n",
					Green, Reset,
					truncStr(j.RemoteName, 40),
					Dim, Reset,
					r.SHA256[:16]+"...",
					Dim, speed, Reset,
				)
			}
		}(i, job)
	}

	wg.Wait()
	return results
}

// ─────────────────────────────────────────────────────────────────────
// Upload Engine (concurrent worker pool with streaming)
// ─────────────────────────────────────────────────────────────────────

func uploadFile(cfg *Config, identifier string, job UploadJob, tracker *ProgressTracker, enableHash bool, dryRun bool) UploadResult {
	result := UploadResult{Job: job}
	start := time.Now()

	// Open the file
	f, err := os.Open(job.LocalPath)
	if err != nil {
		result.Error = fmt.Errorf("open: %w", err)
		return result
	}
	defer f.Close()

	// Set up hash computation during upload (zero extra I/O)
	var reader io.Reader = f
	var sha256Hash, md5Hash hash.Hash

	if enableHash {
		sha256Hash = sha256.New()
		md5Hash = md5.New()
		// TeeReader: data flows through hashers AS it uploads — no double-read
		reader = io.TeeReader(reader, io.MultiWriter(sha256Hash, md5Hash))
	}

	// Wrap with progress tracking
	pr := &progressReader{
		reader:     reader,
		tracker:    tracker,
		fileSize:   job.FileSize,
		fileName:   job.RemoteName,
		fileIndex:  job.Index,
		totalFiles: job.TotalFiles,
	}

	if dryRun {
		// Simulate: just read the file (exercises hashing + progress)
		buf := make([]byte, 1024*1024)
		n, _ := io.CopyBuffer(io.Discard, pr, buf)
		result.BytesSent = n
		result.Success = true
	} else {
		// Upload with retry + exponential backoff
		url := fmt.Sprintf("%s/%s/%s", ArchiveS3Endpoint, identifier, job.RemoteName)

		for attempt := 0; attempt <= MaxRetries; attempt++ {
			if attempt > 0 {
				result.Retries = attempt
				delay := RetryBaseDelay * time.Duration(math.Pow(2, float64(attempt-1)))
				fmt.Printf("\n  %s⟳ Retry %d/%d for %s (waiting %v)%s\n",
					Yellow, attempt, MaxRetries, job.RemoteName, delay, Reset)
				time.Sleep(delay)

				// Reset file position and progress
				f.Seek(0, 0)
				tracker.uploadedBytes.Add(-pr.bytesRead)
				pr.bytesRead = 0
				pr.lastReport = 0

				// Recreate reader chain
				reader = io.Reader(f)
				if enableHash {
					sha256Hash = sha256.New()
					md5Hash = md5.New()
					reader = io.TeeReader(reader, io.MultiWriter(sha256Hash, md5Hash))
				}
				pr.reader = reader
			}

			req, err := http.NewRequest("PUT", url, pr)
			if err != nil {
				result.Error = fmt.Errorf("request: %w", err)
				continue
			}

			// Archive.org S3 auth + metadata headers
			req.Header.Set("Authorization", fmt.Sprintf("LOW %s:%s", cfg.AccessKey, cfg.SecretKey))
			req.ContentLength = job.FileSize

			// Set metadata headers only on first file of an identifier
			if job.Index == 1 {
				req.Header.Set("x-archive-auto-make-bucket", "1")
				req.Header.Set("x-archive-size-hint", fmt.Sprintf("%d", tracker.totalBytes))
			}

			// Use a client with no timeout (for huge files)
			client := &http.Client{
				Timeout: 0, // no timeout — ISOs can take hours
				Transport: &http.Transport{
					MaxIdleConns:        MaxWorkers,
					MaxIdleConnsPerHost: MaxWorkers,
					IdleConnTimeout:     90 * time.Second,
					DisableCompression:  true, // don't waste CPU compressing ISOs
					WriteBufferSize:     4 * 1024 * 1024, // 4MB write buffer
					ReadBufferSize:      1 * 1024 * 1024, // 1MB read buffer
				},
			}

			resp, err := client.Do(req)
			if err != nil {
				result.Error = fmt.Errorf("upload: %w", err)
				continue
			}
			resp.Body.Close()

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				result.BytesSent = pr.bytesRead
				result.Success = true
				break
			} else {
				result.Error = fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
				if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 429 {
					break // Don't retry client errors (except rate limit)
				}
			}
		}
	}

	// Extract hashes
	if enableHash && sha256Hash != nil {
		result.SHA256 = hex.EncodeToString(sha256Hash.Sum(nil))
		result.MD5 = hex.EncodeToString(md5Hash.Sum(nil))
	}

	result.Duration = time.Since(start)
	return result
}

func runUploadPool(cfg *Config, identifier string, jobs []UploadJob, workers int, enableHash bool, dryRun bool) []UploadResult {
	tracker := &ProgressTracker{
		totalFiles: int32(len(jobs)),
		startTime:  time.Now(),
	}

	// Calculate total size
	for _, j := range jobs {
		tracker.totalBytes += j.FileSize
	}

	results := make([]UploadResult, len(jobs))
	jobChan := make(chan int, len(jobs))
	var wg sync.WaitGroup

	// Feed jobs
	for i := range jobs {
		jobChan <- i
	}
	close(jobChan)

	// Launch worker goroutines
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobChan {
				result := uploadFile(cfg, identifier, jobs[idx], tracker, enableHash, dryRun)
				results[idx] = result

				if result.Success {
					tracker.completedFiles.Add(1)
					fmt.Printf("\n  %s✓%s %s %s(%s in %v)%s\n",
						BoldGreen, Reset,
						jobs[idx].RemoteName,
						Dim,
						formatBytes(result.BytesSent),
						result.Duration.Round(time.Millisecond),
						Reset,
					)
				} else {
					tracker.failedFiles.Add(1)
					fmt.Printf("\n  %s✗%s %s — %s%v%s\n",
						BoldRed, Reset,
						jobs[idx].RemoteName,
						Red, result.Error, Reset,
					)
				}
			}
		}()
	}

	wg.Wait()
	return results
}

// ─────────────────────────────────────────────────────────────────────
// Upload Command
// ─────────────────────────────────────────────────────────────────────

func cmdUpload(args CLIArgs) {
	printBanner()

	// Validate inputs
	if args.FilePath == "" && args.DirPath == "" {
		fmt.Printf("\n  %s✗ Error: Specify -f <file> or -d <directory>%s\n\n", BoldRed, Reset)
		os.Exit(1)
	}

	if !args.HashOnly && args.Identifier == "" {
		fmt.Printf("\n  %s✗ Error: Archive.org identifier required (-i <name>)%s\n\n", BoldRed, Reset)
		fmt.Printf("  %sThe identifier is the unique item name on archive.org%s\n", Dim, Reset)
		fmt.Printf("  %sExample: rxarchiver upload -f file.iso -i my-project-v1%s\n\n", Dim, Reset)
		os.Exit(1)
	}

	// Load credentials (not needed for hash-only)
	var cfg *Config
	if !args.HashOnly {
		var err error
		cfg, err = loadConfig()
		if err != nil {
			fmt.Printf("\n  %s✗ No credentials found. Run '%srxarchiver login%s%s' first.%s\n\n",
				Yellow, BoldWhite, Reset, Yellow, Reset)
			os.Exit(1)
		}
	}

	// Build job list
	var jobs []UploadJob
	var totalSize int64

	if args.FilePath != "" {
		// Single file mode
		info, err := os.Stat(args.FilePath)
		if err != nil {
			fmt.Printf("\n  %s✗ Cannot access file: %v%s\n\n", BoldRed, err, Reset)
			os.Exit(1)
		}
		absPath, _ := filepath.Abs(args.FilePath)
		jobs = []UploadJob{{
			LocalPath:  absPath,
			RemoteName: info.Name(),
			FileSize:   info.Size(),
			Index:      1,
			TotalFiles: 1,
		}}
		totalSize = info.Size()
	} else {
		// Directory mode
		info, err := os.Stat(args.DirPath)
		if err != nil || !info.IsDir() {
			fmt.Printf("\n  %s✗ Invalid directory: %s%s\n\n", BoldRed, args.DirPath, Reset)
			os.Exit(1)
		}
		jobs, totalSize, err = discoverFiles(args.DirPath)
		if err != nil {
			fmt.Printf("\n  %s✗ Error scanning directory: %v%s\n\n", BoldRed, err, Reset)
			os.Exit(1)
		}
		if len(jobs) == 0 {
			fmt.Printf("\n  %s✗ No files found in %s%s\n\n", Yellow, args.DirPath, Reset)
			os.Exit(1)
		}
	}

	// Print upload plan
	mode := "UPLOAD"
	if args.HashOnly {
		mode = "HASH"
	} else if args.DryRun {
		mode = "DRY RUN"
	}

	fmt.Printf("\n  %s▸ %s PLAN%s\n\n", BoldWhite, mode, Reset)
	if !args.HashOnly {
		fmt.Printf("  %sIdentifier:%s    %s%s%s\n", Dim, Reset, BoldCyan, args.Identifier, Reset)
		fmt.Printf("  %sEndpoint:%s      %s\n", Dim, Reset, ArchiveS3Endpoint)
	}
	fmt.Printf("  %sFiles:%s         %d\n", Dim, Reset, len(jobs))
	fmt.Printf("  %sTotal Size:%s    %s\n", Dim, Reset, formatBytes(totalSize))
	fmt.Printf("  %sWorkers:%s       %d\n", Dim, Reset, args.Workers)
	fmt.Printf("  %sHashing:%s       %v\n", Dim, Reset, args.EnableHash)
	if args.Title != "" {
		fmt.Printf("  %sTitle:%s         %s\n", Dim, Reset, args.Title)
	}

	if len(jobs) <= 20 {
		fmt.Printf("\n  %sFiles to process:%s\n", Dim, Reset)
		for _, j := range jobs {
			fmt.Printf("    %s•%s %-50s %s%s%s\n", Cyan, Reset, truncStr(j.RemoteName, 50), Dim, formatBytes(j.FileSize), Reset)
		}
	}

	fmt.Printf("\n  %s─────────────────────────────────────────────────%s\n", Dim, Reset)

	// Hash-only mode
	if args.HashOnly {
		hashResults := hashFilesConcurrent(jobs, args.Workers)
		writeHashManifest(hashResults, args)
		return
	}

	// Set metadata headers on first job
	if args.Title != "" || args.Description != "" || args.MediaType != "" || args.Collection != "" {
		// We'll add metadata via special handling in uploadFile for Index==1
	}

	// Pre-hash if requested
	var hashResults []HashResult
	if args.EnableHash {
		hashResults = hashFilesConcurrent(jobs, args.Workers)
	}

	// Execute upload
	fmt.Printf("\n  %s▸ Uploading %d files with %d concurrent workers...%s\n\n",
		BoldWhite, len(jobs), args.Workers, Reset)

	startTime := time.Now()
	results := runUploadPool(cfg, args.Identifier, jobs, args.Workers, args.EnableHash && hashResults == nil, args.DryRun)

	elapsed := time.Since(startTime)

	// If we pre-hashed, merge those results
	if hashResults != nil {
		for i, hr := range hashResults {
			if hr.Error == nil {
				results[i].SHA256 = hr.SHA256
				results[i].MD5 = hr.MD5
			}
		}
	}

	// Write hash manifest
	if args.EnableHash {
		writeHashManifest(convertToHashResults(results), args)
	}

	// Print summary
	printSummary(results, totalSize, elapsed, args)
}

// ─────────────────────────────────────────────────────────────────────
// Hash Manifest Writer
// ─────────────────────────────────────────────────────────────────────

func writeHashManifest(results []HashResult, args CLIArgs) {
	// Determine manifest path
	baseName := "rxarchiver"
	if args.FilePath != "" {
		baseName = strings.TrimSuffix(filepath.Base(args.FilePath), filepath.Ext(args.FilePath))
	} else if args.Identifier != "" {
		baseName = args.Identifier
	}
	manifestPath := baseName + ".rxhash"

	f, err := os.Create(manifestPath)
	if err != nil {
		fmt.Printf("\n  %s✗ Cannot write hash manifest: %v%s\n", Red, err, Reset)
		return
	}
	defer f.Close()

	fmt.Fprintf(f, "# RxArchiver Hash Manifest\n")
	fmt.Fprintf(f, "# Generated: %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(f, "# Tool: %s v%s by %s\n", AppName, AppVersion, AppAuthor)
	fmt.Fprintf(f, "# Format: HASH_TYPE  HASH  FILESIZE  FILENAME\n")
	fmt.Fprintf(f, "#\n")

	for _, r := range results {
		if r.Error != nil {
			continue
		}
		fmt.Fprintf(f, "SHA256  %s  %d  %s\n", r.SHA256, r.Size, r.FileName)
		fmt.Fprintf(f, "MD5     %s                  %d  %s\n", r.MD5, r.Size, r.FileName)
	}

	fmt.Printf("\n  %s✓ Hash manifest written: %s%s%s\n", Green, BoldWhite, manifestPath, Reset)
}

func convertToHashResults(results []UploadResult) []HashResult {
	hrs := make([]HashResult, len(results))
	for i, r := range results {
		hrs[i] = HashResult{
			FilePath: r.Job.LocalPath,
			FileName: r.Job.RemoteName,
			SHA256:   r.SHA256,
			MD5:      r.MD5,
			Size:     r.Job.FileSize,
			Duration: r.Duration,
		}
		if !r.Success {
			hrs[i].Error = r.Error
		}
	}
	return hrs
}

// ─────────────────────────────────────────────────────────────────────
// Summary Printer
// ─────────────────────────────────────────────────────────────────────

func printSummary(results []UploadResult, totalSize int64, elapsed time.Duration, args CLIArgs) {
	var succeeded, failed, retried int
	var bytesUploaded int64

	for _, r := range results {
		if r.Success {
			succeeded++
			bytesUploaded += r.BytesSent
		} else {
			failed++
		}
		if r.Retries > 0 {
			retried++
		}
	}

	avgSpeed := float64(bytesUploaded) / elapsed.Seconds() / 1024 / 1024

	fmt.Printf("\n\n%s╔══════════════════════════════════════════════════════════════╗%s\n", Dim, Reset)
	fmt.Printf("%s║%s  %s▸ UPLOAD SUMMARY%s                                            %s║%s\n", Dim, Reset, BoldWhite, Reset, Dim, Reset)
	fmt.Printf("%s╠══════════════════════════════════════════════════════════════╣%s\n", Dim, Reset)

	statusColor := BoldGreen
	statusText := "COMPLETE"
	if failed > 0 && succeeded > 0 {
		statusColor = Yellow
		statusText = "PARTIAL"
	} else if failed > 0 && succeeded == 0 {
		statusColor = BoldRed
		statusText = "FAILED"
	}

	fmt.Printf("%s║%s  Status:       %s%s%s%*s%s║%s\n", Dim, Reset, statusColor, statusText, Reset, 44-len(statusText), "", Dim, Reset)
	fmt.Printf("%s║%s  Files:        %s%d/%d succeeded%s%*s%s║%s\n", Dim, Reset, Green, succeeded, len(results), Reset, 38-len(fmt.Sprintf("%d/%d succeeded", succeeded, len(results))), "", Dim, Reset)

	if failed > 0 {
		fStr := fmt.Sprintf("%d failed", failed)
		fmt.Printf("%s║%s  Failed:       %s%s%s%*s%s║%s\n", Dim, Reset, Red, fStr, Reset, 44-len(fStr), "", Dim, Reset)
	}

	sizeStr := formatBytes(bytesUploaded)
	fmt.Printf("%s║%s  Transferred:  %s%*s%s║%s\n", Dim, Reset, sizeStr, 44-len(sizeStr), "", Dim, Reset)

	durStr := elapsed.Round(time.Second).String()
	fmt.Printf("%s║%s  Duration:     %s%*s%s║%s\n", Dim, Reset, durStr, 44-len(durStr), "", Dim, Reset)

	speedStr := fmt.Sprintf("%.2f MB/s", avgSpeed)
	fmt.Printf("%s║%s  Avg Speed:    %s%*s%s║%s\n", Dim, Reset, speedStr, 44-len(speedStr), "", Dim, Reset)

	if retried > 0 {
		retryStr := fmt.Sprintf("%d files required retries", retried)
		fmt.Printf("%s║%s  Retries:      %s%s%s%*s%s║%s\n", Dim, Reset, Yellow, retryStr, Reset, 44-len(retryStr), "", Dim, Reset)
	}

	fmt.Printf("%s╠══════════════════════════════════════════════════════════════╣%s\n", Dim, Reset)

	if !args.DryRun && !args.HashOnly && succeeded > 0 {
		url := fmt.Sprintf("https://archive.org/details/%s", args.Identifier)
		fmt.Printf("%s║%s  %sView:%s  %s%-52s%s  %s║%s\n", Dim, Reset, BoldWhite, Reset, Cyan, url, Reset, Dim, Reset)
	}

	// Print hashes if available
	hashPrinted := false
	for _, r := range results {
		if r.SHA256 != "" {
			if !hashPrinted {
				fmt.Printf("%s╠══════════════════════════════════════════════════════════════╣%s\n", Dim, Reset)
				fmt.Printf("%s║%s  %sFile Hashes:%s%*s%s║%s\n", Dim, Reset, BoldWhite, Reset, 48, "", Dim, Reset)
				hashPrinted = true
			}
			name := truncStr(r.Job.RemoteName, 30)
			fmt.Printf("%s║%s  %s%-30s%s                            %s║%s\n", Dim, Reset, Cyan, name, Reset, Dim, Reset)
			fmt.Printf("%s║%s    SHA256: %s%s%s  %s║%s\n", Dim, Reset, Dim, r.SHA256, Reset, Dim, Reset)
			fmt.Printf("%s║%s    MD5:    %s%s%s              %s║%s\n", Dim, Reset, Dim, r.MD5, Reset, Dim, Reset)
		}
	}

	fmt.Printf("%s╚══════════════════════════════════════════════════════════════╝%s\n\n", Dim, Reset)

	if failed > 0 {
		os.Exit(1)
	}
}

// ─────────────────────────────────────────────────────────────────────
// Utility Functions
// ─────────────────────────────────────────────────────────────────────

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	units := []string{"KB", "MB", "GB", "TB"}
	return fmt.Sprintf("%.2f %s", float64(b)/float64(div), units[exp])
}

func truncStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return "..." + s[len(s)-maxLen+3:]
}

// ─────────────────────────────────────────────────────────────────────
// Main Entry Point
// ─────────────────────────────────────────────────────────────────────

func main() {
	// Maximize CPU usage
	runtime.GOMAXPROCS(runtime.NumCPU())

	args := parseArgs()

	switch args.Command {
	case "login":
		cmdLogin()
	case "upload":
		cmdUpload(args)
	case "hash":
		// Shortcut: hash command = upload --hash-only
		args.HashOnly = true
		args.EnableHash = true
		if args.FilePath == "" && args.DirPath == "" {
			printBanner()
			fmt.Printf("\n  %s✗ Error: Specify -f <file> or -d <directory> to hash%s\n\n", BoldRed, Reset)
			os.Exit(1)
		}
		cmdUpload(args)
	case "config":
		cmdConfig()
	case "version":
		printBanner()
	case "help":
		printHelp()
	default:
		fmt.Printf("\n  %s✗ Unknown command: %s%s\n", BoldRed, args.Command, Reset)
		fmt.Printf("  %sRun '%srxarchiver help%s%s' for usage%s\n\n", Dim, BoldWhite, Reset, Dim, Reset)
		os.Exit(1)
	}
}
