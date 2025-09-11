package filewriter

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const dateFormat = "2006-01-02"

type fileInfo struct {
	file  *os.File
	mutex sync.Mutex
}

type LogFileWriter struct {
	basePath string
	files    map[string]*fileInfo
	mapMutex sync.RWMutex
}

func NewLogFileWriter(basePath string) *LogFileWriter {
	return &LogFileWriter{
		basePath: basePath,
		files:    make(map[string]*fileInfo),
	}
}

func (lfw *LogFileWriter) WriteLog(level, message string) error {
	filename := lfw.getFilename(level, time.Now())

	lfw.mapMutex.Lock()
	fileWithMutex, exists := lfw.files[filename]
	if !exists {
		file, err := lfw.createLogFile(filename)
		if err != nil {
			lfw.mapMutex.Unlock()
			return fmt.Errorf("failed to create log file %s: %w", filename, err)
		}
		fileWithMutex = &fileInfo{file: file}
		lfw.files[filename] = fileWithMutex
	}
	lfw.mapMutex.Unlock()

	fileWithMutex.mutex.Lock()
	defer fileWithMutex.mutex.Unlock()

	_, err := fmt.Fprintf(fileWithMutex.file, "%s\n", message)
	if err != nil {
		return fmt.Errorf("failed to write to log file %s: %w", filename, err)
	}

	return fileWithMutex.file.Sync()
}

func (lfw *LogFileWriter) getFilename(level string, timestamp time.Time) string {
	date := timestamp.Format(dateFormat)
	return filepath.Join(lfw.basePath, fmt.Sprintf("%s_%s.log", level, date))
}

func (lfw *LogFileWriter) createLogFile(filename string) (*os.File, error) {
	dir := filepath.Dir(filename)
	// Create directory with 0755 (rwxr-xr-x) - owner: read/write/execute, group/others: read/execute
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Open file with 0644 (rw-r--r--) - owner: read/write, group/others: read-only
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filename, err)
	}

	return file, nil
}

func (lfw *LogFileWriter) Close() error {
	lfw.mapMutex.Lock()
	defer lfw.mapMutex.Unlock()

	var errs []error
	for filename, fileInfo := range lfw.files {
		if err := fileInfo.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close file %s: %w", filename, err))
		}
	}

	lfw.files = make(map[string]*fileInfo)

	if len(errs) > 0 {
		return fmt.Errorf("errors closing files: %v", errs)
	}

	return nil
}
