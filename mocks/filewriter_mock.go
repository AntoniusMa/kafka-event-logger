package mocks

// MockLogFileWriter implements the filewriter.LogWriter interface for testing
type MockLogFileWriter struct {
	Logs        map[string][]string
	WriteErr    error
	CloseErr    error
	CloseCalled bool
}

func NewMockLogFileWriter() *MockLogFileWriter {
	return &MockLogFileWriter{
		Logs: make(map[string][]string),
	}
}

func (m *MockLogFileWriter) WriteLog(level, message string) error {
	if m.WriteErr != nil {
		return m.WriteErr
	}
	m.Logs[level] = append(m.Logs[level], message)
	return nil
}

func (m *MockLogFileWriter) Close() error {
	m.CloseCalled = true
	if m.CloseErr != nil {
		return m.CloseErr
	}
	return nil
}