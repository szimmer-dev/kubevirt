/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright The KubeVirt Authors.
 *
 */

package virtwrap

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/log"

	cmdv1 "kubevirt.io/kubevirt/pkg/handler-launcher-com/cmd/v1"
)

const (
	// openVMMSerialSocket is the console socket virt-handler connects to, as on KVM.
	openVMMSerialSocket = "virt-serial0"
	// openVMMPrivateSerialSocket is where OpenVMM itself listens for COM1. Only the launcher's relay connects to it.
	openVMMPrivateSerialSocket = "openvmm-serial0"
	openVMMSerialLogSuffix     = "-log"

	// Match the virtlogd defaults that bound virt-serial0-log on KVM.
	openVMMSerialLogMaxSize    = 2 * 1024 * 1024
	openVMMSerialLogMaxBackups = 3

	openVMMSerialDialInterval       = 10 * time.Millisecond
	openVMMSerialDialTimeout        = time.Minute
	openVMMSerialClientWriteTimeout = 5 * time.Second
	openVMMSerialBufferSize         = 4096
)

// openVMMSerialRelay reproduces libvirt's serial console behavior on top of OpenVMM.
//
// OpenVMM's socket serial backend serves a single client at a time and discards guest output while no client is
// connected, so it cannot both feed a log and serve virtctl console. The relay is therefore OpenVMM's only client: it
// appends all guest output to virt-serial0-log and fans it out to the console client attached to virt-serial0.
type openVMMSerialRelay struct {
	listener   net.Listener
	socketPath string
	targetPath string
	logWriter  *rotatingFileWriter
	done       chan struct{}
	stopOnce   sync.Once

	mutex  sync.Mutex
	vmm    net.Conn
	client net.Conn
}

func openVMMSerialSocketPath(consoleDir, uid string) string {
	return filepath.Join(consoleDir, uid, openVMMSerialSocket)
}

func openVMMPrivateSerialSocketPath(consoleDir, uid string) string {
	return filepath.Join(consoleDir, uid, openVMMPrivateSerialSocket)
}

func openVMMSerialLogEnabled(vmi *v1.VirtualMachineInstance, options *cmdv1.VirtualMachineOptions) bool {
	autoattach := vmi.Spec.Domain.Devices.AutoattachSerialConsole
	if autoattach != nil && !*autoattach {
		return false
	}
	return isSerialConsoleLogEnabled(options.GetClusterConfig().GetSerialConsoleLogDisabled(), vmi)
}

// startOpenVMMSerialRelay listens on socketPath for console clients and connects to OpenVMM at targetPath once it
// binds. Guest output is appended to logPath unless logPath is empty.
func startOpenVMMSerialRelay(socketPath, targetPath, logPath string) (*openVMMSerialRelay, error) {
	for _, path := range []string{socketPath, targetPath} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("failed to remove stale OpenVMM serial socket %s: %w", path, err)
		}
	}

	var logWriter *rotatingFileWriter
	if logPath != "" {
		var err error
		logWriter, err = openRotatingFileWriter(logPath, openVMMSerialLogMaxSize, openVMMSerialLogMaxBackups)
		if err != nil {
			return nil, fmt.Errorf("failed to open OpenVMM serial console log %s: %w", logPath, err)
		}
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		if logWriter != nil {
			_ = logWriter.Close()
		}
		return nil, fmt.Errorf("failed to listen on OpenVMM serial socket %s: %w", socketPath, err)
	}

	relay := &openVMMSerialRelay{
		listener:   listener,
		socketPath: socketPath,
		targetPath: targetPath,
		logWriter:  logWriter,
		done:       make(chan struct{}),
	}
	go relay.acceptClients()
	go relay.relayGuestOutput()
	return relay, nil
}

func (r *openVMMSerialRelay) Stop() {
	r.stopOnce.Do(func() {
		close(r.done)
		_ = r.listener.Close()
		if err := os.Remove(r.socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Log.Reason(err).Warning("failed to remove OpenVMM serial socket")
		}

		r.mutex.Lock()
		if r.client != nil {
			_ = r.client.Close()
			r.client = nil
		}
		if r.vmm != nil {
			_ = r.vmm.Close()
			r.vmm = nil
		}
		r.mutex.Unlock()

		if r.logWriter != nil {
			if err := r.logWriter.Close(); err != nil {
				log.Log.Reason(err).Warning("failed to close OpenVMM serial console log")
			}
		}
	})
}

func (r *openVMMSerialRelay) stopped() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

// acceptClients serves one console client at a time. A new client replaces the current one so that a connection
// virt-handler failed to clean up cannot lock out later consoles.
func (r *openVMMSerialRelay) acceptClients() {
	for {
		client, err := r.listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Log.Reason(err).Error("OpenVMM serial relay failed to accept a console connection")
			}
			return
		}

		r.mutex.Lock()
		if r.stopped() {
			r.mutex.Unlock()
			_ = client.Close()
			return
		}
		if r.client != nil {
			_ = r.client.Close()
		}
		r.client = client
		r.mutex.Unlock()

		go r.relayConsoleInput(client)
	}
}

func (r *openVMMSerialRelay) relayConsoleInput(client net.Conn) {
	defer r.dropClient(client)
	buffer := make([]byte, openVMMSerialBufferSize)
	for {
		n, err := client.Read(buffer)
		if n > 0 {
			r.mutex.Lock()
			vmm := r.vmm
			r.mutex.Unlock()
			// Input typed before OpenVMM accepts the relay has no guest to go to, as with a disconnected serial port.
			if vmm != nil {
				if _, writeErr := vmm.Write(buffer[:n]); writeErr != nil {
					log.Log.Reason(writeErr).Warning("OpenVMM serial relay failed to forward console input")
				}
			}
		}
		if err != nil {
			return
		}
	}
}

func (r *openVMMSerialRelay) dropClient(client net.Conn) {
	r.mutex.Lock()
	if r.client == client {
		r.client = nil
	}
	r.mutex.Unlock()
	_ = client.Close()
}

func (r *openVMMSerialRelay) connectToOpenVMM() (net.Conn, error) {
	deadline := time.Now().Add(openVMMSerialDialTimeout)
	for {
		conn, err := net.Dial("unix", r.targetPath)
		if err == nil {
			r.mutex.Lock()
			defer r.mutex.Unlock()
			if r.stopped() {
				_ = conn.Close()
				return nil, net.ErrClosed
			}
			r.vmm = conn
			return conn, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		select {
		case <-r.done:
			return nil, net.ErrClosed
		case <-time.After(openVMMSerialDialInterval):
		}
	}
}

func (r *openVMMSerialRelay) relayGuestOutput() {
	vmm, err := r.connectToOpenVMM()
	if err != nil {
		if !errors.Is(err, net.ErrClosed) {
			log.Log.Reason(err).Errorf("OpenVMM serial relay failed to connect to %s", r.targetPath)
		}
		return
	}

	buffer := make([]byte, openVMMSerialBufferSize)
	logFailed := false
	for {
		n, err := vmm.Read(buffer)
		if n > 0 {
			if r.logWriter != nil {
				if _, logErr := r.logWriter.Write(buffer[:n]); logErr != nil && !logFailed && !r.stopped() {
					logFailed = true
					log.Log.Reason(logErr).Error("failed to append to OpenVMM serial console log")
				}
			}
			r.writeToClient(buffer[:n])
		}
		if err != nil {
			return
		}
	}
}

// writeToClient bounds how long a stalled console client can delay guest output, which also feeds the log.
func (r *openVMMSerialRelay) writeToClient(data []byte) {
	r.mutex.Lock()
	client := r.client
	r.mutex.Unlock()
	if client == nil {
		return
	}
	_ = client.SetWriteDeadline(time.Now().Add(openVMMSerialClientWriteTimeout))
	if _, err := client.Write(data); err != nil {
		r.dropClient(client)
	}
}

// rotatingFileWriter appends to a file and rotates it like virtlogd: once the next write would exceed maxSize, path
// is renamed to path.0, older backups shift up by one, and backups beyond maxBackups are discarded.
type rotatingFileWriter struct {
	mutex      sync.Mutex
	path       string
	maxSize    int64
	maxBackups int
	mode       os.FileMode
	uid        int
	gid        int
	file       *os.File
	size       int64
}

func openRotatingFileWriter(path string, maxSize int64, maxBackups int) (*rotatingFileWriter, error) {
	writer := &rotatingFileWriter{path: path, maxSize: maxSize, maxBackups: maxBackups, mode: 0644, uid: -1, gid: -1}
	if err := writer.open(); err != nil {
		return nil, err
	}
	// Recreated files keep the identity virt-launcher gave the original so the guest-console-log container can read them.
	info, err := writer.file.Stat()
	if err != nil {
		_ = writer.file.Close()
		return nil, err
	}
	writer.mode = info.Mode().Perm()
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		writer.uid = int(stat.Uid)
		writer.gid = int(stat.Gid)
	}
	return writer, nil
}

func (w *rotatingFileWriter) open() error {
	file, err := os.OpenFile(w.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, w.mode)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}
	w.file = file
	w.size = info.Size()
	return nil
}

func (w *rotatingFileWriter) Write(data []byte) (int, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.file == nil {
		return 0, os.ErrClosed
	}
	if w.size > 0 && w.size+int64(len(data)) > w.maxSize {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}
	n, err := w.file.Write(data)
	w.size += int64(n)
	return n, err
}

func (w *rotatingFileWriter) rotate() error {
	if err := w.file.Close(); err != nil {
		log.Log.Reason(err).Warningf("failed to close %s before rotation", w.path)
	}
	w.file = nil
	for index := w.maxBackups - 1; index > 0; index-- {
		if err := os.Rename(w.backupPath(index-1), w.backupPath(index)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if w.maxBackups > 0 {
		if err := os.Rename(w.path, w.backupPath(0)); err != nil {
			return err
		}
	} else if err := os.Remove(w.path); err != nil {
		return err
	}
	if err := w.open(); err != nil {
		return err
	}
	if w.uid >= 0 {
		if err := w.file.Chown(w.uid, w.gid); err != nil && !errors.Is(err, os.ErrPermission) {
			log.Log.Reason(err).Warningf("failed to restore ownership of %s after rotation", w.path)
		}
	}
	return nil
}

func (w *rotatingFileWriter) backupPath(index int) string {
	return w.path + "." + strconv.Itoa(index)
}

func (w *rotatingFileWriter) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}
