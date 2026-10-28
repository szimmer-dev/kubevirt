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
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	cmdv1 "kubevirt.io/kubevirt/pkg/handler-launcher-com/cmd/v1"
	"kubevirt.io/kubevirt/pkg/libvmi"
	"kubevirt.io/kubevirt/pkg/pointer"
)

var _ = Describe("OpenVMM serial relay", func() {
	type paths struct {
		socket string
		target string
		log    string
	}

	newPaths := func() paths {
		dir := GinkgoT().TempDir()
		return paths{
			socket: filepath.Join(dir, openVMMSerialSocket),
			target: filepath.Join(dir, openVMMPrivateSerialSocket),
			log:    filepath.Join(dir, openVMMSerialSocket+openVMMSerialLogSuffix),
		}
	}

	// fakeOpenVMM stands in for OpenVMM's COM1 listener and returns the relay's connection once it dials in.
	fakeOpenVMM := func(target string) <-chan net.Conn {
		listener, err := net.Listen("unix", target)
		Expect(err).ToNot(HaveOccurred(), "the fake OpenVMM serial listener should bind its socket")
		DeferCleanup(func() { _ = listener.Close() })
		accepted := make(chan net.Conn, 1)
		go func() {
			conn, acceptErr := listener.Accept()
			if acceptErr == nil {
				accepted <- conn
			}
		}()
		return accepted
	}

	startRelay := func(p paths, logPath string) (*openVMMSerialRelay, net.Conn) {
		relay, err := startOpenVMMSerialRelay(p.socket, p.target, logPath)
		Expect(err).ToNot(HaveOccurred(), "the relay should start listening for console clients")
		DeferCleanup(relay.Stop)
		// Binding after the relay starts mirrors OpenVMM coming up after the launcher and exercises the dial retry.
		accepted := fakeOpenVMM(p.target)
		var vmm net.Conn
		Eventually(accepted).Should(Receive(&vmm), "the relay should connect to OpenVMM's private serial socket")
		DeferCleanup(func() { _ = vmm.Close() })
		Eventually(func() bool {
			relay.mutex.Lock()
			defer relay.mutex.Unlock()
			return relay.vmm != nil
		}).Should(BeTrue(), "the relay should be ready to forward console input once connected to OpenVMM")
		return relay, vmm
	}

	readLog := func(path string) func() string {
		return func() string {
			contents, _ := os.ReadFile(path)
			return string(contents)
		}
	}

	expectRead := func(conn net.Conn, expected string, description string) {
		Expect(conn.SetReadDeadline(time.Now().Add(5*time.Second))).To(Succeed(), "the connection should accept a read deadline")
		received := make([]byte, len(expected))
		_, err := io.ReadFull(conn, received)
		Expect(err).ToNot(HaveOccurred(), description)
		Expect(string(received)).To(Equal(expected), description)
	}

	// Console input reaching OpenVMM proves the relay has registered the client, so later guest output is routed to it.
	attachConsole := func(p paths, vmm net.Conn, probe string) net.Conn {
		client, err := net.Dial("unix", p.socket)
		Expect(err).ToNot(HaveOccurred(), "a console client should be able to connect to virt-serial0")
		DeferCleanup(func() { _ = client.Close() })
		_, err = client.Write([]byte(probe))
		Expect(err).ToNot(HaveOccurred(), "the console client should be able to send input")
		expectRead(vmm, probe, "console input should be forwarded to the guest's serial port")
		return client
	}

	It("logs guest output whether or not a console is attached and relays console I/O", func() {
		p := newPaths()
		_, vmm := startRelay(p, p.log)

		By("logging output while no console is attached")
		_, err := vmm.Write([]byte("firmware\n"))
		Expect(err).ToNot(HaveOccurred(), "the fake guest should emit serial output")
		Eventually(readLog(p.log)).Should(Equal("firmware\n"),
			"guest output must reach virt-serial0-log even with no console client, as virtlogd does on KVM")

		By("relaying output and input for an attached console")
		client := attachConsole(p, vmm, "login\n")
		_, err = vmm.Write([]byte("prompt$ "))
		Expect(err).ToNot(HaveOccurred(), "the fake guest should emit serial output")
		expectRead(client, "prompt$ ", "an attached console should receive guest output")
		Eventually(readLog(p.log)).Should(Equal("firmware\nprompt$ "),
			"output shown on the console should also be logged")

		By("continuing to log after the console disconnects")
		Expect(client.Close()).To(Succeed(), "the console client should disconnect cleanly")
		_, err = vmm.Write([]byte("shutdown\n"))
		Expect(err).ToNot(HaveOccurred(), "the fake guest should emit serial output")
		Eventually(readLog(p.log)).Should(Equal("firmware\nprompt$ shutdown\n"),
			"logging should be unaffected by a console disconnecting")
	})

	It("serves the console without writing a log when serial console logging is disabled", func() {
		p := newPaths()
		_, vmm := startRelay(p, "")

		client := attachConsole(p, vmm, "x")
		_, err := vmm.Write([]byte("boot\n"))
		Expect(err).ToNot(HaveOccurred(), "the fake guest should emit serial output")
		expectRead(client, "boot\n", "the console should still work with logging disabled")

		_, err = os.Stat(p.log)
		Expect(err).To(MatchError(os.ErrNotExist), "no virt-serial0-log should be created when logging is disabled")
	})

	It("replaces an existing console client with a newly connected one", func() {
		p := newPaths()
		_, vmm := startRelay(p, "")

		first := attachConsole(p, vmm, "1")
		second := attachConsole(p, vmm, "2")

		Expect(first.SetReadDeadline(time.Now().Add(5*time.Second))).To(Succeed(), "the first client should accept a deadline")
		_, err := first.Read(make([]byte, 1))
		Expect(err).To(MatchError(io.EOF), "the replaced console client should be disconnected")

		_, err = vmm.Write([]byte("hello"))
		Expect(err).ToNot(HaveOccurred(), "the fake guest should emit serial output")
		expectRead(second, "hello", "guest output should go to the newest console client")
	})

	It("disconnects clients and removes virt-serial0 when stopped", func() {
		p := newPaths()
		relay, vmm := startRelay(p, p.log)
		client := attachConsole(p, vmm, "x")

		relay.Stop()

		Expect(client.SetReadDeadline(time.Now().Add(5*time.Second))).To(Succeed(), "the client should accept a deadline")
		_, err := client.Read(make([]byte, 1))
		Expect(err).To(MatchError(io.EOF), "stopping the relay should disconnect the console client")
		Expect(vmm.SetReadDeadline(time.Now().Add(5*time.Second))).To(Succeed(), "the fake OpenVMM side should accept a deadline")
		_, err = vmm.Read(make([]byte, 1))
		Expect(err).To(MatchError(io.EOF), "stopping the relay should release OpenVMM's serial connection")
		_, err = os.Stat(p.socket)
		Expect(err).To(MatchError(os.ErrNotExist), "stopping the relay should remove the virt-serial0 socket")
	})

	It("stops waiting for OpenVMM when stopped before OpenVMM binds its socket", func() {
		p := newPaths()
		relay, err := startOpenVMMSerialRelay(p.socket, p.target, p.log)
		Expect(err).ToNot(HaveOccurred(), "the relay should start before OpenVMM has bound its serial socket")

		relay.Stop()
		time.Sleep(5 * openVMMSerialDialInterval)

		listener, err := net.Listen("unix", p.target)
		Expect(err).ToNot(HaveOccurred(), "the fake OpenVMM serial listener should bind its socket")
		defer listener.Close()
		Expect(listener.(*net.UnixListener).SetDeadline(time.Now().Add(200*time.Millisecond))).To(Succeed(),
			"the fake OpenVMM listener should accept a deadline")
		_, err = listener.Accept()
		Expect(err).To(HaveOccurred(), "a stopped relay must not keep dialing OpenVMM's serial socket")
	})

	It("rotates the log like virtlogd, keeping a bounded number of backups", func() {
		path := filepath.Join(GinkgoT().TempDir(), "virt-serial0-log")
		Expect(os.WriteFile(path, []byte("old\n"), 0644)).To(Succeed(), "seeding a pre-existing log should succeed")
		writer, err := openRotatingFileWriter(path, 10, 3)
		Expect(err).ToNot(HaveOccurred(), "the writer should open an existing log for appending")
		defer writer.Close()

		// Each 8-byte write overflows the 10-byte cap, including the first, which lands on the pre-existing 4 bytes.
		for _, chunk := range []string{"aaaaaaa\n", "bbbbbbb\n", "ccccccc\n", "ddddddd\n", "eeeeeee\n"} {
			_, err := writer.Write([]byte(chunk))
			Expect(err).ToNot(HaveOccurred(), "writing %q should rotate and append without error", chunk)
		}

		Expect(readLog(path)()).To(Equal("eeeeeee\n"), "the live log should hold only the newest output")
		Expect(readLog(path+".0")()).To(Equal("ddddddd\n"), ".0 should be the most recent backup")
		Expect(readLog(path+".1")()).To(Equal("ccccccc\n"), ".1 should be the second most recent backup")
		Expect(readLog(path+".2")()).To(Equal("bbbbbbb\n"), ".2 should be the oldest retained backup")
		_, err = os.Stat(path + ".3")
		Expect(err).To(MatchError(os.ErrNotExist), "backups beyond the configured count should be discarded")
	})

	DescribeTable("enables the serial console log under the same conditions as KVM",
		func(autoattach *bool, vmiLog *bool, clusterDisabled bool, expected bool) {
			vmi := libvmi.New()
			vmi.Spec.Domain.Devices.AutoattachSerialConsole = autoattach
			vmi.Spec.Domain.Devices.LogSerialConsole = vmiLog
			options := &cmdv1.VirtualMachineOptions{
				ClusterConfig: &cmdv1.ClusterConfig{SerialConsoleLogDisabled: clusterDisabled},
			}

			Expect(openVMMSerialLogEnabled(vmi, options)).To(Equal(expected),
				"autoattach=%v logSerialConsole=%v clusterDisabled=%v", autoattach, vmiLog, clusterDisabled)
		},
		Entry("by default", nil, nil, false, true),
		Entry("not when the cluster disables it", nil, nil, true, false),
		Entry("when the VMI requests it despite the cluster default", nil, pointer.P(true), true, true),
		Entry("not when the VMI opts out", nil, pointer.P(false), false, false),
		Entry("not when no serial console is auto-attached", pointer.P(false), pointer.P(true), false, false),
	)

	It("enables the serial console log when no launcher options are provided", func() {
		Expect(openVMMSerialLogEnabled(libvmi.New(), nil)).To(BeTrue(),
			"missing cluster config should fall back to KubeVirt's default of logging the serial console")
	})
})
