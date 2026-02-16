#import "../include/Holons/Holons.h"
#import <Foundation/Foundation.h>
#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <unistd.h>

static int passed = 0;
static int failed = 0;
static int skipped = 0;

static void assert_eq(NSString *expected, NSString *actual, NSString *label) {
  if ([expected isEqualToString:actual]) {
    passed++;
  } else {
    failed++;
    NSLog(@"FAIL: %@: expected %@, got %@", label, expected, actual);
  }
}

static void assert_true(BOOL condition, NSString *label) {
  if (condition) {
    passed++;
  } else {
    failed++;
    NSLog(@"FAIL: %@", label);
  }
}

static void skip_test(NSString *label) {
  skipped++;
  NSLog(@"SKIP: %@", label);
}

static NSString *read_file_text(NSString *path) {
  NSError *error = nil;
  NSString *text = [NSString stringWithContentsOfFile:path
                                             encoding:NSUTF8StringEncoding
                                                error:&error];
  if (text == nil) {
    return @"";
  }
  return text;
}

static int command_exit_code(const char *cmd) {
  int status = system(cmd);
  if (status == -1 || !WIFEXITED(status)) {
    return -1;
  }
  return WEXITSTATUS(status);
}

static int run_bash_script(NSString *script) {
  if (script.length == 0) {
    return -1;
  }

  char scriptTemplate[] = "/tmp/holons_objc_script_XXXXXX";
  int scriptFD = mkstemp(scriptTemplate);
  if (scriptFD < 0) {
    return -1;
  }

  FILE *scriptFile = fdopen(scriptFD, "w");
  if (scriptFile == NULL) {
    close(scriptFD);
    unlink(scriptTemplate);
    return -1;
  }

  const char *raw = script.UTF8String;
  size_t length = strlen(raw);
  size_t wrote = fwrite(raw, 1, length, scriptFile);
  int closeRC = fclose(scriptFile);
  if (wrote != length || closeRC != 0) {
    unlink(scriptTemplate);
    return -1;
  }

  if (chmod(scriptTemplate, 0700) != 0) {
    unlink(scriptTemplate);
    return -1;
  }

  NSString *command = [NSString stringWithFormat:@"/bin/bash %s", scriptTemplate];
  int rc = command_exit_code(command.UTF8String);
  unlink(scriptTemplate);
  return rc;
}

static NSString *env_string(NSString *name) {
  const char *value = getenv(name.UTF8String);
  if (value == NULL) {
    return nil;
  }
  return [NSString stringWithUTF8String:value];
}

static void restore_env(NSString *name, NSString *value) {
  if (value.length > 0) {
    setenv(name.UTF8String, value.UTF8String, 1);
  } else {
    unsetenv(name.UTF8String);
  }
}

static BOOL loopback_bind_allowed(NSString **reasonOut) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    if (reasonOut != NULL) {
      *reasonOut = [NSString stringWithFormat:@"socket failed: %s", strerror(errno)];
    }
    return NO;
  }

  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t)0);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
    int bindErrno = errno;
    close(fd);
    if (reasonOut != NULL) {
      *reasonOut = [NSString stringWithFormat:@"bind failed: %s", strerror(bindErrno)];
    }
    return NO;
  }

  close(fd);
  if (reasonOut != NULL) {
    *reasonOut = @"";
  }
  return YES;
}

static void test_certification_contract(void) {
  NSString *raw = read_file_text(@"cert.json");
  assert_true(raw.length > 0, @"read cert.json");
  if (raw.length == 0) {
    return;
  }

  NSData *rawData = [raw dataUsingEncoding:NSUTF8StringEncoding];
  assert_true(rawData != nil, @"cert json data");
  if (rawData == nil) {
    return;
  }

  NSError *jsonError = nil;
  NSDictionary<NSString *, id> *cert = [NSJSONSerialization JSONObjectWithData:rawData
                                                                        options:0
                                                                          error:&jsonError];
  assert_true(cert != nil && jsonError == nil, @"parse cert.json");
  if (cert == nil || jsonError != nil) {
    return;
  }

  NSDictionary<NSString *, id> *executables = cert[@"executables"];
  assert_true([executables isKindOfClass:[NSDictionary class]], @"cert executables object");
  if (![executables isKindOfClass:[NSDictionary class]]) {
    return;
  }
  assert_eq(@"./bin/echo-server", executables[@"echo_server"], @"cert echo_server declaration");
  assert_eq(@"./bin/echo-client", executables[@"echo_client"], @"cert echo_client declaration");
  assert_eq(@"./bin/holon-rpc-server", executables[@"holon_rpc_server"],
            @"cert holon_rpc_server declaration");

  NSDictionary<NSString *, id> *capabilities = cert[@"capabilities"];
  assert_true([capabilities isKindOfClass:[NSDictionary class]], @"cert capabilities object");
  if (![capabilities isKindOfClass:[NSDictionary class]]) {
    return;
  }

  NSNumber *dialTCP = capabilities[@"grpc_dial_tcp"];
  NSNumber *dialStdio = capabilities[@"grpc_dial_stdio"];
  NSNumber *dialUnix = capabilities[@"grpc_dial_unix"];
  NSNumber *dialWS = capabilities[@"grpc_dial_ws"];
  NSNumber *holonRPCServer = capabilities[@"holon_rpc_server"];
  assert_true([dialTCP isKindOfClass:[NSNumber class]] && [dialTCP boolValue],
              @"cert grpc_dial_tcp declaration");
  assert_true([dialStdio isKindOfClass:[NSNumber class]] && [dialStdio boolValue],
              @"cert grpc_dial_stdio declaration");
  assert_true([dialUnix isKindOfClass:[NSNumber class]] && [dialUnix boolValue],
              @"cert grpc_dial_unix declaration");
  assert_true([dialWS isKindOfClass:[NSNumber class]] && [dialWS boolValue],
              @"cert grpc_dial_ws declaration");
  assert_true([holonRPCServer isKindOfClass:[NSNumber class]] && [holonRPCServer boolValue],
              @"cert holon_rpc_server declaration");
}

static void test_echo_wrapper_scripts_exist(void) {
  assert_true(access("./bin/echo-client", F_OK) == 0, @"echo-client script exists");
  assert_true(access("./bin/echo-server", F_OK) == 0, @"echo-server script exists");
  assert_true(access("./bin/holon-rpc-server", F_OK) == 0, @"holon-rpc-server script exists");
  assert_true(access("./bin/echo-client", X_OK) == 0, @"echo-client script executable");
  assert_true(access("./bin/echo-server", X_OK) == 0, @"echo-server script executable");
  assert_true(access("./bin/holon-rpc-server", X_OK) == 0, @"holon-rpc-server script executable");
}

static void test_echo_wrapper_invocation(void) {
  char fakeGoTemplate[] = "/tmp/holons_objc_fake_go_XXXXXX";
  char fakeLogTemplate[] = "/tmp/holons_objc_fake_go_log_XXXXXX";
  int fakeFD = mkstemp(fakeGoTemplate);
  assert_true(fakeFD >= 0, @"mkstemp fake go binary");
  if (fakeFD < 0) {
    return;
  }

  int logFD = mkstemp(fakeLogTemplate);
  assert_true(logFD >= 0, @"mkstemp fake go log");
  if (logFD < 0) {
    close(fakeFD);
    unlink(fakeGoTemplate);
    return;
  }

  FILE *script = fdopen(fakeFD, "w");
  assert_true(script != NULL, @"fdopen fake go binary");
  if (script == NULL) {
    close(fakeFD);
    close(logFD);
    unlink(fakeGoTemplate);
    unlink(fakeLogTemplate);
    return;
  }

  fprintf(script,
          "#!/usr/bin/env bash\n"
          "set -euo pipefail\n"
          ": \"${HOLONS_FAKE_GO_LOG:?missing HOLONS_FAKE_GO_LOG}\"\n"
          "{\n"
          "  printf 'PWD=%%s\\n' \"$PWD\"\n"
          "  i=0\n"
          "  for arg in \"$@\"; do\n"
          "    printf 'ARG%%d=%%s\\n' \"$i\" \"$arg\"\n"
          "    i=$((i+1))\n"
          "  done\n"
          "} >\"$HOLONS_FAKE_GO_LOG\"\n");
  fclose(script);
  close(logFD);

  assert_true(chmod(fakeGoTemplate, 0700) == 0, @"chmod fake go binary");

  NSString *prevGoBin = env_string(@"GO_BIN");
  NSString *prevFakeLog = env_string(@"HOLONS_FAKE_GO_LOG");
  NSString *prevGOCache = env_string(@"GOCACHE");

  setenv("GO_BIN", fakeGoTemplate, 1);
  setenv("HOLONS_FAKE_GO_LOG", fakeLogTemplate, 1);
  unsetenv("GOCACHE");

  int clientExit = command_exit_code("./bin/echo-client --message cert-stdio stdio:// >/dev/null 2>&1");
  assert_true(clientExit == 0, @"echo-client wrapper exit");

  NSString *logPath = [NSString stringWithUTF8String:fakeLogTemplate];
  NSString *capture = read_file_text(logPath);
  assert_true(capture.length > 0, @"read echo-client wrapper capture");
  if (capture.length > 0) {
    assert_true([capture containsString:@"PWD="] && [capture containsString:@"/sdk/go-holons"],
                @"echo-client wrapper cwd");
    assert_true([capture containsString:@"ARG0=run"], @"echo-client wrapper uses go run");
    assert_true([capture containsString:@"echo-client-go/main.go"], @"echo-client wrapper helper path");
    assert_true([capture containsString:@"--sdk"] && [capture containsString:@"objc-holons"],
                @"echo-client wrapper sdk default");
    assert_true([capture containsString:@"--server-sdk"] && [capture containsString:@"go-holons"],
                @"echo-client wrapper server sdk default");
    assert_true([capture containsString:@"stdio://"], @"echo-client wrapper forwards uri");
    assert_true([capture containsString:@"--message"] && [capture containsString:@"cert-stdio"],
                @"echo-client wrapper forwards message");
  }

  int serverExit = command_exit_code("./bin/echo-server --listen stdio:// >/dev/null 2>&1");
  assert_true(serverExit == 0, @"echo-server wrapper exit");
  capture = read_file_text(logPath);
  assert_true(capture.length > 0, @"read echo-server wrapper capture");
  if (capture.length > 0) {
    assert_true([capture containsString:@"PWD="] && [capture containsString:@"/sdk/go-holons"],
                @"echo-server wrapper cwd");
    assert_true([capture containsString:@"ARG0=run"], @"echo-server wrapper uses go run");
    assert_true([capture containsString:@"echo-server-go/main.go"], @"echo-server wrapper helper path");
    assert_true([capture containsString:@"--sdk"] && [capture containsString:@"objc-holons"],
                @"echo-server wrapper sdk default");
    assert_true([capture containsString:@"--listen"] && [capture containsString:@"stdio://"],
                @"echo-server wrapper forwards listen URI");
  }

  serverExit =
      command_exit_code("./bin/echo-server --sleep-ms 250 --listen stdio:// >/dev/null 2>&1");
  assert_true(serverExit == 0, @"echo-server wrapper sleep flag exit");
  capture = read_file_text(logPath);
  assert_true(capture.length > 0, @"read echo-server wrapper sleep capture");
  if (capture.length > 0) {
    assert_true([capture containsString:@"--sleep-ms"] && [capture containsString:@"250"],
                @"echo-server wrapper forwards sleep-ms");
  }

  serverExit = command_exit_code("./bin/echo-server serve --listen stdio:// >/dev/null 2>&1");
  assert_true(serverExit == 0, @"echo-server wrapper serve exit");
  capture = read_file_text(logPath);
  assert_true(capture.length > 0, @"read echo-server wrapper serve capture");
  if (capture.length > 0) {
    assert_true([capture containsString:@"ARG2=serve"], @"echo-server wrapper preserves serve token");
    assert_true([capture containsString:@"ARG3=--listen"] && [capture containsString:@"ARG4=stdio://"],
                @"echo-server wrapper preserves serve listen flags");
    assert_true([capture containsString:@"--sdk"] && [capture containsString:@"objc-holons"],
                @"echo-server wrapper serve sdk default");
  }

  int holonRPCExit = command_exit_code(
      "./bin/holon-rpc-server ws://127.0.0.1:8080/rpc >/dev/null 2>&1");
  assert_true(holonRPCExit == 0, @"holon-rpc-server wrapper exit");
  capture = read_file_text(logPath);
  assert_true(capture.length > 0, @"read holon-rpc-server wrapper capture");
  if (capture.length > 0) {
    assert_true([capture containsString:@"PWD="] && [capture containsString:@"/sdk/go-holons"],
                @"holon-rpc-server wrapper cwd");
    assert_true([capture containsString:@"ARG0=run"], @"holon-rpc-server wrapper uses go run");
    assert_true([capture containsString:@"holon-rpc-server-go/main.go"],
                @"holon-rpc-server wrapper helper path");
    assert_true([capture containsString:@"--sdk"] && [capture containsString:@"objc-holons"],
                @"holon-rpc-server wrapper sdk default");
    assert_true([capture containsString:@"ws://127.0.0.1:8080/rpc"],
                @"holon-rpc-server wrapper forwards listen URL");
  }

  unlink(fakeGoTemplate);
  unlink(fakeLogTemplate);
  restore_env(@"GO_BIN", prevGoBin);
  restore_env(@"HOLONS_FAKE_GO_LOG", prevFakeLog);
  restore_env(@"GOCACHE", prevGOCache);
}

static int connect_tcp(const char *host, int port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return -1;
  }

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t)port);
  if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
    close(fd);
    return -1;
  }

  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
    close(fd);
    return -1;
  }

  return fd;
}

static NSString *resolve_go_binary(void) {
  NSString *preferred = @"/Users/bpds/go/go1.25.1/bin/go";
  if ([[NSFileManager defaultManager] isExecutableFileAtPath:preferred]) {
    return preferred;
  }
  return @"go";
}

static NSString *find_sdk_dir(void) {
  NSFileManager *fm = [NSFileManager defaultManager];
  NSString *dir = [[fm currentDirectoryPath] copy];

  for (NSInteger i = 0; i < 12; i++) {
    NSString *candidate = [dir stringByAppendingPathComponent:@"go-holons"];
    BOOL isDir = NO;
    if ([fm fileExistsAtPath:candidate isDirectory:&isDir] && isDir) {
      return dir;
    }
    dir = [dir stringByDeletingLastPathComponent];
    if (dir.length == 0 || [dir isEqualToString:@"/"]) {
      break;
    }
  }

  return nil;
}

static NSString *read_line_with_timeout(NSFileHandle *handle, NSTimeInterval timeout) {
  dispatch_semaphore_t sem = dispatch_semaphore_create(0);
  __block NSString *line = nil;

  dispatch_async(dispatch_get_global_queue(QOS_CLASS_UTILITY, 0), ^{
    NSMutableData *data = [NSMutableData data];
    while (YES) {
      NSData *chunk = [handle readDataOfLength:1];
      if (chunk.length == 0) {
        break;
      }
      uint8_t byte = ((const uint8_t *)chunk.bytes)[0];
      if (byte == '\n') {
        break;
      }
      [data appendData:chunk];
    }
    if (data.length > 0) {
      line = [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding];
    }
    dispatch_semaphore_signal(sem);
  });

  long rc = dispatch_semaphore_wait(
      sem, dispatch_time(DISPATCH_TIME_NOW, (int64_t)(timeout * NSEC_PER_SEC)));
  if (rc != 0) {
    return nil;
  }
  return line;
}

static NSDictionary *invoke_eventually(HOLHolonRPCClient *client, NSString *method,
                                       NSDictionary *params) {
  for (NSInteger i = 0; i < 40; i++) {
    NSError *error = nil;
    NSDictionary *out = [client invoke:method params:params timeout:10000 error:&error];
    if (out != nil && error == nil) {
      return out;
    }
    [NSThread sleepForTimeInterval:0.12];
  }
  return nil;
}

static BOOL with_go_helper(NSString *mode, void (^block)(NSString *url)) {
  NSFileManager *fm = [NSFileManager defaultManager];
  NSString *sdkDir = find_sdk_dir();
  if (sdkDir.length == 0) {
    NSLog(@"FAIL: unable to locate sdk directory containing go-holons");
    return NO;
  }
  NSString *goDir = [sdkDir stringByAppendingPathComponent:@"go-holons"];
  NSString *fixturePath = @"test/fixtures/go_holonrpc_helper.go";

  NSData *fixtureData = [NSData dataWithContentsOfFile:fixturePath];
  if (fixtureData == nil) {
    NSLog(@"FAIL: missing fixture %@", fixturePath);
    return NO;
  }

  NSString *helperPath = [goDir stringByAppendingPathComponent:
                                    [NSString stringWithFormat:@"tmp-holonrpc-%@.go",
                                                               [[NSUUID UUID] UUIDString]]];
  if (![fixtureData writeToFile:helperPath atomically:YES]) {
    NSLog(@"FAIL: unable to write helper file %@", helperPath);
    return NO;
  }

  NSPipe *stdoutPipe = [NSPipe pipe];
  NSPipe *stderrPipe = [NSPipe pipe];

  NSTask *task = [NSTask new];
  task.launchPath = resolve_go_binary();
  task.arguments = @[ @"run", helperPath, mode ];
  task.currentDirectoryPath = goDir;
  task.standardOutput = stdoutPipe;
  task.standardError = stderrPipe;
  NSMutableDictionary *env = [[[NSProcessInfo processInfo] environment] mutableCopy];
  if ([env[@"GOCACHE"] length] == 0) {
    env[@"GOCACHE"] = @"/tmp/go-cache-objc-tests";
  }
  task.environment = env;

  @try {
    [task launch];
  } @catch (NSException *exception) {
    NSLog(@"FAIL: unable to launch go helper: %@", exception.reason);
    [fm removeItemAtPath:helperPath error:nil];
    return NO;
  }

  NSString *url = read_line_with_timeout(stdoutPipe.fileHandleForReading, 20.0);
  if (url.length == 0) {
    NSData *stderrData = [stderrPipe.fileHandleForReading readDataToEndOfFile];
    NSString *stderrText =
        [[NSString alloc] initWithData:stderrData encoding:NSUTF8StringEncoding];
    NSLog(@"FAIL: Go helper did not output URL: %@", stderrText ?: @"");
    if (task.isRunning) {
      [task terminate];
    }
    [task waitUntilExit];
    [fm removeItemAtPath:helperPath error:nil];
    return NO;
  }

  block(url);

  if (task.isRunning) {
    [task terminate];
  }
  [task waitUntilExit];
  [fm removeItemAtPath:helperPath error:nil];
  return YES;
}

static BOOL with_local_holonrpc_server(void (^block)(NSString *url)) {
  NSPipe *stdoutPipe = [NSPipe pipe];
  NSPipe *stderrPipe = [NSPipe pipe];

  NSTask *task = [NSTask new];
  task.launchPath = @"./bin/holon-rpc-server";
  task.arguments = @[ @"ws://127.0.0.1:0/rpc" ];
  task.standardOutput = stdoutPipe;
  task.standardError = stderrPipe;

  NSMutableDictionary *env = [[[NSProcessInfo processInfo] environment] mutableCopy];
  if ([env[@"GOCACHE"] length] == 0) {
    env[@"GOCACHE"] = @"/tmp/go-cache-objc-tests";
  }
  task.environment = env;

  @try {
    [task launch];
  } @catch (NSException *exception) {
    NSLog(@"FAIL: unable to launch local holon-rpc server: %@", exception.reason);
    return NO;
  }

  NSString *rawURL = read_line_with_timeout(stdoutPipe.fileHandleForReading, 20.0);
  NSString *url = [rawURL stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceAndNewlineCharacterSet]];
  if (url.length == 0) {
    NSData *stderrData = [stderrPipe.fileHandleForReading readDataToEndOfFile];
    NSString *stderrText =
        [[NSString alloc] initWithData:stderrData encoding:NSUTF8StringEncoding];
    NSLog(@"FAIL: local holon-rpc server did not output URL: %@", stderrText ?: @"");
    if (task.isRunning) {
      [task terminate];
    }
    [task waitUntilExit];
    return NO;
  }

  block(url);

  if (task.isRunning) {
    [task terminate];
  }
  [task waitUntilExit];
  return YES;
}

int main(int argc, const char *argv[]) {
  @autoreleasepool {
    test_certification_contract();
    test_echo_wrapper_scripts_exist();
    test_echo_wrapper_invocation();

    NSString *bindReason = nil;
    BOOL canBindLoopback = loopback_bind_allowed(&bindReason);

    // Transport
    assert_eq(@"tcp", HOLScheme(@"tcp://:9090"), @"scheme tcp");
    assert_eq(@"unix", HOLScheme(@"unix:///tmp/x.sock"), @"scheme unix");
    assert_eq(@"stdio", HOLScheme(@"stdio://"), @"scheme stdio");
    assert_eq(@"mem", HOLScheme(@"mem://"), @"scheme mem");
    assert_eq(@"ws", HOLScheme(@"ws://127.0.0.1:8080/grpc"), @"scheme ws");
    assert_eq(@"wss", HOLScheme(@"wss://example.com:443/grpc"), @"scheme wss");
    assert_eq(@"tcp://:9090", HOLDefaultURI, @"default uri");

    HOLParsedURI *wssParsed = HOLParseURI(@"wss://example.com:8443");
    assert_eq(@"wss", wssParsed.scheme, @"parseUri scheme");
    assert_eq(@"example.com", wssParsed.host, @"parseUri host");
    assert_true(wssParsed.port.intValue == 8443, @"parseUri port");
    assert_eq(@"/grpc", wssParsed.path, @"parseUri path");
    assert_true(wssParsed.secure, @"parseUri secure");

    NSError *error = nil;
    if (canBindLoopback) {
      HOLTransportListener *tcpAny = HOLListen(@"tcp://127.0.0.1:0", &error);
      assert_true(tcpAny != nil && error == nil, @"listen tcp succeeds");
      assert_true([tcpAny isKindOfClass:[HOLTcpListener class]], @"listen tcp variant");
      HOLTcpListener *tcp = (HOLTcpListener *)tcpAny;

      struct sockaddr_in addr;
      socklen_t len = sizeof(addr);
      memset(&addr, 0, sizeof(addr));
      int rc = getsockname(tcp.fd, (struct sockaddr *)&addr, &len);
      assert_true(rc == 0, @"tcp getsockname");
      int port = (int)ntohs(addr.sin_port);
      assert_true(port > 0, @"tcp bound port");

      int clientFD = connect_tcp("127.0.0.1", port);
      assert_true(clientFD >= 0, @"tcp client connect");

      error = nil;
      HOLConnection *tcpConn = HOLAccept(tcp, &error);
      assert_true(tcpConn != nil && error == nil, @"tcp accept");
      assert_eq(@"tcp", tcpConn.scheme, @"tcp connection scheme");

      const char *ping = "ping";
      ssize_t wrote = write(clientFD, ping, 4);
      assert_true(wrote == 4, @"tcp client write");

      char buf[8] = {0};
      ssize_t readCount = HOLConnectionRead(tcpConn, buf, sizeof(buf));
      assert_true(readCount == 4, @"tcp server read count");
      assert_true(strncmp(buf, "ping", 4) == 0, @"tcp payload");

      HOLCloseConnection(tcpConn);
      close(clientFD);
      HOLCloseListener(tcp);
    } else {
      skip_test([NSString stringWithFormat:@"tcp transport checks skipped (%@)",
                                           bindReason.length > 0 ? bindReason
                                                                 : @"loopback bind unavailable"]);
    }

    error = nil;
    HOLTransportListener *stdioAny = HOLListen(@"stdio://", &error);
    assert_true(stdioAny != nil && [stdioAny isKindOfClass:[HOLStdioListener class]], @"stdio variant");
    error = nil;
    HOLConnection *stdioConn = HOLAccept(stdioAny, &error);
    assert_true(stdioConn != nil && error == nil, @"stdio first accept");
    HOLCloseConnection(stdioConn);
    error = nil;
    HOLConnection *stdioAgain = HOLAccept(stdioAny, &error);
    assert_true(stdioAgain == nil, @"stdio second accept fails");
    assert_true(error != nil, @"stdio second accept error");

    error = nil;
    HOLTransportListener *memAny = HOLListen(@"mem://objc-test", &error);
    assert_true(memAny != nil && [memAny isKindOfClass:[HOLMemListener class]], @"mem variant");
    error = nil;
    HOLConnection *memClient = HOLMemDial(memAny, &error);
    HOLConnection *memServer = HOLAccept(memAny, &error);
    assert_true(memClient != nil && memServer != nil && error == nil, @"mem dial+accept");

    const char *memMsg = "mem";
    ssize_t memWrite = HOLConnectionWrite(memClient, memMsg, 3);
    assert_true(memWrite == 3, @"mem write");

    char memBuf[8] = {0};
    ssize_t memRead = HOLConnectionRead(memServer, memBuf, sizeof(memBuf));
    assert_true(memRead == 3, @"mem read");
    assert_true(strncmp(memBuf, "mem", 3) == 0, @"mem payload");

    HOLCloseConnection(memServer);
    HOLCloseConnection(memClient);
    HOLCloseListener(memAny);

    error = nil;
    HOLTransportListener *wsAny = HOLListen(@"ws://127.0.0.1:8080/holon", &error);
    assert_true(wsAny != nil && [wsAny isKindOfClass:[HOLWSListener class]], @"ws variant");
    HOLWSListener *ws = (HOLWSListener *)wsAny;
    assert_eq(@"127.0.0.1", ws.host, @"ws host");
    assert_true(ws.port == 8080, @"ws port");
    assert_eq(@"/holon", ws.path, @"ws path");
    assert_true(!ws.secure, @"ws secure");

    error = nil;
    HOLConnection *wsConn = HOLAccept(wsAny, &error);
    assert_true(wsConn == nil, @"ws accept unsupported");
    assert_true(error != nil, @"ws accept error");

    error = nil;
    HOLTransportListener *bad = HOLListen(@"ftp://host", &error);
    assert_true(bad == nil, @"unsupported uri fails");
    assert_true(error != nil, @"unsupported uri error");

    // Serve
    assert_eq(@"tcp://:8080", HOLParseFlags(@[ @"--listen", @"tcp://:8080" ]),
              @"parseFlags --listen");
    assert_eq(@"tcp://:3000", HOLParseFlags(@[ @"--port", @"3000" ]),
              @"parseFlags --port");
    assert_eq(HOLDefaultURI, HOLParseFlags(@[]), @"parseFlags default");

    // Identity
    NSString *tmpDir = NSTemporaryDirectory();
    NSString *path = [tmpDir stringByAppendingPathComponent:[NSString stringWithFormat:@"objc_holon_%@.md", [[NSUUID UUID] UUIDString]]];
    NSString *content =
        @"---\n"
         "uuid: \"abc-123\"\n"
         "given_name: \"objc-holon\"\n"
         "family_name: \"Test\"\n"
         "lang: \"objc\"\n"
         "parents: [\"a\", \"b\"]\n"
         "generated_by: \"sophia-who\"\n"
         "proto_status: draft\n"
         "aliases: [\"o1\"]\n"
         "---\n"
         "# Holon\n";
    [content writeToFile:path atomically:YES encoding:NSUTF8StringEncoding error:nil];

    error = nil;
    HOLHolonIdentity *idn = HOLParseHolon(path, &error);
    assert_true(idn != nil && error == nil, @"parse holon success");
    assert_eq(@"abc-123", idn.uuid, @"identity uuid");
    assert_eq(@"objc-holon", idn.givenName, @"identity given_name");
    assert_eq(@"objc", idn.lang, @"identity lang");
    assert_true(idn.parents.count == 2, @"identity parents");
    assert_eq(@"sophia-who", idn.generatedBy, @"identity generated_by");
    assert_eq(@"draft", idn.protoStatus, @"identity proto_status");
    assert_true(idn.aliases.count == 1, @"identity aliases");
    [[NSFileManager defaultManager] removeItemAtPath:path error:nil];

    NSString *noFMPath = [tmpDir stringByAppendingPathComponent:[NSString stringWithFormat:@"objc_holon_no_fm_%@.md", [[NSUUID UUID] UUIDString]]];
    [@"# No frontmatter\n" writeToFile:noFMPath atomically:YES encoding:NSUTF8StringEncoding error:nil];
    error = nil;
    HOLHolonIdentity *noFM = HOLParseHolon(noFMPath, &error);
    assert_true(noFM == nil, @"missing frontmatter fails");
    assert_true(error != nil, @"missing frontmatter error");
    [[NSFileManager defaultManager] removeItemAtPath:noFMPath error:nil];

    // Certification runtime transport checks
    if (canBindLoopback) {
      int memExit =
          command_exit_code("./bin/echo-client --message cert-mem mem:// >/dev/null 2>&1");
      assert_true(memExit == 0, @"echo-client mem runtime");

      int wsExit = command_exit_code(
          "./bin/echo-client --server-sdk objc-holons --message cert-ws "
          "ws://127.0.0.1:0/grpc >/dev/null 2>&1");
      assert_true(wsExit == 0, @"echo-client ws runtime");

      NSString *timeoutScript =
          @"set -euo pipefail\n"
           "cleanup() {\n"
           "  if [ -n \"${S_PID:-}\" ] && kill -0 \"$S_PID\" >/dev/null 2>&1; then\n"
           "    kill -TERM \"$S_PID\" >/dev/null 2>&1 || true\n"
           "    wait \"$S_PID\" >/dev/null 2>&1 || true\n"
           "  fi\n"
           "  rm -f \"${S_OUT:-}\" \"${S_ERR:-}\" \"${TIME_OUT:-}\" \"${TIME_ERR:-}\"\n"
           "}\n"
           "trap cleanup EXIT\n"
           "S_OUT=$(mktemp)\n"
           "S_ERR=$(mktemp)\n"
           "./bin/echo-server --sleep-ms 1800 --listen tcp://127.0.0.1:0 >\"$S_OUT\" 2>\"$S_ERR\" &\n"
           "S_PID=$!\n"
           "ADDR=\"\"\n"
           "for _ in $(seq 1 120); do\n"
           "  if [ -s \"$S_OUT\" ]; then\n"
           "    ADDR=$(head -n1 \"$S_OUT\" | tr -d '\\r\\n')\n"
           "    if [ -n \"$ADDR\" ]; then break; fi\n"
           "  fi\n"
           "  sleep 0.05\n"
           "done\n"
           "[ -n \"$ADDR\" ]\n"
           "TIME_OUT=$(mktemp)\n"
           "TIME_ERR=$(mktemp)\n"
           "set +e\n"
           "./bin/echo-client --server-sdk objc-holons --timeout-ms 500 --message cert-l5-timeout \"$ADDR\" >\"$TIME_OUT\" 2>\"$TIME_ERR\"\n"
           "TIME_RC=$?\n"
           "set -e\n"
           "[ \"$TIME_RC\" -ne 0 ]\n"
           "grep -Eiq 'DeadlineExceeded|deadline exceeded' \"$TIME_ERR\"\n"
           "./bin/echo-client --server-sdk objc-holons --timeout-ms 5000 --message cert-l5-timeout-followup \"$ADDR\" >/dev/null 2>&1\n"
           "kill -TERM \"$S_PID\" >/dev/null 2>&1 || true\n"
           "wait \"$S_PID\" >/dev/null 2>&1 || true\n";
      int timeoutProbe = run_bash_script(timeoutScript);
      assert_true(timeoutProbe == 0, @"echo timeout propagation runtime");
    } else {
      skip_test([NSString stringWithFormat:@"cert runtime transport checks skipped (%@)",
                                           bindReason.length > 0 ? bindReason
                                                                 : @"loopback bind unavailable"]);
    }

    // Holon-RPC interop
    if (canBindLoopback) {
      BOOL rpcLocalServer = with_local_holonrpc_server(^(NSString *url) {
        HOLHolonRPCClient *client = [[HOLHolonRPCClient alloc]
            initWithHeartbeatIntervalMS:250
                     heartbeatTimeoutMS:250
                    reconnectMinDelayMS:100
                    reconnectMaxDelayMS:400
                        reconnectFactor:2.0
                        reconnectJitter:0.1
                       connectTimeoutMS:10000
                       requestTimeoutMS:10000];

        [client registerMethod:@"client.v1.Client/Hello"
                       handler:^NSDictionary *_Nonnull(NSDictionary<NSString *,id> *_Nonnull params) {
                         NSString *name = [params[@"name"] isKindOfClass:[NSString class]]
                                              ? params[@"name"]
                                              : @"";
                         return @{ @"message" : [NSString stringWithFormat:@"hello %@", name] };
                       }];

        NSError *rpcError = nil;
        BOOL ok = [client connect:url error:&rpcError];
        assert_true(ok && rpcError == nil, @"holon-rpc local server connect");

        NSDictionary *ping = [client invoke:@"echo.v1.Echo/Ping"
                                     params:@{ @"message" : @"cert-l3-holonrpc" }
                                      error:&rpcError];
        assert_true(ping != nil && rpcError == nil, @"holon-rpc local server ping");
        assert_eq(@"cert-l3-holonrpc", ping[@"message"], @"holon-rpc local server ping result");
        assert_eq(@"objc-holons", ping[@"sdk"], @"holon-rpc local server sdk");

        NSDictionary *callClient =
            invoke_eventually(client, @"echo.v1.Echo/CallClient", @{});
        assert_true(callClient != nil, @"holon-rpc local server call-client");
        assert_eq(@"hello objc", callClient[@"message"],
                  @"holon-rpc local server call-client result");
        [client close];
      });
      assert_true(rpcLocalServer, @"holon-rpc local server wrapper");

      BOOL rpcEcho = with_go_helper(@"echo", ^(NSString *url) {
        HOLHolonRPCClient *client = [[HOLHolonRPCClient alloc]
            initWithHeartbeatIntervalMS:250
                     heartbeatTimeoutMS:250
                    reconnectMinDelayMS:100
                    reconnectMaxDelayMS:400
                        reconnectFactor:2.0
                        reconnectJitter:0.1
                       connectTimeoutMS:10000
                       requestTimeoutMS:10000];
        NSError *rpcError = nil;
        BOOL ok = [client connect:url error:&rpcError];
        assert_true(ok && rpcError == nil, @"holon-rpc connect echo");

        NSDictionary *out = [client invoke:@"echo.v1.Echo/Ping"
                                    params:@{ @"message" : @"hello" }
                                     error:&rpcError];
        assert_true(out != nil && rpcError == nil, @"holon-rpc invoke echo");
        assert_eq(@"hello", out[@"message"], @"holon-rpc echo result");
        [client close];
      });
      assert_true(rpcEcho, @"holon-rpc helper echo");

      BOOL rpcCallClient = with_go_helper(@"echo", ^(NSString *url) {
        HOLHolonRPCClient *client = [[HOLHolonRPCClient alloc]
            initWithHeartbeatIntervalMS:250
                     heartbeatTimeoutMS:250
                    reconnectMinDelayMS:100
                    reconnectMaxDelayMS:400
                        reconnectFactor:2.0
                        reconnectJitter:0.1
                       connectTimeoutMS:10000
                       requestTimeoutMS:10000];

        [client registerMethod:@"client.v1.Client/Hello"
                       handler:^NSDictionary *_Nonnull(NSDictionary<NSString *,id> *_Nonnull params) {
                         NSString *name = [params[@"name"] isKindOfClass:[NSString class]]
                                              ? params[@"name"]
                                              : @"";
                         return @{ @"message" : [NSString stringWithFormat:@"hello %@", name] };
                       }];

        NSError *rpcError = nil;
        BOOL ok = [client connect:url error:&rpcError];
        assert_true(ok && rpcError == nil, @"holon-rpc connect call-client");

        NSDictionary *out = [client invoke:@"echo.v1.Echo/CallClient"
                                    params:@{}
                                     error:&rpcError];
        assert_true(out != nil && rpcError == nil, @"holon-rpc invoke call-client");
        assert_eq(@"hello go", out[@"message"], @"holon-rpc call-client result");
        [client close];
      });
      assert_true(rpcCallClient, @"holon-rpc helper call-client");

      BOOL rpcReconnect = with_go_helper(@"drop-once", ^(NSString *url) {
        HOLHolonRPCClient *client = [[HOLHolonRPCClient alloc]
            initWithHeartbeatIntervalMS:200
                     heartbeatTimeoutMS:200
                    reconnectMinDelayMS:100
                    reconnectMaxDelayMS:400
                        reconnectFactor:2.0
                        reconnectJitter:0.1
                       connectTimeoutMS:10000
                       requestTimeoutMS:10000];

        NSError *rpcError = nil;
        BOOL ok = [client connect:url error:&rpcError];
        assert_true(ok && rpcError == nil, @"holon-rpc connect drop-once");

        NSDictionary *first = [client invoke:@"echo.v1.Echo/Ping"
                                      params:@{ @"message" : @"first" }
                                       error:&rpcError];
        assert_true(first != nil && rpcError == nil, @"holon-rpc first ping");
        assert_eq(@"first", first[@"message"], @"holon-rpc first payload");

        [NSThread sleepForTimeInterval:0.7];

        NSDictionary *second = invoke_eventually(client, @"echo.v1.Echo/Ping",
                                                 @{ @"message" : @"second" });
        assert_true(second != nil, @"holon-rpc second ping");
        assert_eq(@"second", second[@"message"], @"holon-rpc second payload");

        NSDictionary *hb = invoke_eventually(client, @"echo.v1.Echo/HeartbeatCount", @{});
        NSNumber *count = [hb[@"count"] isKindOfClass:[NSNumber class]] ? hb[@"count"] : @(0);
        assert_true(count.integerValue >= 1, @"holon-rpc heartbeat count");
        [client close];
      });
      assert_true(rpcReconnect, @"holon-rpc helper reconnect");
    } else {
      skip_test([NSString stringWithFormat:@"holon-rpc interop checks skipped (%@)",
                                           bindReason.length > 0 ? bindReason
                                                                 : @"loopback bind unavailable"]);
    }

    NSLog(@"%d passed, %d failed, %d skipped", passed, failed, skipped);
    return failed > 0 ? 1 : 0;
  }
}
