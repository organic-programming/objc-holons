#import "../include/Holons/Holons.h"
#import <Foundation/Foundation.h>
#include <arpa/inet.h>
#include <signal.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <unistd.h>

static int passed = 0;
static int failed = 0;

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

int main(int argc, const char *argv[]) {
  @autoreleasepool {
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

    HOLTransportListener *stdioAny = HOLListen(@"stdio://", &error);
    assert_true([stdioAny isKindOfClass:[HOLStdioListener class]], @"stdio variant");
    HOLConnection *stdioConn = HOLAccept(stdioAny, &error);
    assert_true(stdioConn != nil && error == nil, @"stdio first accept");
    HOLCloseConnection(stdioConn);
    HOLConnection *stdioAgain = HOLAccept(stdioAny, &error);
    assert_true(stdioAgain == nil, @"stdio second accept fails");
    assert_true(error != nil, @"stdio second accept error");

    HOLTransportListener *memAny = HOLListen(@"mem://objc-test", &error);
    assert_true([memAny isKindOfClass:[HOLMemListener class]], @"mem variant");
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

    HOLTransportListener *wsAny = HOLListen(@"ws://127.0.0.1:8080/holon", &error);
    assert_true([wsAny isKindOfClass:[HOLWSListener class]], @"ws variant");
    HOLWSListener *ws = (HOLWSListener *)wsAny;
    assert_eq(@"127.0.0.1", ws.host, @"ws host");
    assert_true(ws.port == 8080, @"ws port");
    assert_eq(@"/holon", ws.path, @"ws path");
    assert_true(!ws.secure, @"ws secure");

    HOLConnection *wsConn = HOLAccept(wsAny, &error);
    assert_true(wsConn == nil, @"ws accept unsupported");
    assert_true(error != nil, @"ws accept error");

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
    HOLHolonIdentity *noFM = HOLParseHolon(noFMPath, &error);
    assert_true(noFM == nil, @"missing frontmatter fails");
    assert_true(error != nil, @"missing frontmatter error");
    [[NSFileManager defaultManager] removeItemAtPath:noFMPath error:nil];

    // Holon-RPC interop
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

    NSLog(@"%d passed, %d failed", passed, failed);
    return failed > 0 ? 1 : 0;
  }
}
