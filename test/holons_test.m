#import "../include/Holons/Holons.h"
#import <Foundation/Foundation.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <string.h>
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

    NSLog(@"%d passed, %d failed", passed, failed);
    return failed > 0 ? 1 : 0;
  }
}
