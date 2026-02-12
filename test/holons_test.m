#import "../include/Holons/Holons.h"
#import <Foundation/Foundation.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>

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
    assert_true(ntohs(addr.sin_port) > 0, @"tcp bound port");
    HOLCloseListener(tcp);

    HOLTransportListener *stdioAny = HOLListen(@"stdio://", &error);
    HOLTransportListener *memAny = HOLListen(@"mem://", &error);
    assert_true([stdioAny isKindOfClass:[HOLStdioListener class]], @"stdio variant");
    assert_true([memAny isKindOfClass:[HOLMemListener class]], @"mem variant");
    assert_eq(@"stdio://", ((HOLStdioListener *)stdioAny).address, @"stdio address");
    assert_eq(@"mem://", ((HOLMemListener *)memAny).address, @"mem address");

    HOLTransportListener *wsAny = HOLListen(@"ws://127.0.0.1:8080/holon", &error);
    assert_true([wsAny isKindOfClass:[HOLWSListener class]], @"ws variant");
    HOLWSListener *ws = (HOLWSListener *)wsAny;
    assert_eq(@"127.0.0.1", ws.host, @"ws host");
    assert_true(ws.port == 8080, @"ws port");
    assert_eq(@"/holon", ws.path, @"ws path");
    assert_true(!ws.secure, @"ws secure");

    HOLTransportListener *bad = HOLListen(@"ftp://host", &error);
    assert_true(bad == nil, @"unsupported uri fails");
    assert_true(error != nil, @"unsupported uri error");

    // Serve
    assert_eq(@"tcp://:8080", HOLParseFlags(@[ @"--listen", @"tcp://:8080" ]),
              @"parseFlags --listen");
    assert_eq(@"tcp://:3000", HOLParseFlags(@[ @"--port", @"3000" ]),
              @"parseFlags --port");
    assert_eq(HOLDefaultURI, HOLParseFlags(@[]), @"parseFlags default");

    NSLog(@"%d passed, %d failed", passed, failed);
    return failed > 0 ? 1 : 0;
  }
}
