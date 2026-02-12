#import <Holons/Holons.h>
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

NSString *const HOLDefaultURI = @"tcp://:9090";
static NSString *const HOLErrorDomain = @"org.organicprogramming.holons";

@implementation HOLParsedURI
@end

@implementation HOLTransportListener
@end

@implementation HOLTcpListener
@end

@implementation HOLUnixListener
@end

@implementation HOLStdioListener
@end

@implementation HOLMemListener
@end

@implementation HOLWSListener
@end

NSString *HOLScheme(NSString *uri) {
  NSRange r = [uri rangeOfString:@"://"];
  if (r.location != NSNotFound) {
    return [uri substringToIndex:r.location];
  }
  return uri;
}

static BOOL HOLSplitHostPort(NSString *addr, int defaultPort, NSString **hostOut,
                             int *portOut, NSError **error) {
  if (addr.length == 0) {
    *hostOut = @"0.0.0.0";
    *portOut = defaultPort;
    return YES;
  }

  NSRange r = [addr rangeOfString:@":" options:NSBackwardsSearch];
  if (r.location == NSNotFound) {
    *hostOut = addr;
    *portOut = defaultPort;
    return YES;
  }

  NSString *host = [addr substringToIndex:r.location];
  NSString *portText = [addr substringFromIndex:r.location + 1];
  if (host.length == 0) {
    host = @"0.0.0.0";
  }

  if (portText.length == 0) {
    *hostOut = host;
    *portOut = defaultPort;
    return YES;
  }

  NSInteger port = [portText integerValue];
  if (port <= 0 && ![portText isEqualToString:@"0"]) {
    if (error != NULL) {
      *error = [NSError errorWithDomain:HOLErrorDomain
                                   code:1
                               userInfo:@{NSLocalizedDescriptionKey :
                                              [NSString stringWithFormat:
                                                            @"invalid port: %@",
                                                            portText]}];
    }
    return NO;
  }

  *hostOut = host;
  *portOut = (int)port;
  return YES;
}

HOLParsedURI *HOLParseURI(NSString *uri) {
  NSString *s = HOLScheme(uri);
  HOLParsedURI *parsed = [HOLParsedURI new];

  if ([s isEqualToString:@"tcp"]) {
    NSString *addr = [uri hasPrefix:@"tcp://"] ? [uri substringFromIndex:6] : @"";
    NSString *host = nil;
    int port = 9090;
    HOLSplitHostPort(addr, 9090, &host, &port, NULL);
    parsed.raw = uri;
    parsed.scheme = @"tcp";
    parsed.host = host;
    parsed.port = @(port);
    parsed.path = nil;
    parsed.secure = NO;
    return parsed;
  }

  if ([s isEqualToString:@"unix"]) {
    NSString *path = [uri hasPrefix:@"unix://"] ? [uri substringFromIndex:7] : @"";
    parsed.raw = uri;
    parsed.scheme = @"unix";
    parsed.host = nil;
    parsed.port = nil;
    parsed.path = path;
    parsed.secure = NO;
    return parsed;
  }

  if ([s isEqualToString:@"stdio"]) {
    parsed.raw = @"stdio://";
    parsed.scheme = @"stdio";
    parsed.host = nil;
    parsed.port = nil;
    parsed.path = nil;
    parsed.secure = NO;
    return parsed;
  }

  if ([s isEqualToString:@"mem"]) {
    parsed.raw = [uri hasPrefix:@"mem://"] ? uri : @"mem://";
    parsed.scheme = @"mem";
    parsed.host = nil;
    parsed.port = nil;
    parsed.path = nil;
    parsed.secure = NO;
    return parsed;
  }

  if ([s isEqualToString:@"ws"] || [s isEqualToString:@"wss"]) {
    BOOL secure = [s isEqualToString:@"wss"];
    NSString *prefix = secure ? @"wss://" : @"ws://";
    NSString *trimmed = [uri hasPrefix:prefix] ? [uri substringFromIndex:prefix.length] : uri;
    NSRange slash = [trimmed rangeOfString:@"/"];
    NSString *addr = slash.location == NSNotFound ? trimmed : [trimmed substringToIndex:slash.location];
    NSString *path = slash.location == NSNotFound ? @"/grpc" : [trimmed substringFromIndex:slash.location];
    if (path.length == 0) {
      path = @"/grpc";
    }
    NSString *host = nil;
    int port = secure ? 443 : 80;
    HOLSplitHostPort(addr, secure ? 443 : 80, &host, &port, NULL);
    parsed.raw = uri;
    parsed.scheme = s;
    parsed.host = host;
    parsed.port = @(port);
    parsed.path = path;
    parsed.secure = secure;
    return parsed;
  }

  parsed.raw = uri;
  parsed.scheme = s;
  parsed.host = nil;
  parsed.port = nil;
  parsed.path = nil;
  parsed.secure = NO;
  return parsed;
}

HOLTransportListener *HOLListen(NSString *uri, NSError **error) {
  HOLParsedURI *parsed = HOLParseURI(uri);

  if ([parsed.scheme isEqualToString:@"tcp"]) {
    NSString *host = parsed.host ?: @"0.0.0.0";
    int port = parsed.port != nil ? parsed.port.intValue : 9090;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      if (error != NULL) {
        *error = [NSError errorWithDomain:HOLErrorDomain
                                     code:2
                                 userInfo:@{NSLocalizedDescriptionKey :
                                                [NSString stringWithUTF8String:strerror(errno)]}];
      }
      return nil;
    }

    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if ([host isEqualToString:@"0.0.0.0"]) {
      addr.sin_addr.s_addr = htonl(INADDR_ANY);
    } else if (inet_pton(AF_INET, host.UTF8String, &addr.sin_addr) != 1) {
      close(fd);
      if (error != NULL) {
        *error = [NSError errorWithDomain:HOLErrorDomain
                                     code:3
                                 userInfo:@{NSLocalizedDescriptionKey :
                                                [NSString stringWithFormat:
                                                              @"invalid tcp host: %@",
                                                              host]}];
      }
      return nil;
    }

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
      close(fd);
      if (error != NULL) {
        *error = [NSError errorWithDomain:HOLErrorDomain
                                     code:4
                                 userInfo:@{NSLocalizedDescriptionKey :
                                                [NSString stringWithUTF8String:strerror(errno)]}];
      }
      return nil;
    }
    if (listen(fd, 16) < 0) {
      close(fd);
      if (error != NULL) {
        *error = [NSError errorWithDomain:HOLErrorDomain
                                     code:5
                                 userInfo:@{NSLocalizedDescriptionKey :
                                                [NSString stringWithUTF8String:strerror(errno)]}];
      }
      return nil;
    }

    HOLTcpListener *lis = [HOLTcpListener new];
    lis.fd = fd;
    lis.host = host;
    lis.port = port;
    return lis;
  }

  if ([parsed.scheme isEqualToString:@"unix"]) {
    NSString *path = parsed.path ?: @"";
    if (path.length == 0) {
      if (error != NULL) {
        *error = [NSError errorWithDomain:HOLErrorDomain
                                     code:6
                                 userInfo:@{NSLocalizedDescriptionKey : @"invalid unix URI"}];
      }
      return nil;
    }

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
      if (error != NULL) {
        *error = [NSError errorWithDomain:HOLErrorDomain
                                     code:7
                                 userInfo:@{NSLocalizedDescriptionKey :
                                                [NSString stringWithUTF8String:strerror(errno)]}];
      }
      return nil;
    }

    unlink(path.UTF8String);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path.UTF8String, sizeof(addr.sun_path) - 1);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
      close(fd);
      if (error != NULL) {
        *error = [NSError errorWithDomain:HOLErrorDomain
                                     code:8
                                 userInfo:@{NSLocalizedDescriptionKey :
                                                [NSString stringWithUTF8String:strerror(errno)]}];
      }
      return nil;
    }
    if (listen(fd, 16) < 0) {
      close(fd);
      if (error != NULL) {
        *error = [NSError errorWithDomain:HOLErrorDomain
                                     code:9
                                 userInfo:@{NSLocalizedDescriptionKey :
                                                [NSString stringWithUTF8String:strerror(errno)]}];
      }
      return nil;
    }

    HOLUnixListener *lis = [HOLUnixListener new];
    lis.fd = fd;
    lis.path = path;
    return lis;
  }

  if ([parsed.scheme isEqualToString:@"stdio"]) {
    HOLStdioListener *lis = [HOLStdioListener new];
    lis.address = @"stdio://";
    return lis;
  }

  if ([parsed.scheme isEqualToString:@"mem"]) {
    HOLMemListener *lis = [HOLMemListener new];
    lis.address = @"mem://";
    return lis;
  }

  if ([parsed.scheme isEqualToString:@"ws"] || [parsed.scheme isEqualToString:@"wss"]) {
    HOLWSListener *lis = [HOLWSListener new];
    lis.host = parsed.host ?: @"0.0.0.0";
    lis.port = parsed.port != nil ? parsed.port.intValue : (parsed.secure ? 443 : 80);
    lis.path = parsed.path ?: @"/grpc";
    lis.secure = parsed.secure;
    return lis;
  }

  if (error != NULL) {
    *error = [NSError errorWithDomain:HOLErrorDomain
                                 code:10
                             userInfo:@{NSLocalizedDescriptionKey :
                                            [NSString stringWithFormat:
                                                          @"unsupported transport URI: %@",
                                                          uri]}];
  }
  return nil;
}

NSString *HOLParseFlags(NSArray<NSString *> *args) {
  for (NSUInteger i = 0; i < args.count; i++) {
    if ([args[i] isEqualToString:@"--listen"] && i + 1 < args.count) {
      return args[i + 1];
    }
    if ([args[i] isEqualToString:@"--port"] && i + 1 < args.count) {
      return [NSString stringWithFormat:@"tcp://:%@", args[i + 1]];
    }
  }
  return HOLDefaultURI;
}

void HOLCloseListener(HOLTransportListener *listener) {
  if ([listener isKindOfClass:[HOLTcpListener class]]) {
    HOLTcpListener *tcp = (HOLTcpListener *)listener;
    if (tcp.fd >= 0) {
      close(tcp.fd);
      tcp.fd = -1;
    }
    return;
  }
  if ([listener isKindOfClass:[HOLUnixListener class]]) {
    HOLUnixListener *unixLis = (HOLUnixListener *)listener;
    if (unixLis.fd >= 0) {
      close(unixLis.fd);
      unixLis.fd = -1;
    }
    if (unixLis.path.length > 0) {
      unlink(unixLis.path.UTF8String);
    }
  }
}
