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

@implementation HOLConnection
@end

@implementation HOLHolonIdentity
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

static NSError *HOLMakeError(NSInteger code, NSString *message) {
  return [NSError errorWithDomain:HOLErrorDomain
                             code:code
                         userInfo:@{NSLocalizedDescriptionKey : message}];
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
    parsed.path = parsed.raw.length > 6 ? [parsed.raw substringFromIndex:6] : @"";
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
    lis.consumed = NO;
    return lis;
  }

  if ([parsed.scheme isEqualToString:@"mem"]) {
    int fds[2] = {-1, -1};
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
      if (error != NULL) {
        *error = HOLMakeError(11, [NSString stringWithUTF8String:strerror(errno)]);
      }
      return nil;
    }
    HOLMemListener *lis = [HOLMemListener new];
    lis.address = parsed.raw ?: @"mem://";
    lis.serverFD = fds[0];
    lis.clientFD = fds[1];
    lis.serverConsumed = NO;
    lis.clientConsumed = NO;
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

HOLConnection *HOLAccept(HOLTransportListener *listener, NSError **error) {
  if ([listener isKindOfClass:[HOLTcpListener class]]) {
    HOLTcpListener *tcp = (HOLTcpListener *)listener;
    int fd = accept(tcp.fd, NULL, NULL);
    if (fd < 0) {
      if (error != NULL) {
        *error = HOLMakeError(12, [NSString stringWithUTF8String:strerror(errno)]);
      }
      return nil;
    }
    HOLConnection *conn = [HOLConnection new];
    conn.readFD = fd;
    conn.writeFD = fd;
    conn.scheme = @"tcp";
    conn.ownsReadFD = YES;
    conn.ownsWriteFD = YES;
    return conn;
  }

  if ([listener isKindOfClass:[HOLUnixListener class]]) {
    HOLUnixListener *unixLis = (HOLUnixListener *)listener;
    int fd = accept(unixLis.fd, NULL, NULL);
    if (fd < 0) {
      if (error != NULL) {
        *error = HOLMakeError(13, [NSString stringWithUTF8String:strerror(errno)]);
      }
      return nil;
    }
    HOLConnection *conn = [HOLConnection new];
    conn.readFD = fd;
    conn.writeFD = fd;
    conn.scheme = @"unix";
    conn.ownsReadFD = YES;
    conn.ownsWriteFD = YES;
    return conn;
  }

  if ([listener isKindOfClass:[HOLStdioListener class]]) {
    HOLStdioListener *stdioLis = (HOLStdioListener *)listener;
    if (stdioLis.consumed) {
      if (error != NULL) {
        *error = HOLMakeError(14, @"stdio:// accepts exactly one connection");
      }
      return nil;
    }
    stdioLis.consumed = YES;
    HOLConnection *conn = [HOLConnection new];
    conn.readFD = STDIN_FILENO;
    conn.writeFD = STDOUT_FILENO;
    conn.scheme = @"stdio";
    conn.ownsReadFD = NO;
    conn.ownsWriteFD = NO;
    return conn;
  }

  if ([listener isKindOfClass:[HOLMemListener class]]) {
    HOLMemListener *memLis = (HOLMemListener *)listener;
    if (memLis.serverConsumed || memLis.serverFD < 0) {
      if (error != NULL) {
        *error = HOLMakeError(15, @"mem:// server side already consumed");
      }
      return nil;
    }
    memLis.serverConsumed = YES;
    int fd = memLis.serverFD;
    memLis.serverFD = -1;

    HOLConnection *conn = [HOLConnection new];
    conn.readFD = fd;
    conn.writeFD = fd;
    conn.scheme = @"mem";
    conn.ownsReadFD = YES;
    conn.ownsWriteFD = YES;
    return conn;
  }

  if ([listener isKindOfClass:[HOLWSListener class]]) {
    if (error != NULL) {
      *error = HOLMakeError(16, @"ws/wss runtime accept is unsupported (metadata-only listener)");
    }
    return nil;
  }

  if (error != NULL) {
    *error = HOLMakeError(17, @"unsupported listener type");
  }
  return nil;
}

HOLConnection *HOLMemDial(HOLTransportListener *listener, NSError **error) {
  if (![listener isKindOfClass:[HOLMemListener class]]) {
    if (error != NULL) {
      *error = HOLMakeError(18, @"HOLMemDial requires a mem:// listener");
    }
    return nil;
  }

  HOLMemListener *memLis = (HOLMemListener *)listener;
  if (memLis.clientConsumed || memLis.clientFD < 0) {
    if (error != NULL) {
      *error = HOLMakeError(19, @"mem:// client side already consumed");
    }
    return nil;
  }

  memLis.clientConsumed = YES;
  int fd = memLis.clientFD;
  memLis.clientFD = -1;

  HOLConnection *conn = [HOLConnection new];
  conn.readFD = fd;
  conn.writeFD = fd;
  conn.scheme = @"mem";
  conn.ownsReadFD = YES;
  conn.ownsWriteFD = YES;
  return conn;
}

ssize_t HOLConnectionRead(HOLConnection *connection, void *buffer, size_t count) {
  return read(connection.readFD, buffer, count);
}

ssize_t HOLConnectionWrite(HOLConnection *connection, const void *buffer, size_t count) {
  return write(connection.writeFD, buffer, count);
}

void HOLCloseConnection(HOLConnection *connection) {
  int readFD = connection.readFD;
  int writeFD = connection.writeFD;

  if (connection.ownsReadFD && readFD >= 0) {
    close(readFD);
  }
  if (connection.ownsWriteFD && writeFD >= 0 && writeFD != readFD) {
    close(writeFD);
  }

  connection.readFD = -1;
  connection.writeFD = -1;
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

static NSString *HOLYAMLValue(NSString *line) {
  NSRange colon = [line rangeOfString:@":"];
  if (colon.location == NSNotFound) {
    return @"";
  }

  NSString *value = [line substringFromIndex:colon.location + 1];
  value = [value stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]];
  if (value.length >= 2 && [value hasPrefix:@"\""] && [value hasSuffix:@"\""]) {
    value = [value substringWithRange:NSMakeRange(1, value.length - 2)];
  }
  return value;
}

static NSArray<NSString *> *HOLYAMLList(NSString *value) {
  NSString *trimmed = [value stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]];
  if (![trimmed hasPrefix:@"["] || ![trimmed hasSuffix:@"]"]) {
    return @[];
  }

  if (trimmed.length <= 2) {
    return @[];
  }

  NSString *inner = [trimmed substringWithRange:NSMakeRange(1, trimmed.length - 2)];
  NSArray<NSString *> *parts = [inner componentsSeparatedByString:@","];
  NSMutableArray<NSString *> *items = [NSMutableArray arrayWithCapacity:parts.count];
  for (NSString *part in parts) {
    NSString *item = [part stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]];
    if (item.length >= 2 && [item hasPrefix:@"\""] && [item hasSuffix:@"\""]) {
      item = [item substringWithRange:NSMakeRange(1, item.length - 2)];
    }
    if (item.length > 0) {
      [items addObject:item];
    }
  }
  return items;
}

HOLHolonIdentity *HOLParseHolon(NSString *path, NSError **error) {
  NSError *readError = nil;
  NSString *text = [NSString stringWithContentsOfFile:path
                                             encoding:NSUTF8StringEncoding
                                                error:&readError];
  if (text == nil) {
    if (error != NULL) {
      *error = readError;
    }
    return nil;
  }

  if (![text hasPrefix:@"---"]) {
    if (error != NULL) {
      *error = HOLMakeError(20, [NSString stringWithFormat:@"%@: missing YAML frontmatter", path]);
    }
    return nil;
  }

  NSRange end = [text rangeOfString:@"---" options:0 range:NSMakeRange(3, text.length - 3)];
  if (end.location == NSNotFound) {
    if (error != NULL) {
      *error = HOLMakeError(21, [NSString stringWithFormat:@"%@: unterminated frontmatter", path]);
    }
    return nil;
  }

  NSString *frontmatter = [text substringWithRange:NSMakeRange(3, end.location - 3)];
  NSArray<NSString *> *lines = [frontmatter componentsSeparatedByCharactersInSet:[NSCharacterSet newlineCharacterSet]];

  HOLHolonIdentity *identity = [HOLHolonIdentity new];
  identity.uuid = @"";
  identity.givenName = @"";
  identity.familyName = @"";
  identity.motto = @"";
  identity.composer = @"";
  identity.clade = @"";
  identity.status = @"";
  identity.born = @"";
  identity.lang = @"";
  identity.parents = @[];
  identity.reproduction = @"";
  identity.generatedBy = @"";
  identity.protoStatus = @"";
  identity.aliases = @[];

  for (NSString *rawLine in lines) {
    NSString *line = [rawLine stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]];
    if (line.length == 0 || [line hasPrefix:@"#"]) {
      continue;
    }

    if ([line hasPrefix:@"uuid:"]) {
      identity.uuid = HOLYAMLValue(line);
    } else if ([line hasPrefix:@"given_name:"]) {
      identity.givenName = HOLYAMLValue(line);
    } else if ([line hasPrefix:@"family_name:"]) {
      identity.familyName = HOLYAMLValue(line);
    } else if ([line hasPrefix:@"motto:"]) {
      identity.motto = HOLYAMLValue(line);
    } else if ([line hasPrefix:@"composer:"]) {
      identity.composer = HOLYAMLValue(line);
    } else if ([line hasPrefix:@"clade:"]) {
      identity.clade = HOLYAMLValue(line);
    } else if ([line hasPrefix:@"status:"]) {
      identity.status = HOLYAMLValue(line);
    } else if ([line hasPrefix:@"born:"]) {
      identity.born = HOLYAMLValue(line);
    } else if ([line hasPrefix:@"lang:"]) {
      identity.lang = HOLYAMLValue(line);
    } else if ([line hasPrefix:@"reproduction:"]) {
      identity.reproduction = HOLYAMLValue(line);
    } else if ([line hasPrefix:@"generated_by:"]) {
      identity.generatedBy = HOLYAMLValue(line);
    } else if ([line hasPrefix:@"proto_status:"]) {
      identity.protoStatus = HOLYAMLValue(line);
    } else if ([line hasPrefix:@"parents:"]) {
      identity.parents = HOLYAMLList(HOLYAMLValue(line));
    } else if ([line hasPrefix:@"aliases:"]) {
      identity.aliases = HOLYAMLList(HOLYAMLValue(line));
    }
  }

  return identity;
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
    return;
  }
  if ([listener isKindOfClass:[HOLMemListener class]]) {
    HOLMemListener *memLis = (HOLMemListener *)listener;
    if (memLis.serverFD >= 0) {
      close(memLis.serverFD);
      memLis.serverFD = -1;
    }
    if (memLis.clientFD >= 0) {
      close(memLis.clientFD);
      memLis.clientFD = -1;
    }
  }
}
