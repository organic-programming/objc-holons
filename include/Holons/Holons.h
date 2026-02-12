#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

/// Default transport URI when --listen is omitted.
extern NSString *const HOLDefaultURI;

@interface HOLParsedURI : NSObject

@property(nonatomic, copy) NSString *raw;
@property(nonatomic, copy) NSString *scheme;
@property(nonatomic, copy, nullable) NSString *host;
@property(nonatomic, strong, nullable) NSNumber *port;
@property(nonatomic, copy, nullable) NSString *path;
@property(nonatomic, assign) BOOL secure;

@end

@interface HOLTransportListener : NSObject
@end

@interface HOLTcpListener : HOLTransportListener
@property(nonatomic, assign) int fd;
@property(nonatomic, copy) NSString *host;
@property(nonatomic, assign) int port;
@end

@interface HOLUnixListener : HOLTransportListener
@property(nonatomic, assign) int fd;
@property(nonatomic, copy) NSString *path;
@end

@interface HOLStdioListener : HOLTransportListener
@property(nonatomic, copy) NSString *address;
@end

@interface HOLMemListener : HOLTransportListener
@property(nonatomic, copy) NSString *address;
@end

@interface HOLWSListener : HOLTransportListener
@property(nonatomic, copy) NSString *host;
@property(nonatomic, assign) int port;
@property(nonatomic, copy) NSString *path;
@property(nonatomic, assign) BOOL secure;
@end

/// Extract the scheme from a transport URI.
NSString *HOLScheme(NSString *uri);

/// Parse a transport URI into a normalized structure.
HOLParsedURI *HOLParseURI(NSString *uri);

/// Parse a transport URI and return a listener variant.
HOLTransportListener *_Nullable HOLListen(NSString *uri, NSError *_Nullable *_Nullable error);

/// Parse --listen or --port from command-line args.
NSString *HOLParseFlags(NSArray<NSString *> *args);

/// Close any open descriptors associated with a listener.
void HOLCloseListener(HOLTransportListener *listener);

NS_ASSUME_NONNULL_END
