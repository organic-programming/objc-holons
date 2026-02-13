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
@property(nonatomic, assign) BOOL consumed;
@end

@interface HOLMemListener : HOLTransportListener
@property(nonatomic, copy) NSString *address;
@property(nonatomic, assign) int serverFD;
@property(nonatomic, assign) int clientFD;
@property(nonatomic, assign) BOOL serverConsumed;
@property(nonatomic, assign) BOOL clientConsumed;
@end

@interface HOLWSListener : HOLTransportListener
@property(nonatomic, copy) NSString *host;
@property(nonatomic, assign) int port;
@property(nonatomic, copy) NSString *path;
@property(nonatomic, assign) BOOL secure;
@end

@interface HOLConnection : NSObject
@property(nonatomic, assign) int readFD;
@property(nonatomic, assign) int writeFD;
@property(nonatomic, copy) NSString *scheme;
@property(nonatomic, assign) BOOL ownsReadFD;
@property(nonatomic, assign) BOOL ownsWriteFD;
@end

typedef NSDictionary<NSString *, id> *_Nonnull (^HOLHolonRPCHandler)(
    NSDictionary<NSString *, id> *_Nonnull params);

@interface HOLHolonRPCClient : NSObject <NSURLSessionWebSocketDelegate, NSURLSessionTaskDelegate>

- (instancetype)init;

- (instancetype)initWithHeartbeatIntervalMS:(NSInteger)heartbeatIntervalMS
                         heartbeatTimeoutMS:(NSInteger)heartbeatTimeoutMS
                        reconnectMinDelayMS:(NSInteger)reconnectMinDelayMS
                        reconnectMaxDelayMS:(NSInteger)reconnectMaxDelayMS
                            reconnectFactor:(double)reconnectFactor
                            reconnectJitter:(double)reconnectJitter
                           connectTimeoutMS:(NSInteger)connectTimeoutMS
                           requestTimeoutMS:(NSInteger)requestTimeoutMS NS_DESIGNATED_INITIALIZER;

- (BOOL)connect:(NSString *)url error:(NSError *_Nullable *_Nullable)error;

- (nullable NSDictionary<NSString *, id> *)invoke:(NSString *)method
                                            params:
                                                (nullable NSDictionary<NSString *, id> *)params
                                           timeout:(NSTimeInterval)timeout
                                             error:(NSError *_Nullable *_Nullable)error;

- (nullable NSDictionary<NSString *, id> *)invoke:(NSString *)method
                                            params:
                                                (nullable NSDictionary<NSString *, id> *)params
                                             error:(NSError *_Nullable *_Nullable)error;

- (void)registerMethod:(NSString *)method handler:(HOLHolonRPCHandler)handler;

- (void)close;

@end

@interface HOLHolonIdentity : NSObject
@property(nonatomic, copy) NSString *uuid;
@property(nonatomic, copy) NSString *givenName;
@property(nonatomic, copy) NSString *familyName;
@property(nonatomic, copy) NSString *motto;
@property(nonatomic, copy) NSString *composer;
@property(nonatomic, copy) NSString *clade;
@property(nonatomic, copy) NSString *status;
@property(nonatomic, copy) NSString *born;
@property(nonatomic, copy) NSString *lang;
@property(nonatomic, copy) NSArray<NSString *> *parents;
@property(nonatomic, copy) NSString *reproduction;
@property(nonatomic, copy) NSString *generatedBy;
@property(nonatomic, copy) NSString *protoStatus;
@property(nonatomic, copy) NSArray<NSString *> *aliases;
@end

/// Extract the scheme from a transport URI.
NSString *HOLScheme(NSString *uri);

/// Parse a transport URI into a normalized structure.
HOLParsedURI *HOLParseURI(NSString *uri);

/// Parse a transport URI and return a listener variant.
HOLTransportListener *_Nullable HOLListen(NSString *uri, NSError *_Nullable *_Nullable error);

/// Accept one runtime connection from a listener.
HOLConnection *_Nullable HOLAccept(HOLTransportListener *listener,
                                   NSError *_Nullable *_Nullable error);

/// Dial the client side of a `mem://` listener.
HOLConnection *_Nullable HOLMemDial(HOLTransportListener *listener,
                                    NSError *_Nullable *_Nullable error);

/// Read bytes from a runtime connection.
ssize_t HOLConnectionRead(HOLConnection *connection, void *buffer, size_t count);

/// Write bytes to a runtime connection.
ssize_t HOLConnectionWrite(HOLConnection *connection, const void *buffer, size_t count);

/// Close file descriptors held by a runtime connection.
void HOLCloseConnection(HOLConnection *connection);

/// Parse --listen or --port from command-line args.
NSString *HOLParseFlags(NSArray<NSString *> *args);

/// Parse HOLON.md identity YAML frontmatter.
HOLHolonIdentity *_Nullable HOLParseHolon(NSString *path,
                                          NSError *_Nullable *_Nullable error);

/// Close any open descriptors associated with a listener.
void HOLCloseListener(HOLTransportListener *listener);

NS_ASSUME_NONNULL_END
