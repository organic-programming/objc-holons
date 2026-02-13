#import <Holons/Holons.h>
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <math.h>

NSString *const HOLDefaultURI = @"tcp://:9090";
static NSString *const HOLErrorDomain = @"org.organicprogramming.holons";
static NSError *HOLMakeError(NSInteger code, NSString *message);

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

@interface HOLPendingCall : NSObject
@property(nonatomic, strong) dispatch_semaphore_t semaphore;
@property(nonatomic, strong, nullable) NSDictionary<NSString *, id> *result;
@property(nonatomic, strong, nullable) NSError *error;
@end

@implementation HOLPendingCall
@end

@interface HOLHolonRPCClient ()
@property(nonatomic, assign) NSInteger heartbeatIntervalMS;
@property(nonatomic, assign) NSInteger heartbeatTimeoutMS;
@property(nonatomic, assign) NSInteger reconnectMinDelayMS;
@property(nonatomic, assign) NSInteger reconnectMaxDelayMS;
@property(nonatomic, assign) double reconnectFactor;
@property(nonatomic, assign) double reconnectJitter;
@property(nonatomic, assign) NSInteger connectTimeoutMS;
@property(nonatomic, assign) NSInteger requestTimeoutMS;

@property(nonatomic, strong) NSURLSession *session;
@property(nonatomic, strong, nullable) NSURLSessionWebSocketTask *task;
@property(nonatomic, copy, nullable) NSString *endpoint;
@property(nonatomic, strong) NSMutableDictionary<NSString *, HOLPendingCall *> *pending;
@property(nonatomic, strong) NSMutableDictionary<NSString *, HOLHolonRPCHandler> *handlers;
@property(nonatomic, strong) dispatch_semaphore_t stateSignal;
@property(nonatomic, strong, nullable) dispatch_semaphore_t connectSemaphore;
@property(nonatomic, strong, nullable) dispatch_source_t heartbeatTimer;
@property(nonatomic, strong) dispatch_queue_t timerQueue;
@property(nonatomic, strong, nullable) NSError *lastConnectionError;
@property(nonatomic, assign) BOOL connected;
@property(nonatomic, assign) BOOL closed;
@property(nonatomic, assign) BOOL reconnectScheduled;
@property(nonatomic, assign) NSUInteger reconnectAttempt;
@property(nonatomic, assign) NSUInteger nextID;
@end

static NSString *HOLRPCIDString(id _Nullable rawID) {
  if (rawID == nil || rawID == [NSNull null]) {
    return nil;
  }
  if ([rawID isKindOfClass:[NSString class]]) {
    return (NSString *)rawID;
  }
  if ([rawID respondsToSelector:@selector(stringValue)]) {
    return [rawID stringValue];
  }
  return [rawID description];
}

@implementation HOLHolonRPCClient

- (instancetype)init {
  return [self initWithHeartbeatIntervalMS:15000
                        heartbeatTimeoutMS:5000
                       reconnectMinDelayMS:500
                       reconnectMaxDelayMS:30000
                           reconnectFactor:2.0
                           reconnectJitter:0.1
                          connectTimeoutMS:10000
                          requestTimeoutMS:10000];
}

- (instancetype)initWithHeartbeatIntervalMS:(NSInteger)heartbeatIntervalMS
                         heartbeatTimeoutMS:(NSInteger)heartbeatTimeoutMS
                        reconnectMinDelayMS:(NSInteger)reconnectMinDelayMS
                        reconnectMaxDelayMS:(NSInteger)reconnectMaxDelayMS
                            reconnectFactor:(double)reconnectFactor
                            reconnectJitter:(double)reconnectJitter
                           connectTimeoutMS:(NSInteger)connectTimeoutMS
                           requestTimeoutMS:(NSInteger)requestTimeoutMS {
  self = [super init];
  if (!self) {
    return nil;
  }

  _heartbeatIntervalMS = heartbeatIntervalMS;
  _heartbeatTimeoutMS = heartbeatTimeoutMS;
  _reconnectMinDelayMS = reconnectMinDelayMS;
  _reconnectMaxDelayMS = reconnectMaxDelayMS;
  _reconnectFactor = reconnectFactor;
  _reconnectJitter = reconnectJitter;
  _connectTimeoutMS = connectTimeoutMS;
  _requestTimeoutMS = requestTimeoutMS;

  NSOperationQueue *delegateQueue = [NSOperationQueue new];
  delegateQueue.maxConcurrentOperationCount = 1;
  NSURLSessionConfiguration *cfg = [NSURLSessionConfiguration ephemeralSessionConfiguration];
  _session = [NSURLSession sessionWithConfiguration:cfg
                                           delegate:self
                                      delegateQueue:delegateQueue];
  _pending = [NSMutableDictionary dictionary];
  _handlers = [NSMutableDictionary dictionary];
  _stateSignal = dispatch_semaphore_create(0);
  _timerQueue = dispatch_queue_create("org.organicprogramming.holons.rpc.timer",
                                      DISPATCH_QUEUE_SERIAL);
  _closed = YES;
  _connected = NO;
  _reconnectScheduled = NO;
  _reconnectAttempt = 0;
  _nextID = 0;
  return self;
}

- (void)dealloc {
  [self close];
  [_session finishTasksAndInvalidate];
#if !__has_feature(objc_arc)
  [super dealloc];
#endif
}

- (BOOL)connect:(NSString *)url error:(NSError **)error {
  if (url.length == 0) {
    if (error != NULL) {
      *error = HOLMakeError(1000, @"url is required");
    }
    return NO;
  }

  [self close];

  @synchronized(self) {
    self.closed = NO;
    self.connected = NO;
    self.endpoint = [url copy];
    self.lastConnectionError = nil;
    self.connectSemaphore = dispatch_semaphore_create(0);
    self.reconnectAttempt = 0;
  }

  [self openWebSocket];

  long waitRC = dispatch_semaphore_wait(
      self.connectSemaphore,
      dispatch_time(DISPATCH_TIME_NOW, self.connectTimeoutMS * NSEC_PER_MSEC));

  NSError *connectError = nil;
  @synchronized(self) {
    if (waitRC != 0) {
      connectError = HOLMakeError(1001, @"holon-rpc connect timeout");
    } else if (!self.connected) {
      connectError = self.lastConnectionError ?: HOLMakeError(1002, @"holon-rpc connect failed");
    }
    self.connectSemaphore = nil;
  }

  if (connectError != nil) {
    if (error != NULL) {
      *error = connectError;
    }
    [self close];
    return NO;
  }

  return YES;
}

- (nullable NSDictionary<NSString *, id> *)invoke:(NSString *)method
                                            params:(NSDictionary<NSString *, id> *)params
                                             error:(NSError **)error {
  return [self invoke:method params:params timeout:self.requestTimeoutMS error:error];
}

- (nullable NSDictionary<NSString *, id> *)invoke:(NSString *)method
                                            params:(NSDictionary<NSString *, id> *)params
                                           timeout:(NSTimeInterval)timeout
                                             error:(NSError **)error {
  if (method.length == 0) {
    if (error != NULL) {
      *error = HOLMakeError(1003, @"method is required");
    }
    return nil;
  }

  if (![self waitUntilConnected:self.connectTimeoutMS error:error]) {
    return nil;
  }

  NSString *requestID = nil;
  HOLPendingCall *call = [HOLPendingCall new];
  call.semaphore = dispatch_semaphore_create(0);

  @synchronized(self) {
    requestID = [NSString stringWithFormat:@"c%lu", (unsigned long)++self.nextID];
    self.pending[requestID] = call;
  }

  NSDictionary *payload = @{
    @"jsonrpc" : @"2.0",
    @"id" : requestID,
    @"method" : method,
    @"params" : params ?: @{}
  };

  NSError *sendError = nil;
  if (![self sendPayload:payload timeoutMS:timeout error:&sendError]) {
    @synchronized(self) {
      [self.pending removeObjectForKey:requestID];
    }
    if (error != NULL) {
      *error = sendError;
    }
    return nil;
  }

  long waitRC = dispatch_semaphore_wait(call.semaphore,
                                        dispatch_time(DISPATCH_TIME_NOW, timeout * NSEC_PER_MSEC));
  if (waitRC != 0) {
    @synchronized(self) {
      [self.pending removeObjectForKey:requestID];
    }
    if (error != NULL) {
      *error = HOLMakeError(1004, @"invoke timeout");
    }
    return nil;
  }

  if (call.error != nil) {
    if (error != NULL) {
      *error = call.error;
    }
    return nil;
  }

  return call.result ?: @{};
}

- (void)registerMethod:(NSString *)method handler:(HOLHolonRPCHandler)handler {
  if (method.length == 0 || handler == nil) {
    return;
  }
  @synchronized(self) {
    self.handlers[method] = [handler copy];
  }
}

- (void)close {
  NSDictionary<NSString *, HOLPendingCall *> *pendingSnapshot = nil;
  NSURLSessionWebSocketTask *taskToClose = nil;

  @synchronized(self) {
    if (self.closed && !self.connected && self.task == nil) {
      return;
    }
    self.closed = YES;
    self.connected = NO;
    self.endpoint = nil;
    self.reconnectScheduled = NO;
    self.reconnectAttempt = 0;
    self.lastConnectionError = HOLMakeError(1005, @"holon-rpc client closed");
    taskToClose = self.task;
    self.task = nil;
    pendingSnapshot = [self.pending copy];
    [self.pending removeAllObjects];
  }

  [self stopHeartbeatLocked];
  dispatch_semaphore_signal(self.stateSignal);

  for (HOLPendingCall *call in pendingSnapshot.allValues) {
    call.error = HOLMakeError(1005, @"holon-rpc client closed");
    dispatch_semaphore_signal(call.semaphore);
  }

  if (taskToClose != nil) {
    [taskToClose cancelWithCloseCode:NSURLSessionWebSocketCloseCodeNormalClosure
                              reason:nil];
  }
}

- (void)openWebSocket {
  NSString *endpoint = nil;
  @synchronized(self) {
    endpoint = self.endpoint;
  }
  if (endpoint.length == 0) {
    return;
  }

  NSURL *url = [NSURL URLWithString:endpoint];
  if (url == nil) {
    [self handleDisconnectWithError:HOLMakeError(1006, @"invalid websocket url")];
    return;
  }

  NSURLSessionWebSocketTask *task =
      [self.session webSocketTaskWithURL:url protocols:@[ @"holon-rpc" ]];
  @synchronized(self) {
    self.task = task;
  }
  [task resume];
}

- (BOOL)waitUntilConnected:(NSInteger)timeoutMS error:(NSError **)error {
  NSDate *deadline = [NSDate dateWithTimeIntervalSinceNow:(double)timeoutMS / 1000.0];

  while (YES) {
    @synchronized(self) {
      if (self.connected) {
        return YES;
      }
      if (self.closed) {
        if (error != NULL) {
          *error = self.lastConnectionError ?: HOLMakeError(1007, @"holon-rpc client closed");
        }
        return NO;
      }
    }

    NSTimeInterval remaining = [deadline timeIntervalSinceNow];
    if (remaining <= 0) {
      if (error != NULL) {
        *error = HOLMakeError(1008, @"holon-rpc wait connected timeout");
      }
      return NO;
    }

    dispatch_time_t waitUntil =
        dispatch_time(DISPATCH_TIME_NOW, (int64_t)(MIN(remaining, 0.05) * NSEC_PER_SEC));
    dispatch_semaphore_wait(self.stateSignal, waitUntil);
  }
}

- (BOOL)sendPayload:(NSDictionary *)payload timeoutMS:(NSInteger)timeoutMS error:(NSError **)error {
  NSURLSessionWebSocketTask *task = nil;
  @synchronized(self) {
    task = self.task;
  }
  if (task == nil) {
    if (error != NULL) {
      *error = HOLMakeError(1009, @"websocket is not connected");
    }
    return NO;
  }

  NSData *raw = [NSJSONSerialization dataWithJSONObject:payload options:0 error:error];
  if (raw == nil) {
    return NO;
  }
  NSString *text = [[NSString alloc] initWithData:raw encoding:NSUTF8StringEncoding];
  if (text == nil) {
    if (error != NULL) {
      *error = HOLMakeError(1010, @"failed to encode payload");
    }
    return NO;
  }

  NSURLSessionWebSocketMessage *msg =
      [[NSURLSessionWebSocketMessage alloc] initWithString:text];
  HOLHolonRPCClient *__unsafe_unretained weakSelf = self;
  [task sendMessage:msg
  completionHandler:^(NSError *_Nullable taskError) {
    if (taskError != nil) {
      HOLHolonRPCClient *selfStrong = weakSelf;
      if (selfStrong != nil) {
        [selfStrong handleDisconnectWithError:taskError];
      }
    }
  }];

  return YES;
}

- (void)receiveNextMessage {
  NSURLSessionWebSocketTask *task = nil;
  @synchronized(self) {
    if (!self.connected || self.closed || self.task == nil) {
      return;
    }
    task = self.task;
  }

  HOLHolonRPCClient *__unsafe_unretained weakSelf = self;
  [task receiveMessageWithCompletionHandler:^(NSURLSessionWebSocketMessage *_Nullable message,
                                              NSError *_Nullable error) {
    HOLHolonRPCClient *selfStrong = weakSelf;
    if (selfStrong == nil) {
      return;
    }
    if (error != nil) {
      [selfStrong handleDisconnectWithError:error];
      return;
    }

    NSString *text = nil;
    if (message.type == NSURLSessionWebSocketMessageTypeString) {
      text = message.string;
    } else if (message.type == NSURLSessionWebSocketMessageTypeData) {
      text = [[NSString alloc] initWithData:message.data encoding:NSUTF8StringEncoding];
    }
    if (text != nil) {
      [selfStrong handleIncomingText:text];
    }

    [selfStrong receiveNextMessage];
  }];
}

- (void)handleIncomingText:(NSString *)text {
  NSData *data = [text dataUsingEncoding:NSUTF8StringEncoding];
  if (data == nil) {
    return;
  }

  NSError *jsonError = nil;
  id obj = [NSJSONSerialization JSONObjectWithData:data options:0 error:&jsonError];
  if (jsonError != nil || ![obj isKindOfClass:[NSDictionary class]]) {
    return;
  }

  NSDictionary *msg = (NSDictionary *)obj;
  if (msg[@"method"] != nil) {
    [self handleIncomingRequest:msg];
    return;
  }
  if (msg[@"result"] != nil || msg[@"error"] != nil) {
    [self handleIncomingResponse:msg];
  }
}

- (void)handleIncomingResponse:(NSDictionary *)msg {
  NSString *requestID = HOLRPCIDString(msg[@"id"]);
  if (requestID.length == 0) {
    return;
  }

  HOLPendingCall *call = nil;
  @synchronized(self) {
    call = self.pending[requestID];
    [self.pending removeObjectForKey:requestID];
  }
  if (call == nil) {
    return;
  }

  NSDictionary *errorObj = [msg[@"error"] isKindOfClass:[NSDictionary class]] ? msg[@"error"] : nil;
  if (errorObj != nil) {
    NSInteger code = [errorObj[@"code"] respondsToSelector:@selector(integerValue)]
                         ? [errorObj[@"code"] integerValue]
                         : -32603;
    NSString *message = [errorObj[@"message"] isKindOfClass:[NSString class]]
                            ? errorObj[@"message"]
                            : @"internal error";
    call.error = HOLMakeError(code, [NSString stringWithFormat:@"rpc error %ld: %@",
                                                             (long)code, message]);
  } else {
    NSDictionary *result = [msg[@"result"] isKindOfClass:[NSDictionary class]] ? msg[@"result"] : @{};
    call.result = result;
  }
  dispatch_semaphore_signal(call.semaphore);
}

- (void)handleIncomingRequest:(NSDictionary *)msg {
  id rawID = msg[@"id"];
  NSString *method = [msg[@"method"] isKindOfClass:[NSString class]] ? msg[@"method"] : nil;
  NSString *jsonrpc = [msg[@"jsonrpc"] isKindOfClass:[NSString class]] ? msg[@"jsonrpc"] : nil;

  BOOL hasID = (rawID != nil && rawID != [NSNull null]);
  if (![jsonrpc isEqualToString:@"2.0"] || method.length == 0) {
    if (hasID) {
      [self sendErrorWithID:rawID code:-32600 message:@"invalid request" data:nil];
    }
    return;
  }

  if ([method isEqualToString:@"rpc.heartbeat"]) {
    if (hasID) {
      [self sendResultWithID:rawID result:@{}];
    }
    return;
  }

  if (hasID) {
    NSString *sid = HOLRPCIDString(rawID);
    if (sid.length == 0 || ![sid hasPrefix:@"s"]) {
      [self sendErrorWithID:rawID
                       code:-32600
                    message:@"server request id must start with 's'"
                       data:nil];
      return;
    }
  }

  HOLHolonRPCHandler handler = nil;
  @synchronized(self) {
    handler = self.handlers[method];
  }
  if (handler == nil) {
    if (hasID) {
      [self sendErrorWithID:rawID
                       code:-32601
                    message:[NSString stringWithFormat:@"method \"%@\" not found", method]
                       data:nil];
    }
    return;
  }

  NSDictionary *params = [msg[@"params"] isKindOfClass:[NSDictionary class]] ? msg[@"params"] : @{};
  @try {
    NSDictionary *result = handler(params ?: @{});
    if (hasID) {
      [self sendResultWithID:rawID result:result ?: @{}];
    }
  } @catch (NSException *exception) {
    if (hasID) {
      [self sendErrorWithID:rawID code:13 message:exception.reason ?: @"internal error" data:nil];
    }
  }
}

- (void)sendResultWithID:(id)rawID result:(NSDictionary *)result {
  NSDictionary *payload = @{
    @"jsonrpc" : @"2.0",
    @"id" : rawID ?: [NSNull null],
    @"result" : result ?: @{}
  };
  [self sendPayload:payload timeoutMS:self.requestTimeoutMS error:nil];
}

- (void)sendErrorWithID:(id)rawID code:(NSInteger)code message:(NSString *)message data:(id)data {
  NSMutableDictionary *errorObj = [@{
    @"code" : @(code),
    @"message" : message ?: @"internal error",
  } mutableCopy];
  if (data != nil) {
    errorObj[@"data"] = data;
  }
  NSDictionary *payload = @{
    @"jsonrpc" : @"2.0",
    @"id" : rawID ?: [NSNull null],
    @"error" : errorObj
  };
  [self sendPayload:payload timeoutMS:self.requestTimeoutMS error:nil];
}

- (void)startHeartbeatLocked {
  [self stopHeartbeatLocked];
  if (self.closed || !self.connected) {
    return;
  }

  dispatch_source_t timer =
      dispatch_source_create(DISPATCH_SOURCE_TYPE_TIMER, 0, 0, self.timerQueue);
  if (timer == nil) {
    return;
  }

  dispatch_source_set_timer(timer,
                            dispatch_time(DISPATCH_TIME_NOW, self.heartbeatIntervalMS * NSEC_PER_MSEC),
                            self.heartbeatIntervalMS * NSEC_PER_MSEC,
                            50 * NSEC_PER_MSEC);
  HOLHolonRPCClient *__unsafe_unretained weakSelf = self;
  dispatch_source_set_event_handler(timer, ^{
    HOLHolonRPCClient *selfStrong = weakSelf;
    if (selfStrong == nil) {
      return;
    }

    NSError *hbError = nil;
    [selfStrong invoke:@"rpc.heartbeat" params:@{} timeout:selfStrong.heartbeatTimeoutMS error:&hbError];
    if (hbError != nil) {
      [selfStrong forceDisconnect];
    }
  });
  dispatch_resume(timer);
  self.heartbeatTimer = timer;
}

- (void)stopHeartbeatLocked {
  dispatch_source_t timer = nil;
  @synchronized(self) {
    timer = self.heartbeatTimer;
    self.heartbeatTimer = nil;
  }
  if (timer != nil) {
    dispatch_source_cancel(timer);
  }
}

- (void)forceDisconnect {
  NSURLSessionWebSocketTask *task = nil;
  @synchronized(self) {
    task = self.task;
  }
  if (task != nil) {
    [task cancelWithCloseCode:NSURLSessionWebSocketCloseCodeGoingAway reason:nil];
  }
}

- (void)scheduleReconnect {
  NSTimeInterval delaySec = 0;
  @synchronized(self) {
    if (self.closed || self.reconnectScheduled || self.endpoint.length == 0) {
      return;
    }
    self.reconnectScheduled = YES;
    double base = MIN(self.reconnectMinDelayMS * pow(self.reconnectFactor, self.reconnectAttempt),
                      (double)self.reconnectMaxDelayMS);
    double jitter = base * self.reconnectJitter * ((double)arc4random() / UINT32_MAX);
    delaySec = (base + jitter) / 1000.0;
    self.reconnectAttempt += 1;
  }

  HOLHolonRPCClient *__unsafe_unretained weakSelf = self;
  dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)(delaySec * NSEC_PER_SEC)),
                 self.timerQueue, ^{
                   HOLHolonRPCClient *selfStrong = weakSelf;
                   if (selfStrong == nil) {
                     return;
                   }
                   @synchronized(selfStrong) {
                     selfStrong.reconnectScheduled = NO;
                     if (selfStrong.closed || selfStrong.connected) {
                       return;
                     }
                   }
                   [selfStrong openWebSocket];
                 });
}

- (void)handleDisconnectWithError:(NSError *)error {
  NSDictionary<NSString *, HOLPendingCall *> *pendingSnapshot = nil;
  dispatch_semaphore_t connectSem = nil;
  BOOL shouldReconnect = NO;

  @synchronized(self) {
    if (self.closed && self.task == nil && !self.connected) {
      return;
    }

    self.connected = NO;
    self.lastConnectionError = error ?: HOLMakeError(1012, @"holon-rpc connection closed");
    self.task = nil;

    pendingSnapshot = [self.pending copy];
    [self.pending removeAllObjects];

    connectSem = self.connectSemaphore;
    shouldReconnect = !self.closed;
  }

  [self stopHeartbeatLocked];

  for (HOLPendingCall *call in pendingSnapshot.allValues) {
    call.error = self.lastConnectionError;
    dispatch_semaphore_signal(call.semaphore);
  }
  if (connectSem != nil) {
    dispatch_semaphore_signal(connectSem);
  }
  dispatch_semaphore_signal(self.stateSignal);

  if (shouldReconnect) {
    [self scheduleReconnect];
  }
}

#pragma mark - NSURLSession Delegate

- (void)URLSession:(NSURLSession *)session
    webSocketTask:(NSURLSessionWebSocketTask *)webSocketTask
didOpenWithProtocol:(NSString *)protocol {
  if (protocol.length == 0 || ![protocol isEqualToString:@"holon-rpc"]) {
    [self handleDisconnectWithError:HOLMakeError(1013, @"server did not negotiate holon-rpc")];
    [webSocketTask cancelWithCloseCode:NSURLSessionWebSocketCloseCodeProtocolError reason:nil];
    return;
  }

  dispatch_semaphore_t connectSem = nil;
  @synchronized(self) {
    self.connected = YES;
    self.lastConnectionError = nil;
    self.reconnectAttempt = 0;
    self.reconnectScheduled = NO;
    self.task = webSocketTask;
    connectSem = self.connectSemaphore;
  }

  if (connectSem != nil) {
    dispatch_semaphore_signal(connectSem);
  }
  dispatch_semaphore_signal(self.stateSignal);
  [self startHeartbeatLocked];
  [self receiveNextMessage];
}

- (void)URLSession:(NSURLSession *)session
    webSocketTask:(NSURLSessionWebSocketTask *)webSocketTask
didCloseWithCode:(NSURLSessionWebSocketCloseCode)closeCode
           reason:(NSData *)reason {
  [self handleDisconnectWithError:nil];
}

- (void)URLSession:(NSURLSession *)session
              task:(NSURLSessionTask *)task
didCompleteWithError:(NSError *)error {
  if (error != nil) {
    [self handleDisconnectWithError:error];
  }
}

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
