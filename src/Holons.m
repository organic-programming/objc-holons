#import <Holons/Holons.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <math.h>

NSString *const HOLDefaultURI = @"tcp://:9090";
static NSString *const HOLErrorDomain = @"org.organicprogramming.holons";
static const NSTimeInterval HOLDefaultConnectTimeout = 5.0;
static NSError *HOLMakeError(NSInteger code, NSString *message);
static NSMutableDictionary<NSString *, id> *HOLStartedChannels(void);

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

@interface GRPCChannel ()
@property(nonatomic, copy, readwrite) NSString *target;
@property(nonatomic, copy, readwrite) NSString *transport;
@property(nonatomic, strong, nullable) NSTask *task;
@property(nonatomic, strong, nullable) NSPipe *stdinPipe;
@property(nonatomic, strong, nullable) NSPipe *stdoutPipe;
@property(nonatomic, strong, nullable) NSPipe *stderrPipe;
@property(nonatomic, assign) BOOL closed;
- (instancetype)initWithTarget:(NSString *)target
                     transport:(NSString *)transport
                          task:(NSTask *_Nullable)task
                     stdinPipe:(NSPipe *_Nullable)stdinPipe
                    stdoutPipe:(NSPipe *_Nullable)stdoutPipe
                    stderrPipe:(NSPipe *_Nullable)stderrPipe;
@end

@implementation GRPCChannel

- (instancetype)initWithTarget:(NSString *)target
                     transport:(NSString *)transport
                          task:(NSTask *)task
                     stdinPipe:(NSPipe *)stdinPipe
                    stdoutPipe:(NSPipe *)stdoutPipe
                    stderrPipe:(NSPipe *)stderrPipe {
  self = [super init];
  if (!self) {
    return nil;
  }
  _target = [target copy] ?: @"";
  _transport = [transport copy] ?: @"";
  _task = task;
  _stdinPipe = stdinPipe;
  _stdoutPipe = stdoutPipe;
  _stderrPipe = stderrPipe;
  _closed = NO;
  return self;
}

@end

@implementation HOLHolonIdentity
@end

@implementation HOLHolonBuild
@end

@implementation HOLHolonArtifacts
@end

@implementation HOLHolonManifest
@end

@implementation HOLHolonEntry
@end

@implementation HolonsConnectOptions

- (instancetype)init {
  self = [super init];
  if (!self) {
    return nil;
  }
  _timeout = HOLDefaultConnectTimeout;
  _transport = @"stdio";
  _start = YES;
  _portFile = nil;
  return self;
}

@end

@interface HOLStartedChannel : NSObject
@property(nonatomic, strong, nullable) NSTask *task;
@property(nonatomic, assign) BOOL ephemeral;
@end

@implementation HOLStartedChannel
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

static NSString *HOLSlugifyPart(NSString *value) {
  NSString *trimmed = [value stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]];
  NSMutableString *slug = [NSMutableString string];

  for (NSUInteger i = 0; i < trimmed.length; i++) {
    unichar ch = [trimmed characterAtIndex:i];
    if (ch == '?') {
      continue;
    }
    if ([[NSCharacterSet whitespaceCharacterSet] characterIsMember:ch]) {
      [slug appendString:@"-"];
      continue;
    }
    NSString *piece = [[NSString stringWithCharacters:&ch length:1] lowercaseString];
    [slug appendString:piece];
  }

  while ([slug hasSuffix:@"-"]) {
    [slug deleteCharactersInRange:NSMakeRange(slug.length - 1, 1)];
  }
  return slug;
}

static NSString *HOLSlugFromIdentity(HOLHolonIdentity *identity) {
  NSString *given = HOLSlugifyPart(identity.givenName ?: @"");
  NSString *family = HOLSlugifyPart(identity.familyName ?: @"");
  if (given.length == 0) {
    return family;
  }
  if (family.length == 0) {
    return given;
  }
  return [NSString stringWithFormat:@"%@-%@", given, family];
}

static HOLHolonManifest *HOLParseManifest(NSString *path, NSError **error) {
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

  NSArray<NSString *> *lines =
      [text componentsSeparatedByCharactersInSet:[NSCharacterSet newlineCharacterSet]];
  HOLHolonManifest *manifest = [HOLHolonManifest new];
  manifest.kind = @"";
  manifest.build = [HOLHolonBuild new];
  manifest.build.runner = @"";
  manifest.build.main = @"";
  manifest.artifacts = [HOLHolonArtifacts new];
  manifest.artifacts.binary = @"";
  manifest.artifacts.primary = @"";

  BOOL sawMapping = NO;
  BOOL sawManifestValue = NO;
  NSString *section = nil;

  for (NSString *rawLine in lines) {
    NSUInteger indent = [rawLine rangeOfCharacterFromSet:
                                     [[NSCharacterSet whitespaceCharacterSet] invertedSet]]
                             .location;
    if (indent == NSNotFound) {
      continue;
    }

    NSString *line = [rawLine stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]];
    if (line.length == 0 || [line hasPrefix:@"#"]) {
      continue;
    }

    NSRange colon = [line rangeOfString:@":"];
    if (colon.location == NSNotFound) {
      continue;
    }

    sawMapping = YES;
    NSString *key = [line substringToIndex:colon.location];
    NSString *value = HOLYAMLValue(line);

    if (indent == 0) {
      section = nil;
      if ([key isEqualToString:@"kind"]) {
        manifest.kind = value;
        sawManifestValue = YES;
      } else if (([key isEqualToString:@"build"] || [key isEqualToString:@"artifacts"]) &&
                 value.length == 0) {
        section = key;
      }
      continue;
    }

    if ([section isEqualToString:@"build"]) {
      if ([key isEqualToString:@"runner"]) {
        manifest.build.runner = value;
        sawManifestValue = YES;
      } else if ([key isEqualToString:@"main"]) {
        manifest.build.main = value;
        sawManifestValue = YES;
      }
    } else if ([section isEqualToString:@"artifacts"]) {
      if ([key isEqualToString:@"binary"]) {
        manifest.artifacts.binary = value;
        sawManifestValue = YES;
      } else if ([key isEqualToString:@"primary"]) {
        manifest.artifacts.primary = value;
        sawManifestValue = YES;
      }
    }
  }

  if (!sawMapping) {
    if (error != NULL) {
      *error = HOLMakeError(20, [NSString stringWithFormat:@"%@: holon.yaml must be a YAML mapping", path]);
    }
    return nil;
  }

  return sawManifestValue ? manifest : nil;
}

static NSString *HOLResolveDiscoveryRoot(NSString *root) {
  NSString *candidate = root;
  if (candidate.length == 0) {
    candidate = [[NSFileManager defaultManager] currentDirectoryPath];
  } else if (![candidate isAbsolutePath]) {
    candidate = [[[NSFileManager defaultManager] currentDirectoryPath]
        stringByAppendingPathComponent:candidate];
  }
  return [[candidate stringByStandardizingPath] stringByResolvingSymlinksInPath];
}

static NSString *HOLRelativePathFromRoot(NSString *root, NSString *path) {
  if ([path isEqualToString:root]) {
    return @".";
  }

  NSString *prefix = [root hasSuffix:@"/"] ? root : [root stringByAppendingString:@"/"];
  if ([path hasPrefix:prefix]) {
    NSString *relative = [path substringFromIndex:prefix.length];
    return relative.length > 0 ? relative : @".";
  }
  return path;
}

static BOOL HOLShouldSkipDiscoveryDir(NSString *name) {
  if ([name isEqualToString:@".git"] || [name isEqualToString:@".op"] ||
      [name isEqualToString:@"node_modules"] || [name isEqualToString:@"vendor"] ||
      [name isEqualToString:@"build"]) {
    return YES;
  }
  return [name hasPrefix:@"."];
}

static NSUInteger HOLRelativePathDepth(NSString *relativePath) {
  if (relativePath.length == 0 || [relativePath isEqualToString:@"."]) {
    return 0;
  }
  return (NSUInteger)[[relativePath pathComponents] count];
}

static NSString *HOLOPPath(void) {
  const char *configured = getenv("OPPATH");
  if (configured != NULL && configured[0] != '\0') {
    return HOLResolveDiscoveryRoot([NSString stringWithUTF8String:configured]);
  }

  const char *home = getenv("HOME");
  if (home != NULL && home[0] != '\0') {
    return HOLResolveDiscoveryRoot([[NSString stringWithUTF8String:home]
        stringByAppendingPathComponent:@".op"]);
  }
  return HOLResolveDiscoveryRoot(@".op");
}

static NSString *HOLOPBin(void) {
  const char *configured = getenv("OPBIN");
  if (configured != NULL && configured[0] != '\0') {
    return HOLResolveDiscoveryRoot([NSString stringWithUTF8String:configured]);
  }
  return [[HOLOPPath() stringByAppendingPathComponent:@"bin"] stringByStandardizingPath];
}

static NSString *HOLCacheDir(void) {
  return [[HOLOPPath() stringByAppendingPathComponent:@"cache"] stringByStandardizingPath];
}

static void HOLAppendOrReplaceEntry(NSMutableArray<HOLHolonEntry *> *entries,
                                    NSMutableDictionary<NSString *, NSNumber *> *indexByKey,
                                    HOLHolonEntry *candidate) {
  NSString *key = candidate.uuid.length > 0 ? candidate.uuid : candidate.dir;
  NSNumber *existingIndex = indexByKey[key];
  if (existingIndex != nil) {
    NSUInteger idx = (NSUInteger)[existingIndex unsignedIntegerValue];
    HOLHolonEntry *existing = entries[idx];
    if (HOLRelativePathDepth(candidate.relativePath) < HOLRelativePathDepth(existing.relativePath)) {
      entries[idx] = candidate;
    }
    return;
  }

  indexByKey[key] = @(entries.count);
  [entries addObject:candidate];
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

  NSArray<NSString *> *lines = [text componentsSeparatedByCharactersInSet:[NSCharacterSet newlineCharacterSet]];

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

  BOOL sawMapping = NO;
  for (NSString *rawLine in lines) {
    NSString *line = [rawLine stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]];
    if (line.length == 0 || [line hasPrefix:@"#"]) {
      continue;
    }
    if ([line containsString:@":"]) {
      sawMapping = YES;
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

  if (!sawMapping) {
    if (error != NULL) {
      *error = HOLMakeError(20, [NSString stringWithFormat:@"%@: holon.yaml must be a YAML mapping", path]);
    }
    return nil;
  }

  return identity;
}

static NSArray<HOLHolonEntry *> *HOLDiscoverWithOrigin(NSString *root,
                                                       NSString *origin,
                                                       NSError **error) {
  NSString *resolvedRoot = HOLResolveDiscoveryRoot(root);
  BOOL isDirectory = NO;
  if (![[NSFileManager defaultManager] fileExistsAtPath:resolvedRoot isDirectory:&isDirectory] ||
      !isDirectory) {
    return @[];
  }

  NSURL *rootURL = [NSURL fileURLWithPath:resolvedRoot isDirectory:YES];
  NSDirectoryEnumerator<NSURL *> *enumerator =
      [[NSFileManager defaultManager] enumeratorAtURL:rootURL
                          includingPropertiesForKeys:@[ NSURLIsDirectoryKey ]
                                             options:0
                                        errorHandler:^BOOL(NSURL *url, NSError *enumeratorError) {
                                          (void)url;
                                          (void)enumeratorError;
                                          return YES;
                                        }];
  if (enumerator == nil) {
    if (error != NULL) {
      *error = HOLMakeError(21, [NSString stringWithFormat:@"cannot enumerate %@", resolvedRoot]);
    }
    return nil;
  }

  NSMutableArray<HOLHolonEntry *> *entries = [NSMutableArray array];
  NSMutableDictionary<NSString *, NSNumber *> *indexByKey = [NSMutableDictionary dictionary];

  for (NSURL *url in enumerator) {
    NSNumber *isDirValue = nil;
    [url getResourceValue:&isDirValue forKey:NSURLIsDirectoryKey error:nil];

    if (isDirValue.boolValue) {
      if (HOLShouldSkipDiscoveryDir(url.lastPathComponent)) {
        [enumerator skipDescendants];
      }
      continue;
    }

    if (![url.lastPathComponent isEqualToString:@"holon.yaml"]) {
      continue;
    }

    NSString *manifestPath = [[[url path] stringByStandardizingPath] stringByResolvingSymlinksInPath];
    HOLHolonIdentity *identity = HOLParseHolon(manifestPath, nil);
    if (identity == nil) {
      continue;
    }

    NSString *dirPath = [[[manifestPath stringByDeletingLastPathComponent] stringByStandardizingPath]
        stringByResolvingSymlinksInPath];
    HOLHolonEntry *entry = [HOLHolonEntry new];
    entry.slug = HOLSlugFromIdentity(identity);
    entry.uuid = identity.uuid ?: @"";
    entry.dir = dirPath;
    entry.relativePath = HOLRelativePathFromRoot(resolvedRoot, dirPath);
    entry.origin = origin;
    entry.identity = identity;
    entry.manifest = HOLParseManifest(manifestPath, nil);
    HOLAppendOrReplaceEntry(entries, indexByKey, entry);
  }

  return [entries sortedArrayUsingComparator:^NSComparisonResult(HOLHolonEntry *left,
                                                                 HOLHolonEntry *right) {
    NSComparisonResult rel = [left.relativePath compare:right.relativePath];
    if (rel != NSOrderedSame) {
      return rel;
    }
    return [left.uuid compare:right.uuid];
  }];
}

NSArray<HOLHolonEntry *> *HOLDiscover(NSString *root, NSError **error) {
  return HOLDiscoverWithOrigin(root, @"local", error);
}

NSArray<HOLHolonEntry *> *HOLDiscoverLocal(NSError **error) {
  return HOLDiscoverWithOrigin([[NSFileManager defaultManager] currentDirectoryPath], @"local", error);
}

NSArray<HOLHolonEntry *> *HOLDiscoverAll(NSError **error) {
  NSMutableArray<HOLHolonEntry *> *entries = [NSMutableArray array];
  NSMutableDictionary<NSString *, NSNumber *> *indexByKey = [NSMutableDictionary dictionary];

  NSArray<NSArray<NSString *> *> *roots = @[
    @[ [[NSFileManager defaultManager] currentDirectoryPath], @"local" ],
    @[ HOLOPBin(), @"$OPBIN" ],
    @[ HOLCacheDir(), @"cache" ],
  ];

  for (NSArray<NSString *> *pair in roots) {
    NSArray<HOLHolonEntry *> *found = HOLDiscoverWithOrigin(pair[0], pair[1], error);
    if (found == nil) {
      return nil;
    }
    for (HOLHolonEntry *entry in found) {
      HOLAppendOrReplaceEntry(entries, indexByKey, entry);
    }
  }

  return [entries sortedArrayUsingComparator:^NSComparisonResult(HOLHolonEntry *left,
                                                                 HOLHolonEntry *right) {
    NSComparisonResult rel = [left.relativePath compare:right.relativePath];
    if (rel != NSOrderedSame) {
      return rel;
    }
    return [left.uuid compare:right.uuid];
  }];
}

HOLHolonEntry *HOLFindBySlug(NSString *slug, NSError **error) {
  NSArray<HOLHolonEntry *> *entries = HOLDiscoverAll(error);
  if (entries == nil) {
    return nil;
  }
  for (HOLHolonEntry *entry in entries) {
    if ([entry.slug isEqualToString:slug]) {
      return entry;
    }
  }
  return nil;
}

HOLHolonEntry *HOLFindByUUID(NSString *prefix, NSError **error) {
  NSArray<HOLHolonEntry *> *entries = HOLDiscoverAll(error);
  if (entries == nil) {
    return nil;
  }
  for (HOLHolonEntry *entry in entries) {
    if ([entry.uuid hasPrefix:prefix]) {
      return entry;
    }
  }
  return nil;
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

static NSMutableDictionary<NSString *, id> *HOLStartedChannels(void) {
  static NSMutableDictionary<NSString *, id> *channels = nil;
  static dispatch_once_t onceToken;
  dispatch_once(&onceToken, ^{
    channels = [NSMutableDictionary dictionary];
  });
  return channels;
}

static NSString *HOLTrimmedString(NSString *value) {
  if (value == nil) {
    return @"";
  }
  return [value stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceAndNewlineCharacterSet]];
}

static NSString *HOLChannelKey(GRPCChannel *channel) {
  return [NSString stringWithFormat:@"%p", channel];
}

static BOOL HOLIsDirectTarget(NSString *target) {
  NSString *trimmed = HOLTrimmedString(target);
  if (trimmed.length == 0) {
    return NO;
  }
  if ([trimmed containsString:@"://"]) {
    return YES;
  }
  return [trimmed containsString:@":"];
}

static NSString *HOLTransportForTarget(NSString *target) {
  NSString *trimmed = HOLTrimmedString(target);
  if (trimmed.length == 0) {
    return @"";
  }
  if ([trimmed containsString:@"://"]) {
    return [HOLScheme(trimmed) lowercaseString];
  }
  return [trimmed containsString:@":"] ? @"tcp" : @"";
}

static NSString *HOLNormalizeDialTarget(NSString *target) {
  NSString *trimmed = HOLTrimmedString(target);
  if (trimmed.length == 0) {
    return @"";
  }
  if (![trimmed containsString:@"://"]) {
    HOLParsedURI *parsed = HOLParseURI([@"tcp://" stringByAppendingString:trimmed]);
    NSString *host = parsed.host;
    if (host.length == 0 || [host isEqualToString:@"0.0.0.0"] || [host isEqualToString:@"::"] ||
        [host isEqualToString:@"[::]"]) {
      host = @"127.0.0.1";
    }
    if (parsed.port == nil) {
      return trimmed;
    }
    return [NSString stringWithFormat:@"%@:%d", host, parsed.port.intValue];
  }

  HOLParsedURI *parsed = HOLParseURI(trimmed);
  if ([parsed.scheme isEqualToString:@"tcp"]) {
    NSString *host = parsed.host;
    if (host.length == 0 || [host isEqualToString:@"0.0.0.0"] || [host isEqualToString:@"::"] ||
        [host isEqualToString:@"[::]"]) {
      host = @"127.0.0.1";
    }
    if (parsed.port == nil) {
      return trimmed;
    }
    return [NSString stringWithFormat:@"%@:%d", host, parsed.port.intValue];
  }

  return trimmed;
}

static NSString *HOLNormalizeEndpointURI(NSString *target) {
  NSString *trimmed = HOLTrimmedString(target);
  if (trimmed.length == 0) {
    return @"";
  }
  if (![trimmed containsString:@"://"]) {
    NSString *dialTarget = HOLNormalizeDialTarget(trimmed);
    return dialTarget.length > 0 ? [@"tcp://" stringByAppendingString:dialTarget] : @"";
  }

  HOLParsedURI *parsed = HOLParseURI(trimmed);
  if ([parsed.scheme isEqualToString:@"tcp"]) {
    NSString *host = parsed.host;
    if (host.length == 0 || [host isEqualToString:@"0.0.0.0"] || [host isEqualToString:@"::"] ||
        [host isEqualToString:@"[::]"]) {
      host = @"127.0.0.1";
    }
    if (parsed.port == nil) {
      return trimmed;
    }
    return [NSString stringWithFormat:@"tcp://%@:%d", host, parsed.port.intValue];
  }

  return trimmed;
}

static BOOL HOLWaitForConnectResult(int fd, NSTimeInterval timeout) {
  fd_set writeSet;
  FD_ZERO(&writeSet);
  FD_SET(fd, &writeSet);

  struct timeval tv;
  tv.tv_sec = (int)timeout;
  tv.tv_usec = (int)((timeout - floor(timeout)) * 1000000.0);

  int rc = select(fd + 1, NULL, &writeSet, NULL, timeout >= 0 ? &tv : NULL);
  if (rc <= 0) {
    return NO;
  }

  int soError = 0;
  socklen_t len = sizeof(soError);
  if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &soError, &len) != 0) {
    return NO;
  }
  return soError == 0;
}

static BOOL HOLCanConnectTCP(NSString *host, int port, NSTimeInterval timeout) {
  NSString *resolvedHost = HOLTrimmedString(host);
  if (resolvedHost.length == 0 || [resolvedHost isEqualToString:@"0.0.0.0"] ||
      [resolvedHost isEqualToString:@"::"] || [resolvedHost isEqualToString:@"[::]"]) {
    resolvedHost = @"127.0.0.1";
  }
  if (port <= 0) {
    return NO;
  }

  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  char portText[32];
  snprintf(portText, sizeof(portText), "%d", port);

  struct addrinfo *results = NULL;
  if (getaddrinfo(resolvedHost.UTF8String, portText, &hints, &results) != 0) {
    return NO;
  }

  BOOL connected = NO;
  for (struct addrinfo *it = results; it != NULL; it = it->ai_next) {
    int fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (fd < 0) {
      continue;
    }

    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
      fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    int rc = connect(fd, it->ai_addr, (socklen_t)it->ai_addrlen);
    if (rc == 0) {
      connected = YES;
    } else if (errno == EINPROGRESS || errno == EWOULDBLOCK) {
      connected = HOLWaitForConnectResult(fd, timeout);
    }

    close(fd);
    if (connected) {
      break;
    }
  }

  freeaddrinfo(results);
  return connected;
}

static BOOL HOLCanConnectUnix(NSString *path, NSTimeInterval timeout) {
  NSString *trimmedPath = HOLTrimmedString(path);
  if (trimmedPath.length == 0 || trimmedPath.length >= sizeof(((struct sockaddr_un *)0)->sun_path)) {
    return NO;
  }

  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    return NO;
  }

  int flags = fcntl(fd, F_GETFL, 0);
  if (flags >= 0) {
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  }

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, trimmedPath.UTF8String, sizeof(addr.sun_path) - 1);

  BOOL connected = NO;
  int rc = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
  if (rc == 0) {
    connected = YES;
  } else if (errno == EINPROGRESS || errno == EWOULDBLOCK) {
    connected = HOLWaitForConnectResult(fd, timeout);
  }

  close(fd);
  return connected;
}

static BOOL HOLCanDialTarget(NSString *target, NSTimeInterval timeout) {
  NSString *trimmed = HOLTrimmedString(target);
  if (trimmed.length == 0) {
    return NO;
  }

  NSString *transport = HOLTransportForTarget(trimmed);
  if ([transport isEqualToString:@"tcp"]) {
    HOLParsedURI *parsed = [trimmed containsString:@"://"] ? HOLParseURI(trimmed)
                                                            : HOLParseURI([@"tcp://"
                                                                              stringByAppendingString:trimmed]);
    NSString *host = parsed.host;
    if (host.length == 0 || [host isEqualToString:@"0.0.0.0"] || [host isEqualToString:@"::"] ||
        [host isEqualToString:@"[::]"]) {
      host = @"127.0.0.1";
    }
    return HOLCanConnectTCP(host, parsed.port != nil ? parsed.port.intValue : 0, timeout);
  }

  if ([transport isEqualToString:@"unix"]) {
    HOLParsedURI *parsed = HOLParseURI(trimmed);
    return HOLCanConnectUnix(parsed.path ?: @"", timeout);
  }

  return NO;
}

static NSString *HOLDefaultPortFilePath(NSString *slug) {
  NSString *cwd = [[NSFileManager defaultManager] currentDirectoryPath];
  return [[cwd stringByAppendingPathComponent:@".op/run"]
      stringByAppendingPathComponent:[slug stringByAppendingString:@".port"]];
}

static BOOL HOLWritePortFile(NSString *path, NSString *uri) {
  NSString *trimmedPath = HOLTrimmedString(path);
  NSString *trimmedURI = HOLTrimmedString(uri);
  if (trimmedPath.length == 0 || trimmedURI.length == 0) {
    return NO;
  }

  NSError *error = nil;
  NSString *dir = [trimmedPath stringByDeletingLastPathComponent];
  if (![[NSFileManager defaultManager] createDirectoryAtPath:dir
                                 withIntermediateDirectories:YES
                                                  attributes:nil
                                                       error:&error]) {
    return NO;
  }

  NSString *content = [trimmedURI stringByAppendingString:@"\n"];
  return [content writeToFile:trimmedPath
                   atomically:YES
                     encoding:NSUTF8StringEncoding
                        error:&error];
}

static NSString *HOLUsablePortFile(NSString *path, NSTimeInterval timeout) {
  NSString *trimmedPath = HOLTrimmedString(path);
  if (trimmedPath.length == 0) {
    return nil;
  }

  NSError *error = nil;
  NSString *content = [NSString stringWithContentsOfFile:trimmedPath
                                                encoding:NSUTF8StringEncoding
                                                   error:&error];
  if (content == nil) {
    return nil;
  }

  NSString *uri = HOLTrimmedString(content);
  if (uri.length == 0) {
    [[NSFileManager defaultManager] removeItemAtPath:trimmedPath error:nil];
    return nil;
  }

  NSTimeInterval checkTimeout = timeout / 4.0;
  if (checkTimeout <= 0) {
    checkTimeout = 0.25;
  }
  if (checkTimeout > 1.0) {
    checkTimeout = 1.0;
  }

  if (HOLCanDialTarget(uri, checkTimeout)) {
    return HOLNormalizeEndpointURI(uri);
  }

  [[NSFileManager defaultManager] removeItemAtPath:trimmedPath error:nil];
  return nil;
}

static NSString *HOLExecutableOnPath(NSString *name) {
  NSString *trimmed = HOLTrimmedString(name);
  if (trimmed.length == 0) {
    return nil;
  }

  NSString *pathEnv = [[[NSProcessInfo processInfo] environment] objectForKey:@"PATH"] ?: @"";
  for (NSString *dir in [pathEnv componentsSeparatedByString:@":"]) {
    if (dir.length == 0) {
      continue;
    }
    NSString *candidate = [[dir stringByAppendingPathComponent:trimmed] stringByStandardizingPath];
    if ([[NSFileManager defaultManager] isExecutableFileAtPath:candidate]) {
      return candidate;
    }
  }
  return nil;
}

static NSString *HOLResolveBinaryPathForEntry(HOLHolonEntry *entry) {
  if (entry.manifest == nil) {
    return nil;
  }

  NSString *name = HOLTrimmedString(entry.manifest.artifacts.binary);
  if (name.length == 0) {
    return nil;
  }

  NSFileManager *fm = [NSFileManager defaultManager];
  if ([name isAbsolutePath] && [fm isExecutableFileAtPath:name]) {
    return [[name stringByStandardizingPath] stringByResolvingSymlinksInPath];
  }

  NSString *relativeCandidate = [[entry.dir stringByAppendingPathComponent:name] stringByStandardizingPath];
  if ([fm isExecutableFileAtPath:relativeCandidate]) {
    return [relativeCandidate stringByResolvingSymlinksInPath];
  }

  NSString *builtCandidate =
      [[[entry.dir stringByAppendingPathComponent:@".op/build/bin"] stringByAppendingPathComponent:[name lastPathComponent]]
          stringByStandardizingPath];
  if ([fm isExecutableFileAtPath:builtCandidate]) {
    return [builtCandidate stringByResolvingSymlinksInPath];
  }

  return HOLExecutableOnPath([name lastPathComponent]);
}

static void HOLRememberChannel(GRPCChannel *channel, NSTask *task, BOOL ephemeral) {
  if (channel == nil || task == nil) {
    return;
  }
  HOLStartedChannel *handle = [HOLStartedChannel new];
  handle.task = task;
  handle.ephemeral = ephemeral;
  @synchronized(HOLStartedChannels()) {
    HOLStartedChannels()[HOLChannelKey(channel)] = handle;
  }
}

static void HOLCloseFileHandle(NSFileHandle *handle) {
  if (handle == nil) {
    return;
  }
  @try {
    [handle closeFile];
  } @catch (NSException *exception) {
    (void)exception;
  }
}

static void HOLCloseChannel(GRPCChannel *channel) {
  if (channel == nil || channel.closed) {
    return;
  }
  channel.closed = YES;
  HOLCloseFileHandle(channel.stdinPipe.fileHandleForWriting);
  HOLCloseFileHandle(channel.stdoutPipe.fileHandleForReading);
  HOLCloseFileHandle(channel.stderrPipe.fileHandleForReading);
}

static void HOLStopTask(NSTask *task) {
  if (task == nil) {
    return;
  }
  if (!task.isRunning) {
    @try {
      [task waitUntilExit];
    } @catch (NSException *exception) {
      (void)exception;
    }
    return;
  }

  @try {
    [task terminate];
  } @catch (NSException *exception) {
    (void)exception;
  }

  NSDate *deadline = [NSDate dateWithTimeIntervalSinceNow:2.0];
  while (task.isRunning && [deadline timeIntervalSinceNow] > 0) {
    [NSThread sleepForTimeInterval:0.025];
  }

  if (task.isRunning) {
    @try {
      [task interrupt];
    } @catch (NSException *exception) {
      (void)exception;
    }
  }

  NSDate *interruptDeadline = [NSDate dateWithTimeIntervalSinceNow:0.25];
  while (task.isRunning && [interruptDeadline timeIntervalSinceNow] > 0) {
    [NSThread sleepForTimeInterval:0.025];
  }
}

static NSTask *HOLLaunchTask(NSString *binaryPath,
                             NSArray<NSString *> *arguments,
                             NSPipe **stdinPipeOut,
                             NSPipe **stdoutPipeOut,
                             NSPipe **stderrPipeOut) {
  NSString *launchPath = HOLTrimmedString(binaryPath);
  if (launchPath.length == 0) {
    return nil;
  }

  NSPipe *stdinPipe = [NSPipe pipe];
  NSPipe *stdoutPipe = [NSPipe pipe];
  NSPipe *stderrPipe = [NSPipe pipe];

  NSTask *task = [NSTask new];
  task.launchPath = launchPath;
  task.arguments = arguments ?: @[];
  task.standardInput = stdinPipe;
  task.standardOutput = stdoutPipe;
  task.standardError = stderrPipe;
  task.environment = [[NSProcessInfo processInfo] environment];

  @try {
    [task launch];
  } @catch (NSException *exception) {
    (void)exception;
    return nil;
  }

  if (stdinPipeOut != NULL) {
    *stdinPipeOut = stdinPipe;
  }
  if (stdoutPipeOut != NULL) {
    *stdoutPipeOut = stdoutPipe;
  }
  if (stderrPipeOut != NULL) {
    *stderrPipeOut = stderrPipe;
  }
  return task;
}

static NSString *HOLFirstURI(NSString *line) {
  NSString *trimmedLine = HOLTrimmedString(line);
  if (trimmedLine.length == 0) {
    return nil;
  }

  NSArray<NSString *> *fields =
      [trimmedLine componentsSeparatedByCharactersInSet:[NSCharacterSet whitespaceCharacterSet]];
  NSCharacterSet *trimSet =
      [NSCharacterSet characterSetWithCharactersInString:@"\"'()[]{}.,"];
  for (NSString *field in fields) {
    NSString *token = [field stringByTrimmingCharactersInSet:trimSet];
    if ([token hasPrefix:@"tcp://"] || [token hasPrefix:@"unix://"] ||
        [token hasPrefix:@"ws://"] || [token hasPrefix:@"wss://"] ||
        [token hasPrefix:@"stdio://"]) {
      return token;
    }
  }
  return nil;
}

static void HOLScanPipeForURI(NSPipe *pipe, NSObject *lock, NSMutableDictionary *state) {
  dispatch_async(dispatch_get_global_queue(QOS_CLASS_UTILITY, 0), ^{
    @autoreleasepool {
      NSFileHandle *handle = pipe.fileHandleForReading;
      NSMutableData *buffer = [NSMutableData data];

      while (YES) {
        NSData *chunk = [handle readDataOfLength:1];
        if (chunk.length == 0) {
          break;
        }

        uint8_t byte = ((const uint8_t *)chunk.bytes)[0];
        if (byte == '\n' || byte == '\r') {
          if (buffer.length > 0) {
            NSString *line = [[NSString alloc] initWithData:buffer encoding:NSUTF8StringEncoding];
            NSString *uri = HOLFirstURI(line);
            if (uri.length > 0) {
              @synchronized(lock) {
                if ([state[@"uri"] length] == 0) {
                  state[@"uri"] = [uri copy];
                }
              }
              return;
            }
            [buffer setLength:0];
          }
          continue;
        }

        [buffer appendData:chunk];
      }

      if (buffer.length > 0) {
        NSString *line = [[NSString alloc] initWithData:buffer encoding:NSUTF8StringEncoding];
        NSString *uri = HOLFirstURI(line);
        if (uri.length > 0) {
          @synchronized(lock) {
            if ([state[@"uri"] length] == 0) {
              state[@"uri"] = [uri copy];
            }
          }
        }
      }
    }
  });
}

static NSString *HOLReadAdvertisedURI(NSTask *task,
                                      NSPipe *stdoutPipe,
                                      NSPipe *stderrPipe,
                                      NSTimeInterval timeout) {
  NSObject *lock = [NSObject new];
  NSMutableDictionary *state = [NSMutableDictionary dictionary];

  HOLScanPipeForURI(stdoutPipe, lock, state);
  HOLScanPipeForURI(stderrPipe, lock, state);

  NSDate *deadline = [NSDate dateWithTimeIntervalSinceNow:timeout];
  while ([deadline timeIntervalSinceNow] > 0) {
    @synchronized(lock) {
      if ([state[@"uri"] length] > 0) {
        return [state[@"uri"] copy];
      }
    }
    if (!task.isRunning) {
      break;
    }
    [NSThread sleepForTimeInterval:0.05];
  }

  @synchronized(lock) {
    return [state[@"uri"] copy];
  }
}

static GRPCChannel *HOLDialDirectTarget(NSString *target, NSTimeInterval timeout) {
  NSString *dialTarget = HOLNormalizeDialTarget(target);
  if (dialTarget.length == 0) {
    return nil;
  }
  if (!HOLCanDialTarget(target, timeout)) {
    return nil;
  }
  return [[GRPCChannel alloc] initWithTarget:dialTarget
                                   transport:HOLTransportForTarget(target)
                                        task:nil
                                   stdinPipe:nil
                                  stdoutPipe:nil
                                  stderrPipe:nil];
}

static GRPCChannel *HOLStartStdioHolon(NSString *binaryPath, NSTimeInterval timeout, BOOL ephemeral) {
  NSPipe *stdinPipe = nil;
  NSPipe *stdoutPipe = nil;
  NSPipe *stderrPipe = nil;
  NSTask *task = HOLLaunchTask(binaryPath, @[ @"serve", @"--listen", @"stdio://" ],
                               &stdinPipe, &stdoutPipe, &stderrPipe);
  if (task == nil) {
    return nil;
  }

  NSDate *deadline =
      [NSDate dateWithTimeIntervalSinceNow:timeout > 0 ? MIN(timeout, 0.2) : 0.1];
  while ([deadline timeIntervalSinceNow] > 0 && task.isRunning) {
    [NSThread sleepForTimeInterval:0.01];
  }

  if (!task.isRunning) {
    HOLCloseFileHandle(stdinPipe.fileHandleForWriting);
    HOLCloseFileHandle(stdoutPipe.fileHandleForReading);
    HOLCloseFileHandle(stderrPipe.fileHandleForReading);
    return nil;
  }

  GRPCChannel *channel = [[GRPCChannel alloc] initWithTarget:@"stdio://"
                                                   transport:@"stdio"
                                                        task:task
                                                   stdinPipe:stdinPipe
                                                  stdoutPipe:stdoutPipe
                                                  stderrPipe:stderrPipe];
  HOLRememberChannel(channel, task, ephemeral);
  return channel;
}

static GRPCChannel *HOLStartTCPHolon(NSString *binaryPath,
                                     NSTimeInterval timeout,
                                     NSString *portFile,
                                     BOOL ephemeral) {
  NSPipe *stdinPipe = nil;
  NSPipe *stdoutPipe = nil;
  NSPipe *stderrPipe = nil;
  NSTask *task = HOLLaunchTask(binaryPath, @[ @"serve", @"--listen", @"tcp://127.0.0.1:0" ],
                               &stdinPipe, &stdoutPipe, &stderrPipe);
  if (task == nil) {
    return nil;
  }

  NSString *advertisedURI = HOLReadAdvertisedURI(task, stdoutPipe, stderrPipe, timeout);
  NSString *normalizedURI = HOLNormalizeEndpointURI(advertisedURI);
  if (normalizedURI.length == 0 || !HOLCanDialTarget(normalizedURI, timeout)) {
    HOLStopTask(task);
    HOLCloseFileHandle(stdinPipe.fileHandleForWriting);
    HOLCloseFileHandle(stdoutPipe.fileHandleForReading);
    HOLCloseFileHandle(stderrPipe.fileHandleForReading);
    return nil;
  }

  if (!ephemeral && !HOLWritePortFile(portFile, normalizedURI)) {
    HOLStopTask(task);
    HOLCloseFileHandle(stdinPipe.fileHandleForWriting);
    HOLCloseFileHandle(stdoutPipe.fileHandleForReading);
    HOLCloseFileHandle(stderrPipe.fileHandleForReading);
    return nil;
  }

  GRPCChannel *channel = [[GRPCChannel alloc] initWithTarget:HOLNormalizeDialTarget(normalizedURI)
                                                   transport:@"tcp"
                                                        task:task
                                                   stdinPipe:stdinPipe
                                                  stdoutPipe:stdoutPipe
                                                  stderrPipe:stderrPipe];
  HOLRememberChannel(channel, task, ephemeral);
  return channel;
}

static GRPCChannel *HOLConnectInternal(NSString *target,
                                       HolonsConnectOptions *options,
                                       BOOL ephemeral) {
  NSString *trimmedTarget = HOLTrimmedString(target);
  if (trimmedTarget.length == 0) {
    return nil;
  }

  HolonsConnectOptions *resolvedOptions = options ?: [HolonsConnectOptions new];
  NSTimeInterval timeout = resolvedOptions.timeout > 0 ? resolvedOptions.timeout
                                                       : HOLDefaultConnectTimeout;

  if (HOLIsDirectTarget(trimmedTarget)) {
    return HOLDialDirectTarget(trimmedTarget, timeout);
  }

  NSString *transport = [[HOLTrimmedString(resolvedOptions.transport) lowercaseString] copy];
  if (transport.length == 0) {
    transport = @"stdio";
  }
  if (![transport isEqualToString:@"stdio"] && ![transport isEqualToString:@"tcp"]) {
    return nil;
  }

  NSString *portFile = HOLTrimmedString(resolvedOptions.portFile);
  if (portFile.length == 0) {
    portFile = HOLDefaultPortFilePath(trimmedTarget);
  }

  NSError *error = nil;
  HOLHolonEntry *entry = HOLFindBySlug(trimmedTarget, &error);
  if (entry == nil) {
    return nil;
  }

  NSString *reusedURI = HOLUsablePortFile(portFile, timeout);
  if (reusedURI.length > 0) {
    return HOLDialDirectTarget(reusedURI, timeout);
  }
  if (!resolvedOptions.start) {
    return nil;
  }

  NSString *binaryPath = HOLResolveBinaryPathForEntry(entry);
  if (binaryPath.length == 0) {
    return nil;
  }

  if ([transport isEqualToString:@"stdio"]) {
    return HOLStartStdioHolon(binaryPath, timeout, YES);
  }

  return HOLStartTCPHolon(binaryPath, timeout, portFile, ephemeral);
}

@implementation Holons

+ (GRPCChannel *)connect:(NSString *)target {
  return HOLConnectInternal(target, nil, YES);
}

+ (GRPCChannel *)connect:(NSString *)target options:(HolonsConnectOptions *)options {
  return HOLConnectInternal(target, options, NO);
}

+ (void)disconnect:(GRPCChannel *)channel {
  if (channel == nil) {
    return;
  }

  HOLStartedChannel *handle = nil;
  @synchronized(HOLStartedChannels()) {
    handle = HOLStartedChannels()[HOLChannelKey(channel)];
    [HOLStartedChannels() removeObjectForKey:HOLChannelKey(channel)];
  }

  HOLCloseChannel(channel);

  if (handle.ephemeral) {
    HOLStopTask(handle.task);
  }
}

@end
