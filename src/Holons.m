#import <Holons/Holons.h>

NSString *const HOLDefaultURI = @"tcp://:9090";

NSString *HOLScheme(NSString *uri) {
  NSRange r = [uri rangeOfString:@"://"];
  if (r.location != NSNotFound) {
    return [uri substringToIndex:r.location];
  }
  return uri;
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
