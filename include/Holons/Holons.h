#import <Foundation/Foundation.h>

/// Default transport URI when --listen is omitted.
extern NSString *const HOLDefaultURI;

/// Extract the scheme from a transport URI.
NSString *HOLScheme(NSString *uri);

/// Parse --listen or --port from command-line args.
NSString *HOLParseFlags(NSArray<NSString *> *args);
