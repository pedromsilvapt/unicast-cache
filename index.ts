import { ConsoleBackend, FilterBackend, HttpRequestLogger, MultiBackend, SharedLogger } from 'clui-logger';
import { TIMESTAMP_SHORT } from 'clui-logger/lib/Backends/ConsoleBackend';
import restify from 'restify';

// Instantiate DI Container
// TODO...

// Load configuration file from the current working directory
// TODO...
// const config: TfsConfig = Config.load('config.kdl', TfsConfigFormat).data;
const config = { debug: false, server: { port: 3003 } };

// Configure Logger
const logger = new SharedLogger(new MultiBackend([
    new FilterBackend(
        new ConsoleBackend(TIMESTAMP_SHORT),
        config.debug ? [">=debug"] : [">=info"]
    ),
    // Enable logging to a file
    // new FileBackend( this.storage.getPath( 'logs', 'app-:YYYY-:MM-:DD.log' ) ),
]));

// Configure Http Logger
const httpLoggerMiddleware = new HttpRequestLogger(logger.service('http'), act => {
    return act.req.method === 'OPTIONS' || (act.req.url?.startsWith('/index.html') ?? false); // || act.req.url?.startsWith( '/media/send' ) ) || act.req.url.startsWith( '/api/media/artwork' );
});

// Instantiate endpoints controller
// TODO...

// Create the HTTP server
var server = restify.createServer({ name: 'TFS Capacity Viewer' });

// Configure Logger & Restify
server.use(restify.plugins.jsonBodyParser());
server.use(httpLoggerMiddleware.before());
server.on('after', httpLoggerMiddleware.after());

// Configure Endpoints
// TODO...

// Configure static files
server.get('/*', restify.plugins.serveStaticFiles('./client'));

// Start the server
server.listen(config.server.port, function () {
    logger.info('http', `${server.name} listening at ${server.url}`);
});
