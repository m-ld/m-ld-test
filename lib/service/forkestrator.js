const { fork } = require('child_process');
const { dirSync: newTmpDir } = require('tmp');
const restify = require('restify');
const { BadRequestError, InternalServerError, NotFoundError } = require('restify-errors');
const { once } = require('events');
const LOG = require('loglevel');
const genericPool = require('generic-pool');
const { EventEmitter } = require('events');
const { logTs } = require('../util');

// noinspection JSClosureCompilerSyntax - no idea
/** @implements m_ld_test.Orchestrator */
class Forkestrator extends EventEmitter {
  http = restify.createServer();
  /** @type Set<string> */
  domains = new Set;
  /** @type {{ [cloneId: string]: m_ld_test.CloneChildProcess }} */
  clones = {};
  /** @type {{ [reqId: string]: [restify.Response, restify.Next] }} */
  requests = {};

  /**
   * @param {m_ld_test.Remoting} remoting
   * @param {string} cloneModulePath
   */
  constructor(remoting, cloneModulePath) {
    super();
    this.pool = genericPool.createPool({
      async create() {
        const process = fork(cloneModulePath, {
          // Strictly disallow unhandled promise rejections for compliance testing
          execArgv: ['--unhandled-rejections=strict']
        });
        // Wait for the 'active' message
        await once(process, 'message');
        return process;
      },
      async destroy(process) {
        process.kill();
        await once(process, 'exit');
      },
      async validate(process) {
        return process.connected;
      }
    }, {
      min: 5, // Pre-allocation
      max: 100
    });
    this.remoting = remoting;
    this.http.use(restify.plugins.queryParser());
    this.http.use(restify.plugins.bodyParser());
    ['start', 'transact', 'stop', 'kill', 'destroy', 'partition']
      .forEach(route => this.http.post('/' + route, this[route].bind(this)));
    this.http.on('after', req => { delete this.requests[req.id()]; });
    this.http.listen(0, () => {
      const url = `http://localhost:${this.http.address().port}`;
      LOG.info(logTs(), `Orchestrator listening on ${url}`);
      this.pool.ready().then(() => this.emit('listening', url));
    });
    remoting.initialise(this.clones);
  }

  /**
   * @param {restify.Request} startReq
   * @param {restify.Response} startRes
   * @param {restify.Next} next
   */
  start(startReq, startRes, next) {
    this.registerRequest(startReq, startRes, next);
    const { cloneId, domain } = startReq.query;
    let tmpDir;
    if (cloneId in this.clones) {
      tmpDir = this.clones[cloneId].tmpDir;
      if (this.clones[cloneId].process)
        return next(new BadRequestError(`Clone ${cloneId} is already started`));
    } else {
      // noinspection JSCheckFunctionSignatures
      tmpDir = newTmpDir({ unsafeCleanup: true });
    }
    const genesis = !this.domains.has(domain);
    LOG.info(logTs(), cloneId, `Starting ${genesis ? 'genesis ' : ''}clone on domain ${domain}`);
    this.domains.add(domain);

    this.remoting.provision(cloneId).then(({ config, meta }) => {
      config = Object.assign(config, {
        '@id': cloneId,
        '@domain': domain,
        genesis,
        networkTimeout: 1000,
        logLevel: LOG.getLevel()
      }, startReq.body);

      this.pool.acquire().then(process => {
        startRes.header('transfer-encoding', 'chunked');
        const clone = this.clones[cloneId] = { process, tmpDir, ...meta };
        const releaseProcess = (cb) => {
          LOG.debug(logTs(), cloneId, `Releasing clone process`);
          this.pool.release(process).then(() => {
            process.off('message', msgHandler);
            delete clone.process;
            this.remoting.release(clone).then(cb, cb);
          }).catch(err => LOG.error(logTs(), cloneId, err));
        };
        const msgHandler = message => {
          switch (message['@type']) {
            case 'started':
            case 'updated':
            case 'status':
              startRes.write(JSON.stringify(message));
              break;
            case 'closed':
              startRes.end();
              next();
              break;
            case 'unstarted':
              releaseProcess(err => {
                startRes.status(400);
                startRes.write(JSON.stringify(new BadRequestError(err || message.err)));
                startRes.end();
              });
              break;
            case 'next': {
              const { requestId, body } = message;
              const [res] = this.requests[requestId];
              res.write(JSON.stringify(body));
            }
              break;
            case 'complete': {
              const { requestId } = message;
              const [res, next] = this.requests[requestId];
              res.end();
              next();
            }
              break;
            case 'error': {
              const { requestId, err } = message;
              const [res, next] = this.requests[requestId];
              if (res.header('transfer-encoding') === 'chunked') {
                // Restify tries to set content-length, which is bad for chunking
                res.status(500);
                res.write(JSON.stringify(new InternalServerError(err)));
                res.end();
              } else {
                next(new InternalServerError(err));
              }
            }
              break;
            case 'destroyed': {
              const { requestId } = message;
              const [res, next] = this.requests[requestId];
              releaseProcess(err => {
                if (err)
                  next(new InternalServerError(err));
                else
                  res.send({ '@type': 'destroyed' });
              });
              this.destroyDataAndForget(cloneId, tmpDir);
            }
              break;
            case 'stopped': {
              const { requestId } = message;
              const [res, next] = this.requests[requestId];
              releaseProcess(err => {
                if (err)
                  next(new InternalServerError(err));
                else
                  res.send({ '@type': 'stopped' });
              });
            }
              break;
          }
        };
        process.send({
          '@type': 'start',
          config,
          tmpDirName: tmpDir.name,
          requestId: startReq.id()
        });
        process.on('message', msgHandler);
      });
    }).catch(err => {
      next(new InternalServerError(err));
    });
  }

  /**
   * @param {restify.Request} req
   * @param {restify.Response} res
   * @param {restify.Next} next
   */
  transact(req, res, next) {
    this.registerRequest(req, res, next);
    const { cloneId } = req.query;
    this.withClone(cloneId, ({ process }) => {
      process.send({
        id: req.id(),
        '@type': 'transact',
        request: req.body
      }, err => err && next(err));
      res.header('transfer-encoding', 'chunked');
    }, next);
  }

  /**
   * @param {restify.Request} req
   * @param {restify.Response} res
   * @param {restify.Next} next
   */
  stop(req, res, next) {
    this.registerRequest(req, res, next);
    const { cloneId } = req.query;
    this.withClone(cloneId, ({ process }) => {
      LOG.debug(logTs(), cloneId, `Stopping clone`);
      process.send({
        id: req.id(), '@type': 'stop'
      }, err => {
        if (err) {
          // This clone process is already killed
          LOG.debug(logTs(), cloneId, `Already killed`, err);
          res.send({ '@type': 'stopped', cloneId });
          next();
        }
      });
    }, next);
  }

  /**
   * @param {restify.Request} req
   * @param {restify.Response} res
   * @param {restify.Next} next
   */
  kill(req, res, next) {
    const { cloneId } = req.query;
    this.withClone(cloneId, clone => {
      LOG.debug(logTs(), cloneId, `Killing clone process`);
      clone.process.kill();
      clone.process.on('exit', () => {
        this.pool.destroy(clone.process).then(() => {
          delete clone.process;
          this.remoting.release(clone, { unref: true }).then(() => {
            res.send({ '@type': 'killed', cloneId });
            next();
          }).catch(err => {
            next(new InternalServerError(err));
          });
        }).catch(err => LOG.error(logTs(), cloneId, err));
      });
    }, next);
  }

  /**
   * @param {restify.Request} req
   * @param {restify.Response} res
   * @param {restify.Next} next
   */
  destroy(req, res, next) {
    this.registerRequest(req, res, next);
    const { cloneId } = req.query;
    this.withClone(cloneId, ({ process, tmpDir }) => {
      LOG.debug(logTs(), cloneId, `Destroying clone`);
      const alreadyDestroyed = () => {
        this.destroyDataAndForget(cloneId, tmpDir);
        res.send({ '@type': 'destroyed', cloneId });
        next();
      };
      if (process) {
        process.send({
          id: req.id(), '@type': 'destroy'
        }, err => {
          if (err) {
            // This clone process is already killed
            LOG.debug(logTs(), cloneId, `Already killed`, err);
            alreadyDestroyed();
          }
        });
      } else {
        alreadyDestroyed();
      }
    }, next);
  }

  /**
   * @param {restify.Request} req
   * @param {restify.Response} res
   * @param {restify.Next} next
   */
  partition(req, res, next) {
    const { cloneId, state: stateString } = req.query;
    LOG.debug(logTs(), cloneId, `Partitioning clone (${stateString})`);
    const state = stateString !== 'false';
    this.withClone(cloneId, async clone => {
      try {
        await this.remoting.partition(clone, state);
        res.send({ '@type': 'partitioned', state });
      } catch (e) {
        next(new InternalServerError(e));
      }
    }, next);
  }

  /**
   * @param {string} cloneId
   * @param {import('tmp').DirResult} tmpDir
   */
  destroyDataAndForget(cloneId, tmpDir) {
    LOG.info(logTs(), cloneId, `Destroying clone data`);
    tmpDir.removeCallback();
    delete this.clones[cloneId];
  }

  /**
   * @param {restify.Request} req
   * @param {restify.Response} res
   * @param {restify.Next} next
   */
  registerRequest(req, res, next) {
    this.requests[req.id()] = [res, next];
  }

  /**
   * @param {string} cloneId
   * @param {(clone: m_ld_test.CloneChildProcess) => void} op
   * @param {restify.Next} next
   */
  withClone(cloneId, op/*(subprocess, tmpDir)*/, next) {
    if (cloneId in this.clones) {
      op(this.clones[cloneId]);
    } else {
      next(new NotFoundError(`Clone ${cloneId} not available`));
    }
  }

  async close() {
    LOG.info(logTs(), `Orchestrator shutting down`);
    await this.pool.drain();
    await this.pool.clear();
    this.http.close();
  }
}

module.exports = Forkestrator;