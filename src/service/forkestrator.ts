import { ChildProcess, fork } from 'child_process';
import { DirResult, dirSync as newTmpDir } from 'tmp';
import restify, { Next, Request, Response } from 'restify';
import { BadRequestError, InternalServerError, NotFoundError } from 'restify-errors';
import { EventEmitter, once } from 'events';
import LOG from 'loglevel';
import genericPool, { Pool } from 'generic-pool';
import { logTs } from '../util';
import { CloneChildProcess, Orchestrator, Remoting } from './index';
import { CloneMessage } from './clone-process';

export class Forkestrator<ProcessType extends CloneChildProcess>
  extends EventEmitter implements Orchestrator {
  private readonly http = restify.createServer();
  private readonly domains = new Set<string>();
  private readonly clones: { [cloneId: string]: ProcessType } = {};
  private readonly requests: { [reqId: string]: [Response, Next] } = {};
  private readonly pool: Pool<ChildProcess>;

  constructor(
    private readonly remoting: Remoting<ProcessType>,
    cloneModulePath: string
  ) {
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
    (['start', 'transact', 'stop', 'kill', 'destroy', 'partition'] as
      ['start', 'transact', 'stop', 'kill', 'destroy', 'partition'])
      .forEach(route => this.http.post(`/${route}`, this[route].bind(this)));
    this.http.on('after', req => { delete this.requests[req.id()]; });
    this.http.listen(0, () => {
      const url = `http://localhost:${this.http.address().port}`;
      LOG.info(logTs(), `Orchestrator listening on ${url}`);
      this.pool.ready().then(() => this.emit('listening', url));
    });
    remoting.initialise(this.clones);
  }

  start(startReq: Request, startRes: Response, next: Next) {
    this.registerRequest(startReq, startRes, next);
    const { cloneId, domain } = startReq.query;
    let tmpDir: DirResult;
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
        // @ts-ignore
        this.clones[cloneId] = { process, tmpDir, ...meta };
        const clone = this.clones[cloneId];
        const releaseProcess = (cb: (err: any) => void) => {
          LOG.debug(logTs(), cloneId, `Releasing clone process`);
          this.pool.release(process).then(() => {
            process.off('message', msgHandler);
            delete clone.process;
            this.remoting.release(clone).then(cb, cb);
          }).catch(err => LOG.error(logTs(), cloneId, err));
        };
        const msgHandler = (message: CloneMessage) => {
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
  transact(req: Request, res: Response, next: Next) {
    this.registerRequest(req, res, next);
    const { cloneId } = req.query;
    this.withClone(cloneId, ({ process }) => {
      process!.send({
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
  stop(req: Request, res: Response, next: Next) {
    this.registerRequest(req, res, next);
    const { cloneId } = req.query;
    this.withClone(cloneId, ({ process }) => {
      LOG.debug(logTs(), cloneId, `Stopping clone`);
      process!.send({
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
  kill(req: Request, res: Response, next: Next) {
    const { cloneId } = req.query;
    this.withClone(cloneId, clone => {
      LOG.debug(logTs(), cloneId, `Killing clone process`);
      clone.process!.kill();
      clone.process!.on('exit', () => {
        this.pool.destroy(clone.process!).then(() => {
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
  destroy(req: Request, res: Response, next: Next) {
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
  partition(req: Request, res: Response, next: Next) {
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

  destroyDataAndForget(cloneId: string, tmpDir: DirResult) {
    LOG.info(logTs(), cloneId, `Destroying clone data`);
    tmpDir.removeCallback();
    delete this.clones[cloneId];
  }

  registerRequest(req: Request, res: Response, next: Next) {
    this.requests[req.id()] = [res, next];
  }

  withClone(cloneId: string, op: (clone: ProcessType) => void, next: Next) {
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